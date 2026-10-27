import { createHash, randomBytes } from "node:crypto";
import { chmod, lstat, mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { delimiter, dirname, join } from "node:path";

export const providers = ["codex", "claude", "gemini", "opencode", "qwen", "kimi", "grok", "deepseek", "autohand", "cursor", "copilot"];
export function portsFromEnv(env) {
  const defaults = { api: 14910, web: 14911, postgres: 15432, temporal: 17233, opencode: 17047 };
  const ports = Object.fromEntries(Object.entries(defaults).map(([name, fallback]) => {
    const raw = env[`SPACE_DEV_${name.toUpperCase()}_PORT`];
    const port = raw === undefined ? fallback : Number(raw);
    if (!Number.isInteger(port) || port < 1024 || port > 65535 || port === 4910 || port === 4911) {
      throw new Error(`Invalid SPACE_DEV_${name.toUpperCase()}_PORT: choose an unprivileged port other than 4910/4911.`);
    }
    return [name, port];
  }));
  if (new Set(Object.values(ports)).size !== Object.keys(ports).length) throw new Error("Development ports must be distinct.");
  return ports;
}

// Read only the named service's literal, digest-pinned image. Never copy pins into this tool.
export function imageFromCompose(source, service) {
  const block = source.match(new RegExp(`^  ${service}:\\r?\\n([\\s\\S]*?)(?=^  [a-zA-Z][\\w-]*:|$(?![\\s\\S]))`, "m"))?.[1];
  const image = block?.match(/^    image: ([^\s]+)\s*$/m)?.[1];
  if (!image || !/^[\w./:-]+@sha256:[a-f0-9]{64}$/.test(image)) throw new Error(`${service} must have a literal digest-pinned image in the launcher compose file.`);
  return image;
}

export function composeDocument(config, images) {
  return {
    name: config.project,
    services: {
      postgres: {
        image: images.postgres,
        environment: { POSTGRES_DB: "spaceapp_dev", POSTGRES_USER: "spaceapp", POSTGRES_PASSWORD_FILE: "/run/secrets/postgres-password" },
        ports: [`127.0.0.1:${config.ports.postgres}:5432`],
        volumes: ["postgres:/var/lib/postgresql/data"], secrets: ["postgres-password"],
        healthcheck: { test: ["CMD-SHELL", "pg_isready -U spaceapp -d spaceapp_dev"], interval: "2s", timeout: "5s", retries: 60 }
      },
      temporal: {
        image: images.temporal,
        // Compose file-backed secrets retain their host uid/mode on Linux. Read
        // the 0600 secret as root, then drop to the image's temporal user.
        user: "0:0",
        depends_on: { postgres: { condition: "service_healthy" } },
        environment: { DB: "postgres12", DB_PORT: "5432", POSTGRES_USER: "spaceapp", POSTGRES_SEEDS: "postgres", DYNAMIC_CONFIG_FILE_PATH: "config/dynamicconfig/docker.yaml" },
        entrypoint: ["/bin/sh", "-ec"],
        command: ['export POSTGRES_PWD="$$(cat /run/secrets/postgres-password)"; exec su -p -s /bin/sh temporal -c "exec /etc/temporal/entrypoint.sh autosetup"'],
        secrets: ["postgres-password"], ports: [`127.0.0.1:${config.ports.temporal}:7233`],
        healthcheck: { test: ["CMD-SHELL", "temporal operator cluster health --address temporal:7233 >/dev/null && temporal operator namespace describe --namespace default --address temporal:7233 >/dev/null"], interval: "3s", timeout: "10s", retries: 60 }
      }
    },
    volumes: { postgres: {} },
    secrets: { "postgres-password": { file: "./postgres-password" } }
  };
}

export async function location(root) {
  root = await realpath(root);
  const project = `spaceapp-dev-${createHash("sha256").update(root).digest("hex").slice(0, 12)}`;
  const state = join(root, ".dev-stack");
  if (process.platform !== "win32" && Buffer.byteLength(join(state, "control.sock")) > 100) throw new Error("Checkout path is too long for Unix sockets. Move it to a short path on the Linux/macOS filesystem (on WSL, not /mnt/c).");
  const info = await lstat(state).catch(error => { if (error.code !== "ENOENT") throw error; });
  if (info && (!info.isDirectory() || info.isSymbolicLink())) throw new Error("Refusing a symlink or non-directory .dev-stack.");
  return { root, state, project };
}

export async function ownedState(loc) {
  const record = JSON.parse(await readFile(join(loc.state, "owner.json"), "utf8"));
  if (record.version !== 1 || record.root !== loc.root || record.project !== loc.project) throw new Error("Development state ownership does not match this checkout; refusing to modify it.");
  return record;
}

export async function privateFile(path, content) {
  const info = await lstat(path).catch(error => { if (error.code !== "ENOENT") throw error; });
  if (info?.isSymbolicLink() || (info && !info.isFile())) throw new Error("Refusing a non-regular development state file.");
  await writeFile(path, content, { mode: 0o600 });
  await chmod(path, 0o600);
}

export async function prepareState(loc, env) {
  let config;
  const info = await lstat(loc.state).catch(error => { if (error.code !== "ENOENT") throw error; });
  if (info) config = await ownedState(loc);
  else {
    await mkdir(loc.state, { mode: 0o700 });
    config = { version: 1, root: loc.root, project: loc.project, ports: portsFromEnv(env) };
    await privateFile(join(loc.state, "owner.json"), JSON.stringify(config));
    const secrets = Object.fromEntries(["database", "session", "setup", "control", "opencode"].map(name => [name, randomBytes(32).toString("hex")]));
    await privateFile(join(loc.state, "secrets.json"), JSON.stringify(secrets));
    await privateFile(join(loc.state, "postgres-password"), secrets.database);
    await privateFile(join(loc.state, "setup-token"), secrets.setup);
  }
  await chmod(loc.state, 0o700);
  for (const directory of ["home", "tmp", "bin", "logs", "memory", "memory-graph", "artifacts", "workspaces", "projects", "providers/opencode", "telegram", "streaming", "demos"]) {
    await mkdir(join(loc.state, directory), { recursive: true, mode: 0o700 });
  }
  return config;
}

export function runtimeEnvironment(loc, config, secrets, inherited = process.env) {
  const s = (...parts) => join(loc.state, ...parts);
  // Do not inherit provider keys, NODE_OPTIONS, SPACE_* paths, or shell startup hooks.
  const env = Object.fromEntries(["PATH", "LANG", "LC_ALL", "TERM"].filter(key => inherited[key]).map(key => [key, inherited[key]]));
  Object.assign(env, {
    PATH: [s("bin"), dirname(process.execPath), inherited.PATH || "/usr/bin:/bin"].join(delimiter),
    HOME: s("home"), TMPDIR: s("tmp"), NODE_ENV: "development", FORCE_COLOR: "0",
    SPACE_INSTANCE: "dev", SPACE_INSTANCE_ENABLED_JOBS: "", SPACE_PUBLIC_DISTRIBUTION: "true",
    SPACE_INSTALL_ROOT: loc.root, SPACE_RUNTIME_STORE: "postgres",
    SPACE_DATABASE_URL: `postgresql://spaceapp:${secrets.database}@127.0.0.1:${config.ports.postgres}/spaceapp_dev`,
    SPACE_SESSION_SECRET: secrets.session, SPACE_SETUP_TOKEN_FILE: s("setup-token"), SPACE_DEV_LOGIN: "false",
    SPACE_API_HOST: "127.0.0.1", SPACE_API_PORT: String(config.ports.api),
    SPACE_WEB_HOST: "127.0.0.1", SPACE_WEB_PORT: String(config.ports.web), SPACE_API_ORIGIN: `http://127.0.0.1:${config.ports.api}`,
    SPACE_TEMPORAL_ADDRESS: `127.0.0.1:${config.ports.temporal}`, SPACE_TEMPORAL_NAMESPACE: config.project, SPACE_TEMPORAL_TASK_QUEUE: config.project,
    SPACEAPP_WORKFLOWS_ENABLED: "true", SPACE_CLI_ENABLED: "true", SPACE_CLI_COMMAND_PATH: s("bin"), SPACE_CLI_HOST_SOCKET: s("cli.sock"),
    SPACE_CLI_WORKSPACE_ROOT: s("workspaces"), SPACE_CLI_ROOT_ENABLED: "false", SPACE_CLI_ADMIN_HOST_SOCKET: s("disabled-admin.sock"),
    SPACE_BROWSER_SESSIONS_ENABLED: "false", SPACE_BROWSER_EVIDENCE_ENABLED: "false", SPACE_BROWSER_HOST_SOCKET: s("disabled-browser.sock"),
    SPACE_FILES_HOST_MODE: "disabled", SPACE_AGENT_TOOLS_WRITER: "disabled", SPACE_TELEMETRY_ENABLED: "false", SPACE_CHESS_MOVE_SOCKET: "disabled",
    SPACE_PROJECTS_ROOT: s("projects"), SPACE_DEMO_PROJECTS_ROOT: join(loc.root, "demo-projects"), SPACE_DEMO_PROJECTS_VAR_ROOT: s("demos"),
    SPACE_DOT_WORKSPACE_ROOT: s("workspaces"), SPACE_ARTIFACT_ROOT: s("artifacts"), SPACE_MEMORY_ROOT: s("memory"), SPACE_MEMORY_GRAPH_ROOT: s("memory-graph"),
    SPACE_MEMORY_INDEX_PATH: s("memory", "space_memory.md"), SPACE_MEMORY_MONTHLY_PATH: s("memory", "space_memory_monthly.md"), SPACE_MEMORY_LOCK_PATH: s("memory", ".memory.lock"),
    SPACE_CODEX_HOME: s("providers", "codex"), SPACE_CODEX_APP_SERVER_HOME: s("providers", "codex"), SPACE_CODEX_GOALS_DB_PATH: s("codex-goals.sqlite"),
    SPACE_CONNECTOR_KEY_FILE: s("connector.key"), SPACE_DESIGN_GATEWAY_KEY_FILE: s("disabled-design.key"), SPACE_TELEGRAM_SECRET_ROOT: s("telegram"), SPACE_STREAMING_SECRET_ROOT: s("streaming"),
    SPACE_TELEGRAM_OWNERSHIP_MANIFEST: s("telegram", "ownership.json"), SPACE_APP_DIAGNOSTICS_ROOT: s("diagnostics"), SPACE_MAINTENANCE_AUTOPILOT_STATE_FILE: s("maintenance", "state.json"),
    OPENCODE_SERVER_USERNAME: "spaceapp-dev", OPENCODE_SERVER_PASSWORD: secrets.opencode, OPENCODE_DISABLE_AUTOUPDATE: "true",
    XDG_CONFIG_HOME: s("providers", "opencode", "config"), XDG_DATA_HOME: s("providers", "opencode", "data"), XDG_CACHE_HOME: s("providers", "opencode", "cache"), XDG_STATE_HOME: s("providers", "opencode", "state")
  });
  return env;
}
