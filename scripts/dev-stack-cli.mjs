import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { access, mkdir, readFile } from "node:fs/promises";
import { constants } from "node:fs";
import { basename, join } from "node:path";

/** Contributor-only wrappers: all provider storage belongs to this checkout. */
export async function runCli(state, argv = process.argv) {
  const provider = basename(argv[1]).replace(/-vscode-parity$/, "");
  const args = argv.slice(2);
  if (args[0] === "--run-inner") args.shift();
  if (provider !== "opencode") {
    console.error("This contributor stack includes only OpenCode. Other providers are disabled.");
    process.exitCode = 69;
    return;
  }
  const config = JSON.parse(await readFile(join(state, "owner.json"), "utf8"));
  const secrets = JSON.parse(await readFile(join(state, "secrets.json"), "utf8"));
  const binary = join(state, "vendor", "node_modules", ".bin", "opencode");
  await access(binary, constants.X_OK);
  const home = join(state, "providers", "opencode");
  const env = Object.fromEntries(["PATH", "TERM", "LANG", "LC_ALL", "SPACE_CLI_MODEL_ID", "SPACE_CLI_REASONING_EFFORT"].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { HOME: home, TMPDIR: join(state, "tmp"), OPENCODE_DISABLE_AUTOUPDATE: "true",
    XDG_CONFIG_HOME: join(home, "config"), XDG_DATA_HOME: join(home, "data"), XDG_CACHE_HOME: join(home, "cache"), XDG_STATE_HOME: join(home, "state"),
    OPENCODE_SERVER_USERNAME: "spaceapp-dev", OPENCODE_SERVER_PASSWORD: secrets.opencode });
  for (const path of [home, env.XDG_CONFIG_HOME, env.XDG_DATA_HOME, env.XDG_CACHE_HOME, env.XDG_STATE_HOME]) await mkdir(path, { recursive: true, mode: 0o700 });
  if (args[0] === "credential-status") { console.log("READY"); return; }
  if (args[0] === "credential-observation") { console.log(`OBSERVATION:${createHash("sha256").update(binary).digest("hex")}`); return; }
  if (args[0] === "inspect-env") { console.log("runtime=opencode\nhome=isolated\ncredential=not-required"); return; }
  if (["credential-smoke", "credential-install", "opencode-text", "opencode-models", "login", "setup"].includes(args[0])) {
    console.error("This contributor stack demonstrates the OpenCode terminal; provider execution and credential import are not configured.");
    process.exitCode = 69;
    return;
  }
  const pane = !args.length || ["--continue", "-c"].includes(args[0]);
  const child = spawn(binary, pane ? ["--server", `http://127.0.0.1:${config.ports.opencode}`, ...args, process.cwd()] : args, { env, stdio: "inherit" });
  for (const signal of ["SIGTERM", "SIGINT", "SIGHUP"]) process.on(signal, () => child.kill(signal));
  child.on("error", () => { console.error("Cannot start the checkout's OpenCode binary."); process.exitCode = 1; });
  child.on("exit", code => { process.exitCode = code ?? 1; });
}
