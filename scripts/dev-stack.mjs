#!/usr/bin/env node
import { spawn } from "node:child_process";
import { createServer, createConnection } from "node:net";
import { chmod, copyFile, lstat, mkdir, readFile, rm, symlink } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { composeDocument, imageFromCompose, location, ownedState, prepareState, privateFile, providers, runtimeEnvironment } from "./dev-stack-config.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const log = message => process.stdout.write(`[dev:stack] ${message}\n`);
const alive = pid => { try { process.kill(pid, 0); return true; } catch (error) { if (error.code === "ESRCH") return false; throw error; } };

export function run(command, args, { cwd = root, env = process.env, quiet = false, signal } = {}) {
  return new Promise((resolve, reject) => {
    signal?.throwIfAborted();
    const child = spawn(command, args, { cwd, env, detached: true, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    const capture = data => { output = (output + data.toString()).slice(-2 * 1024 * 1024); };
    child.stdout.on("data", capture);
    child.stderr.on("data", capture);
    const abort = () => { try { process.kill(-child.pid, "SIGTERM"); } catch (error) { if (error.code !== "ESRCH") reject(error); } };
    signal?.addEventListener("abort", abort, { once: true });
    child.on("error", reject);
    child.on("close", code => {
      signal?.removeEventListener("abort", abort);
      if (signal?.aborted) { try { process.kill(-child.pid, "SIGKILL"); } catch {} }
      if (!quiet) {
        const sensitive = [env.SPACE_DATABASE_URL, env.SPACE_SESSION_SECRET, env.OPENCODE_SERVER_PASSWORD].filter(Boolean);
        for (const value of sensitive) output = output.replaceAll(value, "[redacted]");
        if (env.SPACE_DATABASE_URL) output = output.replaceAll(new URL(env.SPACE_DATABASE_URL).password, "[redacted]");
        process.stdout.write(output);
      }
      code === 0 ? resolve() : reject(new Error(`${command} ${args[0] || ""} failed (exit ${code ?? "signal"}).`));
    });
  });
}

async function checkPorts(ports) {
  // Fail before starting containers; never adopt a process already using a port.
  for (const [name, port] of Object.entries(ports)) {
    const server = createServer();
    await new Promise((resolve, reject) => {
      server.once("error", () => reject(new Error(`${name} port ${port} is occupied. Choose SPACE_DEV_${name.toUpperCase()}_PORT before initializing state, or reset the owned stack to change ports.`)));
      server.listen(port, "127.0.0.1", resolve);
    });
    await new Promise(resolve => server.close(resolve));
  }
}

export async function waitForReady(url, { signal, timeout = 180_000, check = body => body.ok === true } = {}) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    signal?.throwIfAborted();
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(2000) });
      if (response.ok && check(await response.json())) return;
    } catch { /* bounded startup retry; actual child failures abort separately */ }
    await delay(500, undefined, { signal });
  }
  throw new Error(`Readiness timed out for ${new URL(url).pathname}; inspect .dev-stack/logs.`);
}

function compose(loc, ...args) {
  return run("docker", ["compose", "--project-name", loc.project, "--file", join(loc.state, "compose.json"), ...args], { cwd: loc.root });
}

async function requestStop(loc) {
  const lock = JSON.parse(await readFile(join(loc.state, "running.json"), "utf8").catch(error => { if (error.code !== "ENOENT") throw error; return "null"; }));
  if (!lock) return;
  const secrets = JSON.parse(await readFile(join(loc.state, "secrets.json"), "utf8"));
  if (!alive(lock.pid)) {
    await rm(join(loc.state, "run-lock"), { recursive: true, force: true });
    await rm(join(loc.state, "running.json"), { force: true });
    return;
  }
  await new Promise((resolve, reject) => {
    const client = createConnection(join(loc.state, "control.sock"));
    client.setTimeout(15_000, () => client.destroy(new Error("Development supervisor did not accept stop.")));
    client.on("connect", () => client.end(JSON.stringify({ token: secrets.control }) + "\n"));
    client.on("error", reject);
    client.on("data", data => { if (data.toString().trim() === "stopping") resolve(); });
  });
  const deadline = Date.now() + 30_000;
  while (alive(lock.pid) && Date.now() < deadline) await delay(100);
  if (alive(lock.pid)) throw new Error("Supervisor is still stopping; reset refused. Inspect .dev-stack/logs and retry down.");
}

async function prepareWrappers(loc) {
  const bin = join(loc.state, "bin");
  const source = `#!/usr/bin/env node\nimport {runCli} from ${JSON.stringify(pathToFileURL(join(loc.root, "scripts", "dev-stack-cli.mjs")).href)};\nawait runCli(${JSON.stringify(loc.state)});\n`;
  for (const provider of providers) {
    const path = join(bin, `${provider}-vscode-parity`);
    await privateFile(path, source);
    await chmod(path, 0o700);
  }
  const node = join(bin, "node");
  const info = await lstat(node).catch(error => { if (error.code !== "ENOENT") throw error; });
  if (!info) await symlink(process.execPath, node);
}

async function stopChildren(children) {
  // CLI host closes its owned PTYs before the final group sweep.
  for (const child of children) {
    if (child.exitCode === null && child.signalCode === null) { try { process.kill(-child.pid, "SIGTERM"); } catch (error) { if (error.code !== "ESRCH") throw error; } }
  }
  const deadline = Date.now() + 10_000;
  while (children.some(child => child.exitCode === null && child.signalCode === null) && Date.now() < deadline) await delay(100);
  for (const child of children) {
    // A group can outlive its original leader (watcher/build descendants).
    try { process.kill(-child.pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; }
  }
}

async function start(loc) {
  const config = await prepareState(loc, process.env);
  const secrets = JSON.parse(await readFile(join(loc.state, "secrets.json"), "utf8"));
  const env = runtimeEnvironment(loc, config, secrets);
  const controller = new AbortController();
  const children = [];
  const logWrites = [];
  let closing = false;
  let failure;
  let server;
  const finish = reason => {
    if (closing) return;
    closing = true;
    if (reason instanceof Error) failure = reason;
    controller.abort();
  };
  const lockPath = join(loc.state, "run-lock");
  try { await mkdir(lockPath, { mode: 0o700 }); }
  catch (error) { if (error.code === "EEXIST") throw new Error("This checkout already has a dev-stack run lock. Run npm run dev:stack -- down first."); throw error; }
  const onSignal = () => finish();
  for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) process.on(signal, onSignal);
  function supervise(name, command, args, cwd = loc.root) {
    const child = spawn(command, args, { cwd, env, detached: true, stdio: ["ignore", "pipe", "pipe"] });
    children.push(child);
    // Keep output out of the terminal: server errors may contain connection URLs.
    let transcript = "";
    const redact = text => Object.values(secrets).reduce((value, secret) => value.replaceAll(secret, "[redacted]"), text);
    const capture = data => { transcript = (transcript + data.toString()).slice(-128 * 1024); };
    child.stdout.on("data", capture);
    child.stderr.on("data", capture);
    child.on("error", () => finish(new Error(`${name} could not start.`)));
    child.on("exit", (code, signal) => {
      logWrites.push(privateFile(join(loc.state, "logs", `${name}.log`), redact(transcript)));
      if (!closing) finish(new Error(`${name} exited unexpectedly (${signal || code}); the whole development stack is stopping. Inspect .dev-stack/logs/${name}.log.`));
    });
    log(`Started ${name} (pid ${child.pid}).`);
    return child;
  }
  try {
    await privateFile(join(loc.state, "running.json"), JSON.stringify({ pid: process.pid }));
    await rm(join(loc.state, "control.sock"), { force: true });
    server = createServer(socket => {
      let buffer = "";
      socket.setTimeout(2000, () => socket.destroy());
      socket.on("error", () => {});
      socket.on("data", data => {
        buffer += data.toString();
        if (buffer.length > 1024) { socket.destroy(); return; }
        if (!buffer.includes("\n")) return;
        try {
          if (JSON.parse(buffer).token === secrets.control) { socket.end("stopping\n"); finish(); }
          else socket.destroy();
        } catch { socket.destroy(); }
      });
    });
    await new Promise((resolve, reject) => { server.once("error", reject); server.listen(join(loc.state, "control.sock"), resolve); });
    await chmod(join(loc.state, "control.sock"), 0o600);
    await checkPorts(config.ports);
    const template = await readFile(join(loc.root, "packages/run-spaceapp/templates/compose.yml"), "utf8");
    await privateFile(join(loc.state, "compose.json"), JSON.stringify(composeDocument(config, {
      postgres: imageFromCompose(template, "postgres"), temporal: imageFromCompose(template, "temporal")
    }), null, 2));
    log("Starting the checkout's PostgreSQL and Temporal containers (first run downloads pinned images).");
    await compose(loc, "up", "--detach", "--wait", "--wait-timeout", "240");
    controller.signal.throwIfAborted();
    await compose(loc, "exec", "-T", "temporal", "sh", "-ec", `temporal operator namespace describe --namespace ${config.project} --address temporal:7233 >/dev/null 2>&1 || temporal operator namespace create --namespace ${config.project} --address temporal:7233`);
    log("Building source packages and applying migrations.");
    await run("npm", ["run", "build"], { env, signal: controller.signal });
    await run("npm", ["run", "db:migrate"], { env, signal: controller.signal });
    const dockerfile = await readFile(join(loc.root, "Dockerfile"), "utf8");
    const pinned = dockerfile.match(/npm install --global[^\n]* (@opencode\/cli|opencode-ai)@([\w.-]+)/);
    if (!pinned) throw new Error("Cannot find the pinned OpenCode package in Dockerfile.");
    const vendor = join(loc.state, "vendor");
    const installed = JSON.parse(await readFile(join(vendor, "node_modules", pinned[1], "package.json"), "utf8").catch(() => "null"));
    if (installed?.version !== pinned[2]) {
      log(`Installing the Dockerfile's ${pinned[1]}@${pinned[2]} inside .dev-stack/vendor.`);
      await run("npm", ["install", "--prefix", vendor, "--no-audit", "--no-fund", "--save-exact", "--fetch-timeout=30000", "--fetch-retries=1", `${pinned[1]}@${pinned[2]}`], {
        env: { ...env, NODE_OPTIONS: "--dns-result-order=ipv4first" }, signal: controller.signal
      });
    }
    for (const name of ["space_memory.md", "space_memory_monthly.md", "installation-guide.md"]) {
      const destination = join(loc.state, "memory", name);
      if (!(await lstat(destination).catch(() => null))) { await copyFile(join(loc.root, "starter-memory", name), destination); await chmod(destination, 0o600); }
    }
    await prepareWrappers(loc);
    supervise("opencode", join(vendor, "node_modules/.bin/opencode"), ["serve", "--hostname", "127.0.0.1", "--port", String(config.ports.opencode)]);
    supervise("cli-host", process.execPath, ["packages/cli-host/dist/main.js"]);
    supervise("worker", process.execPath, ["apps/worker/dist/worker.js"]);
    supervise("api", process.execPath, ["node_modules/tsx/dist/cli.mjs", "watch", "apps/api/src/server.ts"]);
    supervise("web", process.execPath, [join(loc.root, "node_modules/vite/bin/vite.js"), "--host", "127.0.0.1", "--port", String(config.ports.web), "--strictPort"], join(loc.root, "apps/web"));
    await waitForReady(`http://127.0.0.1:${config.ports.api}/readyz`, { signal: controller.signal });
    log(`Ready: http://127.0.0.1:${config.ports.web}`);
    log(`One-time owner setup token (valid for 15 minutes): ${secrets.setup}`);
    log("Browser Host and optional companions are disabled. API and web reload automatically; run npm run dev:stack -- rebuild after packages/* or worker edits.");
    await new Promise(resolve => { if (controller.signal.aborted) resolve(); else controller.signal.addEventListener("abort", resolve, { once: true }); });
  } catch (error) { if (!closing) failure = error; }
  finally {
    closing = true;
    controller.abort();
    await stopChildren(children);
    await Promise.all(logWrites);
    if (failure) {
      log("Container startup diagnostics (development secrets redacted):");
      await run("docker", ["compose", "--project-name", loc.project, "--file", join(loc.state, "compose.json"), "logs", "--no-color", "--tail", "40"], { env }).catch(() => {});
    }
    if (server) await new Promise(resolve => server.close(resolve));
    await compose(loc, "down", "--remove-orphans").catch(error => { failure ||= error; });
    await rm(join(loc.state, "run-lock"), { recursive: true, force: true });
    await rm(join(loc.state, "running.json"), { force: true });
    await rm(join(loc.state, "control.sock"), { force: true });
    for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) process.off(signal, onSignal);
    log("Development processes and containers stopped. State is retained; reset removes it.");
  }
  if (failure) throw failure;
}

export async function main(command = process.argv[2] || "up") {
  if (process.platform === "win32") throw new Error("Run dev:stack inside WSL2 on Windows 11, with Node 22 and Docker available inside WSL.");
  if (!["up", "down", "reset", "rebuild"].includes(command)) throw new Error("Usage: npm run dev:stack -- [up|down|reset|rebuild]");
  const loc = await location(root);
  if (command === "up") return start(loc);
  if (!(await lstat(loc.state).catch(() => null))) { log("No development stack state exists for this checkout."); return; }
  await ownedState(loc);
  await requestStop(loc);
  if (command === "rebuild") { log("Rebuilding and restarting the development stack."); return start(loc); }
  await compose(loc, "down", "--remove-orphans", ...(command === "reset" ? ["--volumes"] : []));
  if (command === "reset") { await ownedState(loc); await rm(loc.state, { recursive: true }); log("Removed this checkout's containers, volumes and .dev-stack state."); }
  else log("Development stack stopped; data is retained.");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(`[dev:stack] ${error.message}`); process.exitCode = 1; });
}
