#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn, execFile } from "node:child_process";
import { createServer } from "node:net";
import { lstat, readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { setTimeout as delay } from "node:timers/promises";
import WebSocket from "ws";
import { waitForReady, run } from "./dev-stack.mjs";
import { location, portsFromEnv } from "./dev-stack-config.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const loc = await location(root);
assert.equal(await lstat(loc.state).catch(() => null), null, "Smoke requires a fresh checkout: existing .dev-stack data is never reset by this test.");
const ports = portsFromEnv(process.env);
const began = Date.now();
const up = spawn(process.execPath, ["scripts/dev-stack.mjs"], { cwd: root, env: process.env, stdio: ["ignore", "pipe", "pipe"] });
let output = "";
let failure;
const events = [];
let pendingLine = "";
const capture = data => {
  output = (output + data.toString()).slice(-4 * 1024 * 1024);
  pendingLine += data.toString();
  const lines = pendingLine.split("\n");
  pendingLine = lines.pop();
  for (const line of lines) if (!line.includes("One-time owner setup token")) process.stdout.write(line.replace(/[a-f0-9]{64}/g, "[digest-or-secret]") + "\n");
};
up.stdout.on("data", capture);
up.stderr.on("data", capture);
const stopped = new Promise(resolve => up.on("close", (code, signal) => resolve({ code, signal })));
const abort = new AbortController();
up.on("error", () => abort.abort());
up.on("exit", () => abort.abort());
let socket;
try {
  await waitForReady(`http://127.0.0.1:${ports.api}/readyz`, { signal: abort.signal, timeout: 900_000 });
  const origin = `http://127.0.0.1:${ports.web}`;
  const ready = await fetch(`${origin}/readyz`).then(r => r.json());
  assert.equal(ready.ok, true);
  assert.notEqual(ready.dependencies.worker, "disabled");
  console.log("PASS real PostgreSQL migrations, Temporal worker pollers, API readiness and Vite proxy");
  const secrets = JSON.parse(await readFile(join(loc.state, "secrets.json"), "utf8"));
  let cookie = "";
  let csrf;
  const call = async (path, body, method = body === undefined ? "GET" : "POST") => {
    const response = await fetch(`${origin}${path}`, { method,
      headers: { ...(body === undefined ? {} : { "content-type": "application/json" }), ...(cookie ? { cookie } : {}), ...(csrf ? { [csrf.headerName]: csrf.csrfToken } : {}) },
      body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(120_000) });
    const newCookie = response.headers.getSetCookie().find(item => item.startsWith("space_session="));
    if (newCookie) cookie = newCookie.split(";")[0];
    const data = await response.json();
    assert.equal(response.ok, true, `${path}: HTTP ${response.status}, code ${data.error?.code ?? "unknown"}`);
    return data;
  };
  const before = await call("/api/auth/me");
  assert.equal(before.isAuthenticated, false, "dev-login must not authenticate the smoke");
  const credentials = { email: "dev-stack-smoke@example.invalid", password: "Development smoke only 42!" };
  const claimed = await call("/api/setup/claim", { token: secrets.setup, ...credentials });
  assert.equal(claimed.isAuthenticated, true);
  assert.ok(claimed.starterRoomId);
  cookie = "";
  const loggedIn = await call("/api/auth/login", credentials);
  assert.equal(loggedIn.isAuthenticated, true);
  csrf = await call("/api/auth/csrf");
  console.log("PASS one-time owner setup and ordinary password sign-in (no dev-login)");
  const rooms = await call("/api/rooms");
  const room = rooms.data.find(item => item.id === claimed.starterRoomId);
  const panes = await call(`/api/panes?roomId=${encodeURIComponent(claimed.starterRoomId)}`);
  const pane = panes.data.find(item => item.terminalRuntimeId === "cli:opencode");
  assert.ok(pane, "Getting Started must contain an OpenCode pane");
  assert.equal(room?.name, "Getting Started");
  const session = await call(`/api/panes/${encodeURIComponent(pane.id)}/cli/session`, { runtimeId: "cli:opencode", cwd: join(loc.state, "workspaces") });
  assert.ok(session.websocket?.token);
  const query = new URLSearchParams({ sessionId: session.session.sessionId, token: session.websocket.token, initialCols: "100", initialRows: "30" });
  socket = new WebSocket(`ws://127.0.0.1:${ports.web}/api/panes/${encodeURIComponent(pane.id)}/cli/terminal?${query}`, { headers: { cookie, origin } });
  let terminal = "";
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("OpenCode terminal did not render its UI within 90 seconds.")), 90_000);
    socket.on("error", reject);
    socket.on("message", data => {
      try {
        const event = JSON.parse(data.toString());
        events.push(event.type);
        if (event.type === "error") { clearTimeout(timer); reject(new Error(`Terminal: ${event.code}`)); }
        if (event.type === "output") {
          terminal = (terminal + event.data).slice(-1024 * 1024);
          // Require actual OpenCode UI text, not just a host allocation or an ANSI escape.
          if (/opencode/i.test(terminal) && /model|session|ask|prompt|build|agent/i.test(terminal)) { clearTimeout(timer); resolve(); }
        }
      } catch (error) { clearTimeout(timer); reject(error); }
    });
  });
  await delay(1500);
  const observed = await call(`/api/panes/${encodeURIComponent(pane.id)}/cli/session?includeTranscript=false`);
  assert.equal(observed.session.status, "RUNNING");
  console.log("PASS Getting Started opens a real OpenCode UI through API + WebSocket + source CLI host + PTY");
  socket.close();
  socket = undefined;
  // A supervised process loss must fail loudly and take the rest of the stack down.
  const workerPid = Number(output.match(/Started worker \(pid (\d+)\)/)?.[1]);
  assert.ok(workerPid > 1);
  process.kill(workerPid, "SIGTERM");
  const exit = await Promise.race([stopped, delay(45_000).then(() => { throw new Error("Worker death did not stop the supervisor."); })]);
  assert.notEqual(exit.code, 0);
  assert.match(output, /worker exited unexpectedly/);
  console.log("PASS worker loss fails clearly and stops the remaining processes");
} catch (error) { failure = error; }
finally {
  socket?.terminate();
  if (failure) {
    for (const name of await readdir(join(loc.state, "logs")).catch(() => [])) {
      const text = await readFile(join(loc.state, "logs", name), "utf8").catch(() => "");
      output += `\n--- ${name} ---\n${text.slice(-12_000)}\n`;
    }
  }
  try { await run(process.execPath, ["scripts/dev-stack.mjs", "reset"]); }
  catch {
    up.kill("SIGTERM");
    await Promise.race([stopped, delay(30_000)]);
    await run(process.execPath, ["scripts/dev-stack.mjs", "reset"]).catch(error => { failure ||= error; });
  }
  await Promise.race([stopped, delay(30_000).then(() => { throw new Error("Development supervisor did not exit during cleanup."); })]).catch(error => { failure ||= error; });
}
try {
  assert.equal(await lstat(loc.state).catch(() => null), null, "reset must remove owned state");
  const exec = promisify(execFile);
  for (const args of [["ps", "--all", "--quiet", "--filter", `label=com.docker.compose.project=${loc.project}`], ["volume", "ls", "--quiet", "--filter", `label=com.docker.compose.project=${loc.project}`]]) {
    assert.equal((await exec("docker", args)).stdout.trim(), "", "reset must leave no owned containers or volumes");
  }
  for (const port of Object.values(ports)) {
    const server = createServer();
    await new Promise((resolve, reject) => { server.once("error", reject); server.listen(port, "127.0.0.1", resolve); });
    await new Promise(resolve => server.close(resolve));
  }
  for (const match of output.matchAll(/Started [\w-]+ \(pid (\d+)\)/g)) {
    assert.throws(() => process.kill(Number(match[1]), 0), { code: "ESRCH" }, "owned source process leaked after reset");
  }
  console.log("PASS reset removed all owned containers, volumes, source processes, listeners and state");
} catch (error) { failure ||= error; }
if (failure) {
  // Logs may contain the explicitly printed setup token; omit all long hex secrets from proof output.
  console.error(output.replace(/[a-f0-9]{64}/g, "[redacted]"));
  throw failure;
}
console.log(JSON.stringify({ passed: true, platform: process.platform, wsl: Boolean(process.env.WSL_DISTRO_NAME), seconds: Math.round((Date.now() - began) / 1000), terminalEvents: events.length, cleaned: true }));
