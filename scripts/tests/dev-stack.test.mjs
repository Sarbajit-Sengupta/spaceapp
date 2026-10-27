import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, mkdir, readFile, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { composeDocument, imageFromCompose, location, ownedState, portsFromEnv, prepareState, privateFile, runtimeEnvironment } from "../dev-stack-config.mjs";

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "sd-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  return location(root);
}

test("port configuration is distinct, unprivileged, configurable and separate from installations", () => {
  assert.equal(portsFromEnv({}).web, 14911);
  assert.equal(portsFromEnv({ SPACE_DEV_WEB_PORT: "24911" }).web, 24911);
  for (const value of ["", "4910", "4911", "80", "65536", "NaN", "1e2", "12000.5"]) assert.throws(() => portsFromEnv({ SPACE_DEV_WEB_PORT: value }));
  assert.throws(() => portsFromEnv({ SPACE_DEV_WEB_PORT: "14910" }), /distinct/);
});

test("reads each digest from the launcher's actual service, never a nearby image", async () => {
  const source = await readFile(new URL("../../packages/run-spaceapp/templates/compose.yml", import.meta.url), "utf8");
  assert.match(imageFromCompose(source, "postgres"), /^pgvector\/pgvector:.*@sha256:/);
  assert.match(imageFromCompose(source, "temporal"), /^temporalio\/auto-setup:.*@sha256:/);
  assert.throws(() => imageFromCompose(source, "spaceapp-core"), /digest-pinned/);
  assert.throws(() => imageFromCompose("  postgres:\n    image: postgres:latest\n", "postgres"));
  assert.throws(() => imageFromCompose("  unrelated:\n    image: postgres@sha256:" + "a".repeat(64), "postgres"));
});

test("compose exposes only loopback and owns a separate volume/project", async t => {
  const loc = await fixture(t);
  const config = await prepareState(loc, {});
  const doc = composeDocument(config, { postgres: "pg", temporal: "tm" });
  assert.match(doc.name, /^spaceapp-dev-[a-f0-9]{12}$/);
  for (const service of Object.values(doc.services)) for (const port of service.ports) assert.match(port, /^127\.0\.0\.1:/);
  assert.equal(doc.services.postgres.environment.POSTGRES_DB, "spaceapp_dev");
  assert.deepEqual(doc.services.postgres.volumes, ["postgres:/var/lib/postgresql/data"]);
  assert.ok(!JSON.stringify(doc).includes("SPACE_SESSION_SECRET"));
});

test("private state survives restarts, secrets differ between checkouts and files are mode 0600", async t => {
  const first = await fixture(t);
  const second = await fixture(t);
  const config = await prepareState(first, {});
  const secret = await readFile(join(first.state, "secrets.json"), "utf8");
  assert.deepEqual(await prepareState(first, { SPACE_DEV_WEB_PORT: "24911" }), config);
  assert.equal(await readFile(join(first.state, "secrets.json"), "utf8"), secret);
  await prepareState(second, {});
  assert.notEqual(await readFile(join(second.state, "secrets.json"), "utf8"), secret);
  if (process.platform !== "win32") {
    for (const name of ["owner.json", "secrets.json", "postgres-password", "setup-token"]) assert.equal((await stat(join(first.state, name))).mode & 0o777, 0o600);
    assert.equal((await stat(first.state)).mode & 0o777, 0o700);
  }
});

test("runtime ignores inherited credentials, login shortcuts and installed-instance paths", async t => {
  const loc = await fixture(t);
  const config = await prepareState(loc, {});
  const secrets = JSON.parse(await readFile(join(loc.state, "secrets.json"), "utf8"));
  const env = runtimeEnvironment(loc, config, secrets, {
    PATH: "/usr/bin", OPENAI_API_KEY: "do-not-inherit", NODE_OPTIONS: "--require=/tmp/attack", SPACE_DEV_LOGIN: "true", SPACE_DATABASE_URL: "postgres://production", SPACE_MEMORY_ROOT: "/installed/memory"
  });
  assert.equal(env.OPENAI_API_KEY, undefined);
  assert.equal(env.NODE_OPTIONS, undefined);
  assert.equal(env.SPACE_DEV_LOGIN, "false");
  assert.equal(new URL(env.SPACE_DATABASE_URL).pathname, "/spaceapp_dev");
  assert.equal(env.SPACE_MEMORY_ROOT, join(loc.state, "memory"));
  assert.equal(env.SPACE_INSTANCE, "dev");
  assert.equal(env.SPACE_TEMPORAL_NAMESPACE, loc.project);
  assert.equal(env.SPACE_API_HOST, "127.0.0.1");
  assert.equal(env.SPACE_WEB_HOST, "127.0.0.1");
  assert.equal(env.SPACE_CLI_ROOT_ENABLED, "false");
});

test("refuses foreign state and symlinked state/files instead of overwriting outside the checkout", async t => {
  const loc = await fixture(t);
  await mkdir(loc.state);
  await writeFile(join(loc.state, "unrelated"), "keep");
  await assert.rejects(prepareState(loc, {}));
  assert.equal(await readFile(join(loc.state, "unrelated"), "utf8"), "keep");
  await writeFile(join(loc.state, "owner.json"), JSON.stringify({ version: 1, root: "another-checkout", project: loc.project }));
  await assert.rejects(ownedState(loc), /ownership/);
  if (process.platform === "win32") return;
  const other = await fixture(t);
  await symlink(loc.state, other.state);
  await assert.rejects(location(other.root), /symlink/);
  const outside = join(loc.root, "outside");
  await writeFile(outside, "keep");
  await symlink(outside, join(loc.state, "secret-link"));
  await assert.rejects(privateFile(join(loc.state, "secret-link"), "overwrite"), /non-regular/);
  assert.equal(await readFile(outside, "utf8"), "keep");
});
