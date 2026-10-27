import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

// Explicit maintenance command: ordinary test runs never rewrite baselines.
const vitest = fileURLToPath(new URL("../../../../node_modules/vitest/vitest.mjs", import.meta.url));
const result = spawnSync(process.execPath, [vitest, "run", "tests/temporal-sandbox.test.ts"], {
  cwd: fileURLToPath(new URL("../../", import.meta.url)),
  env: { ...process.env, SPACE_RECORD_WORKFLOW_HISTORIES: "1" },
  stdio: "inherit"
});
if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
