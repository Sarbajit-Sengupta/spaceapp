# Synthetic workflow replay baselines

These histories come from the production workflow code running on
`TestWorkflowEnvironment.createTimeSkipping()` with stubbed activities. They
contain fictional rooms, turns, goals and memory records. Worker identities and
failure stack traces are replaced with synthetic values before serialization.
No installed SpaceApp, database, provider account or production history is used.

Run the suite after the internal packages have been built (`npm run check`):

```sh
npm test -w @space/worker
```

The SDK downloads and caches its test-server binary on first use; subsequent
runs use that executable. Workflows and activities do not call external services.
The tests run serially because virtual time is global to each test server. The
250-poll scenarios use dedicated servers to isolate their clock locks.

## Recording

```sh
npm run temporal:record-histories -w @space/worker
```

Ordinary tests only read the committed baselines. The explicit recording command
runs the same scenarios and writes protobuf JSON using `History.toObject` with
string longs/base64 bytes, then the replay test decodes with `History.fromObject`.
This avoids dependency on protobuf reflection across the test runner's module
boundaries. Each workflow has a baseline; additional files cover wake-up,
timeout, continue-as-new, bounded carried state, action failure, and both
supervisor branches.

`tests/support/prepatch-workflows.ts` freezes the pre-continuation supervisor
implementation so its histories contain no `room-background-continuation-v1`
marker. The replay gate always uses **current production** workflow code for
these histories. The historical entrypoint is never registered in production.
The negative entrypoint inserts a timer before the first goal activity and must
produce `DeterminismViolationError` against the committed goal history.

Re-recording is allowed only together with a reviewed, replay-safe `patched()`
workflow change. Keep the old histories for the old patch branch and add new
histories for the new branch. Never replace a failing baseline merely to make
replay pass, and never edit recorded event order, activity arguments or patch
markers by hand. Initial baseline creation does not change workflow behavior.

The tests are discovered by the existing worker `vitest run` script and thus by
`npm run test:public` and the public CI workflow.
