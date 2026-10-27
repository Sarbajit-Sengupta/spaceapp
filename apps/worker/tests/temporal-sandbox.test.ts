import { afterAll, afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import type { WorkflowHandle } from "@temporalio/client";
import type { RoomAgentSupervisorInput } from "@space/contracts";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker, bundleWorkflowCode } from "@temporalio/worker";
import { ApplicationFailure, TimeoutFailure, TimeoutType, defaultPayloadConverter } from "@temporalio/common";
import { temporal } from "@temporalio/proto";
import { Context } from "@temporalio/activity";
import { readFile, readdir, mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import { DOT_GOAL_SIGNAL, ROOM_AGENT_ENQUEUE_SIGNAL, ROOM_AGENT_STOP_SIGNAL, roomAgentSupervisorInputSchema, roomAgentSupervisorQueueItemSchema } from "@space/contracts";

import { continuationFixture } from "./room-continuation-fixture.js";
import { discardSupervisorTurn, finishSupervisorTurn, prepareBackgroundContinuation, supervisorTurnState } from "../src/room-background-continuation.js";
vi.setConfig({ testTimeout: 30_000 });
// No Temporal APIs are mocked. Only activity implementations are replaced.
const completed = { status: "COMPLETED", message: "Synthetic result", roomAgentOutcome: {
  status: "VERIFIED", statusReason: "Synthetic verification", executedActionCount: 1
} };
const turn = (id = "one") => ({ roomId: "room:synthetic", paneId: "pane:synthetic", prompt: "Synthetic request",
  traceId: `trace:${id}`, agentRunId: `agent_run:${id}`, artifactIds: [], permissionMode: null, collaborationMode: "default" });
const item = (id = "one") => roomAgentSupervisorQueueItemSchema.parse({ missionId: `mission:${id}`, turn: turn(id) });
const supervisorInput = (overrides = {}) => roomAgentSupervisorInputSchema.parse({ roomId: "room:synthetic", ...overrides });
const fixtureDir = new URL("./fixtures/workflow-histories/", import.meta.url);
const source = (name: string) => fileURLToPath(new URL(`./support/${name}.ts`, import.meta.url));
const record = process.env.SPACE_RECORD_WORKFLOW_HISTORIES === "1";
let env: TestWorkflowEnvironment;
let bundle: Awaited<ReturnType<typeof bundleWorkflowCode>>;
let brokenBundle: Awaited<ReturnType<typeof bundleWorkflowCode>>;
const workers: Worker[] = [];
const running: Promise<void>[] = [];
const handles: WorkflowHandle[] = [];
let activities: Record<string, ReturnType<typeof vi.fn>>;

async function eventually(check: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + 40_000;
  while (!(await check())) {
    if (Date.now() >= deadline) throw Error("Timed out waiting for sandbox evidence");
    await delay(10);
  }
}
async function capture(name: string, handle: WorkflowHandle) {
  if (!record) return;
  await mkdir(fixtureDir, { recursive: true });
  await writeFile(new URL(`${name}.json`, fixtureDir), JSON.stringify(temporal.api.history.v1.History.toObject(await handle.fetchHistory(), { longs: String, bytes: String, enums: Number }), (key, value) => key === "identity" ? "synthetic-worker" : key === "stackTrace" ? "Synthetic test failure" : value, 2) + "\n");
}
async function start(type: string, input: unknown, queue = "sandbox-main", followRuns = true) {
  const handle = await env.client.workflow.start(type, { workflowId: `synthetic-${randomUUID()}`, taskQueue: queue,
    args: [input], followRuns });
  const pinned = env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId, { followRuns });
  handles.push(pinned);
  return pinned;
}
async function execute(type: string, input: unknown, fixture?: string) {
  const handle = await start(type, input);
  const result = await handle.result();
  if (fixture) await capture(fixture, handle);
  return result;
}
async function terminate(handle: WorkflowHandle) {
  await handle.terminate("Synthetic test finished");
}
async function nextRunInput<T = RoomAgentSupervisorInput>(handle: WorkflowHandle): Promise<T> {
  await eventually(async () => (await handle.describe()).status.name === "CONTINUED_AS_NEW");
  const history = await handle.fetchHistory();
  const event = history.events?.find((e) => e.workflowExecutionContinuedAsNewEventAttributes);
  return defaultPayloadConverter.fromPayload(event!.workflowExecutionContinuedAsNewEventAttributes!.input!.payloads![0]!) as T;
}

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping();
  bundle = await bundleWorkflowCode({ workflowsPath: source("sandbox-workflows") });
  const legacy = await bundleWorkflowCode({ workflowsPath: source("prepatch-workflows") });
  brokenBundle = await bundleWorkflowCode({ workflowsPath: source("nondeterministic-workflows") });
  // Keep workers running for the suite. Fixed queue names match the production
  // workflow's cross-queue activity/child routing inside this disposable server.
  const activityNames = ["recordDummyTurnStarted", "recordDummyTurnCompleted", "runCodexAppServerTurn", "runOpenCodeAgentTurn", "runCliAgentTurn",
    "dotGoalStep", "dotGoalObserve", "checkRoomAgentSupervisorTurn", "prepareRoomAgentBackgroundContinuation", "discardRoomAgentSupervisorTurn", "finishRoomAgentSupervisorTurn",
    "markRoomAgentMissionStarted", "markRoomAgentMissionFinished", "markRoomAgentMissionContinued", "settleRoomAgentBackgroundMission",
    "runRoomAgentBackgroundAction", "failRoomAgentBackgroundAction", "refreshMemoryGraphSnapshot", "refreshAllMonthsMemoryGraphSnapshot",
    "reconcileStaleSpaceAgentSessions", "createScheduledMemoryAudit", "prepareMemoryConsolidation", "executePersistedMemoryChangeSet", "finalizeMemoryRepair"];
  activities = Object.fromEntries(activityNames.map(name => [name, vi.fn()]));
  for (const [taskQueue, workflowBundle] of [["sandbox-main", bundle], ["sandbox-legacy", legacy], ["space-memory-maintenance", bundle], ["space-memory-mutations", bundle]] as const) {
    const worker = await Worker.create({ connection: env.nativeConnection, taskQueue, workflowBundle, activities,
      shutdownGraceTime: "1 second" });
    workers.push(worker);
    running.push(worker.run());
  }
}, 90_000);

beforeEach(() => {
  for (const fn of Object.values(activities)) fn.mockReset().mockResolvedValue(undefined);
  activities.recordDummyTurnCompleted.mockResolvedValue(completed);
  activities.runCodexAppServerTurn.mockResolvedValue(completed);
  activities.runOpenCodeAgentTurn.mockResolvedValue(completed);
  activities.runCliAgentTurn.mockResolvedValue(completed);
  activities.checkRoomAgentSupervisorTurn.mockResolvedValue("RUN");
  activities.prepareRoomAgentBackgroundContinuation.mockResolvedValue({ state: "DONE" });
  activities.settleRoomAgentBackgroundMission.mockResolvedValue(true);
  activities.dotGoalStep.mockResolvedValue({ state: "DONE", goalStatus: "COMPLETED", runId: null });
  activities.dotGoalObserve.mockResolvedValue({ run: "COMPLETED", goalStatus: "RUNNING" });
  activities.runRoomAgentBackgroundAction.mockResolvedValue({ status: "COMPLETED" });
  activities.refreshMemoryGraphSnapshot.mockResolvedValue({ sourceHash: "synthetic-hash" });
  activities.createScheduledMemoryAudit.mockResolvedValue({ id: "audit:synthetic", workflowId: `synthetic-consolidation-${randomUUID()}` });
  activities.prepareMemoryConsolidation.mockResolvedValue({ changeSetId: null, run: { id: "audit:synthetic", status: "COMPLETED" } });
  activities.executePersistedMemoryChangeSet.mockResolvedValue({ status: "COMPLETED", changeSetId: "change:synthetic" });
  activities.finalizeMemoryRepair.mockResolvedValue({ id: "audit:synthetic", status: "COMPLETED" });
});

afterEach(async () => {
  for (const handle of handles.splice(0)) {
    try { await env.client.workflow.getHandle(handle.workflowId).terminate("Synthetic test finished"); } catch { /* Already closed. */ }
  }
});
afterAll(async () => {
  try {
    for (const handle of handles) { try { await terminate(handle); } catch { /* Already closed. */ } }
    workers.forEach(worker => worker.shutdown());
    await Promise.all(running);
  } finally { await env?.teardown(); }
}, 30_000);

it("retries a dummy activity and records the production workflow", async () => {
  activities.recordDummyTurnStarted.mockRejectedValueOnce(Error("Synthetic transient failure"));
  expect(await execute("dummyTurnWorkflow", turn(), "dummy-turn")).toEqual(completed);
  expect(activities.recordDummyTurnStarted).toHaveBeenCalledTimes(2);
  expect(activities.recordDummyTurnCompleted).toHaveBeenCalledOnce();
}, 20_000);

it.each([[undefined, "runCodexAppServerTurn"], ["opencode", "runOpenCodeAgentTurn"], ["cli:claude", "runCliAgentTurn"]])(
  "runs a real turn workflow for provider %s", async (providerId, activity) => {
    expect(await execute("codexAppServerTurnWorkflow", { ...turn(), providerId }, providerId ? undefined : "codex-turn")).toEqual(completed);
    expect(activities[activity]).toHaveBeenCalledOnce();
  });
it("retries a transient native turn failure", async () => {
  activities.runCodexAppServerTurn.mockRejectedValueOnce(Error("Synthetic retry"));
  expect(await execute("codexAppServerTurnWorkflow", turn())).toEqual(completed);
  expect(activities.runCodexAppServerTurn).toHaveBeenCalledTimes(2);
});
it("propagates a nonretryable turn failure", async () => {
  activities.runOpenCodeAgentTurn.mockRejectedValue(ApplicationFailure.nonRetryable("Synthetic failure", "SYNTHETIC_TURN_FAILED"));
  await expect(execute("codexAppServerTurnWorkflow", { ...turn(), providerId: "opencode" })).rejects.toThrow();
  expect(activities.runOpenCodeAgentTurn).toHaveBeenCalledOnce();
});
it("finishes a goal and records its history", async () => {
  expect(await execute("dotGoalWorkflow", { goalId: "goal:synthetic" }, "dot-goal")).toEqual({ goalStatus: "COMPLETED" });
});
it("keeps a wake signal received during an activity", async () => {
  let release!: () => void;
  activities.dotGoalStep.mockImplementationOnce(async () => {
    await new Promise<void>(resolve => { release = resolve; });
    return { state: "PAUSED", runId: null, goalStatus: "RUNNING" };
  });
  const handle = await start("dotGoalWorkflow", { goalId: "goal:wake" });
  await eventually(() => Boolean(release));
  await handle.signal(DOT_GOAL_SIGNAL);
  release();
  expect(await handle.result()).toEqual({ goalStatus: "COMPLETED" });
  const history = await handle.fetchHistory();
  expect(history.events?.filter((e) => e.timerFiredEventAttributes)).toHaveLength(0);
  await capture("dot-goal-wake", handle);
});
it("fires the goal timer when no signal arrives", async () => {
  activities.dotGoalStep.mockResolvedValueOnce({ state: "WAITING", runId: null, goalStatus: "RUNNING" });
  const handle = await start("dotGoalWorkflow", { goalId: "goal:timeout" });
  expect(await handle.result()).toEqual({ goalStatus: "COMPLETED" });
  expect((await handle.fetchHistory()).events?.some((e) => e.timerFiredEventAttributes)).toBe(true);
  await capture("dot-goal-timeout", handle);
});
it("continues an unfinished goal as new after 300 steps", async () => {
  let steps = 0;
  activities.dotGoalStep.mockImplementation(async () => ++steps <= 300
    ? { state: "WAITING", runId: null, goalStatus: "RUNNING" }
    : { state: "DONE", runId: null, goalStatus: "COMPLETED" });
  const handle = await start("dotGoalWorkflow", { goalId: "goal:continue" }, "sandbox-main", false);
  await expect(handle.result()).rejects.toThrow();
  expect(await nextRunInput(handle)).toEqual({ goalId: "goal:continue" });
  await capture("dot-goal-continue", handle);
  // Let the next run close before resetting activity implementations.
  expect(await env.client.workflow.getHandle(handle.workflowId).result()).toEqual({ goalStatus: "COMPLETED" });
}, 30_000);
it("records background action cleanup after exhausted activity retries", async () => {
  activities.runRoomAgentBackgroundAction.mockRejectedValue(Error("Synthetic transport failure"));
  const handle = await start("roomAgentActionWorkflow", { bridge: { roomId: "room:synthetic", missionId: "mission:synthetic" }, traceId: "trace:synthetic" });
  await expect(handle.result()).rejects.toThrow();
  expect(activities.runRoomAgentBackgroundAction).toHaveBeenCalledTimes(5);
  expect(activities.failRoomAgentBackgroundAction).toHaveBeenCalledWith(expect.objectContaining({
    roomId: "room:synthetic", missionId: "mission:synthetic", cancelled: false, actionId: handle.workflowId
  }));
  await capture("room-action-failure", handle);
});
it("continues maintenance after optional refresh and reconciliation failures and runs its child", async () => {
  activities.refreshAllMonthsMemoryGraphSnapshot.mockRejectedValue(Error("Synthetic refresh failure"));
  activities.reconcileStaleSpaceAgentSessions.mockRejectedValue(Error("Synthetic reconciliation failure"));
  expect(await execute("memoryMaintenanceWorkflow", { traceId: "trace:maintenance" }, "memory-maintenance")).toEqual({ sourceHash: "synthetic-hash" });
  expect(activities.refreshAllMonthsMemoryGraphSnapshot).toHaveBeenCalledTimes(3);
  expect(activities.reconcileStaleSpaceAgentSessions).toHaveBeenCalledTimes(3);
  expect(activities.prepareMemoryConsolidation).toHaveBeenCalledOnce();
});
it("runs consolidation mutation on the production mutation queue and finalizes", async () => {
  activities.prepareMemoryConsolidation.mockResolvedValue({ changeSetId: "change:synthetic", run: { id: "audit:synthetic" } });
  activities.executePersistedMemoryChangeSet.mockRejectedValueOnce(Error("Synthetic mutation retry"));
  expect(await execute("memoryConsolidationWorkflow", { runId: "audit:synthetic", traceId: "trace:synthetic" }, "memory-consolidation")).toEqual({ id: "audit:synthetic", status: "COMPLETED" });
  expect(activities.executePersistedMemoryChangeSet).toHaveBeenCalledTimes(2);
  expect(activities.finalizeMemoryRepair).toHaveBeenCalledOnce();
});
it("retries a memory mutation in the real sandbox", async () => {
  activities.executePersistedMemoryChangeSet.mockRejectedValueOnce(Error("Synthetic mutation retry"));
  expect(await execute("memoryMutationWorkflow", { changeSetId: "change:synthetic", traceId: "trace:synthetic" }, "memory-mutation")).toEqual({ status: "COMPLETED", changeSetId: "change:synthetic" });
  expect(activities.executePersistedMemoryChangeSet).toHaveBeenCalledTimes(2);
});
it("propagates an exhausted memory mutation failure", async () => {
  activities.executePersistedMemoryChangeSet.mockRejectedValue(Error("Synthetic permanent failure"));
  await expect(execute("memoryMutationWorkflow", { changeSetId: "change:synthetic", traceId: "trace:synthetic" })).rejects.toThrow();
  expect(activities.executePersistedMemoryChangeSet).toHaveBeenCalledTimes(3);
});

it.each(["sandbox-main", "sandbox-legacy"])("processes enqueue signals on %s", async queue => {
  const handle = await start("roomAgentSupervisorWorkflow", supervisorInput(), queue);
  await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, item("signal"));
  const finish = queue === "sandbox-main" ? activities.finishRoomAgentSupervisorTurn : activities.markRoomAgentMissionFinished;
  await eventually(() => finish.mock.calls.length === 1);
  expect(activities.runCodexAppServerTurn).toHaveBeenCalledOnce();
  await terminate(handle);
  await capture(queue === "sandbox-main" ? "room-supervisor" : "room-supervisor-prepatch", handle);
});
it.each(["sandbox-main", "sandbox-legacy"])("carries pending state after 50 processed turns on %s", async queue => {
  const handle = await start("roomAgentSupervisorWorkflow", supervisorInput({ pending: [item("first"), item("carried")], processedCount: 49 }), queue, false);
  // result() unlocks time skipping, including retry/child completion timers.
  const outcome = handle.result().catch(() => undefined);
  const carried = await nextRunInput(handle);
  expect(carried.processedCount).toBe(0);
  expect(carried.pending).toEqual([item("carried")]);
  await outcome;
  await capture(queue === "sandbox-main" ? "room-supervisor-continue" : "room-supervisor-prepatch-continue", handle);
  const finish = queue === "sandbox-main" ? activities.finishRoomAgentSupervisorTurn : activities.markRoomAgentMissionFinished;
  await eventually(() => finish.mock.calls.length === 2);
  await env.client.workflow.getHandle(handle.workflowId).terminate("Synthetic test finished");
}, 20_000);
it.each(["sandbox-main", "sandbox-legacy"])("polls every second and carries monitoring after 250 polls on %s", async queue => {
  // Virtual time is global to a test server. Give these long-running polling
  // histories their own server so no result waiter from another run can hold
  // its clock lock while this case advances time.
  const local = await TestWorkflowEnvironment.createTimeSkipping();
  const workflowBundle = queue === "sandbox-main" ? bundle : await bundleWorkflowCode({ workflowsPath: source("prepatch-workflows") });
  const poll = vi.fn(async () => queue === "sandbox-main" ? { state: "WAITING" } : false);
  const worker = await Worker.create({ connection: local.nativeConnection, taskQueue: queue, workflowBundle,
    activities: { prepareRoomAgentBackgroundContinuation: poll, settleRoomAgentBackgroundMission: poll } });
  try {
    await worker.runUntil(async () => {
      const started = await local.client.workflow.start("roomAgentSupervisorWorkflow", {
        workflowId: `synthetic-poll-${randomUUID()}`, taskQueue: queue, args: [supervisorInput({ monitoringMissionIds: ["mission:monitor"] })]
      });
      const handle = local.client.workflow.getHandle(started.workflowId, started.firstExecutionRunId, { followRuns: false });
      try {
        await local.sleep("251 seconds");
        const carried = await nextRunInput(handle);
        expect(carried.monitoringMissionIds).toEqual(["mission:monitor"]);
        expect(poll.mock.calls.length).toBeGreaterThanOrEqual(250);
        const history = await handle.fetchHistory();
        expect(history.events?.filter((e) => e.timerFiredEventAttributes)).toHaveLength(250);
        await capture(queue === "sandbox-main" ? "room-supervisor-poll" : "room-supervisor-prepatch-poll", handle);
      } finally { await local.client.workflow.getHandle(started.workflowId).terminate("Synthetic test finished"); }
    });
  } finally { await local.teardown(); }
}, 60_000);
it("replays every committed synthetic history against current production workflows", async () => {
  const names = (await readdir(fixtureDir)).filter(name => name.endsWith(".json")).sort();
  expect(names.length).toBeGreaterThanOrEqual(8);
  for await (const result of Worker.runReplayHistories({ workflowBundle: bundle }, await Promise.all(names.map(async name => ({
    workflowId: name, history: temporal.api.history.v1.History.fromObject(JSON.parse(await readFile(new URL(name, fixtureDir), "utf8")))
  }))))) {
    expect(result.error, `Replay failed: ${result.workflowId}`).toBeUndefined();
  }
}, 30_000);
it("rejects an extra timer with DeterminismViolationError", async () => {
  const history = temporal.api.history.v1.History.fromObject(JSON.parse(await readFile(new URL("dot-goal.json", fixtureDir), "utf8")));
  const results = [];
  for await (const result of Worker.runReplayHistories({ workflowBundle: brokenBundle }, [{ workflowId: "dot-goal.json", history }])) results.push(result);
  expect(results).toHaveLength(1);
  expect(results[0].error?.name).toBe("DeterminismViolationError");
});

it.each(["sandbox-main", "sandbox-legacy"])("interrupts a running child with a stop signal on %s", async queue => {
  let started = false;
  activities.runCodexAppServerTurn.mockImplementation(async () => {
    started = true;
    // Heartbeating makes the server deliver the child's cancellation to the
    // real activity context, rather than throwing a fake cancellation error.
    const interval = setInterval(() => Context.current().heartbeat(), 20);
    try { await Context.current().cancelled; } finally { clearInterval(interval); }
  });
  const handle = await start("roomAgentSupervisorWorkflow", supervisorInput({ pending: [item("active")] }), queue);
  await eventually(() => started);
  await handle.signal(ROOM_AGENT_STOP_SIGNAL, { missionId: "mission:active", reason: "Synthetic operator stop" });
  const finish = queue === "sandbox-main" ? activities.finishRoomAgentSupervisorTurn : activities.markRoomAgentMissionFinished;
  await eventually(() => finish.mock.calls.some(([value]) => value.status === "INTERRUPTED"));
  expect(finish.mock.calls[0][0]).toMatchObject({ status: "INTERRUPTED", statusReason: "Synthetic operator stop" });
  await terminate(handle);
});
it("retires a prepared automatic turn when a stop arrives before the activity reply", async () => {
  let release!: () => void;
  const automatic = item("automatic");
  activities.prepareRoomAgentBackgroundContinuation.mockImplementationOnce(async () => {
    await new Promise<void>(resolve => { release = resolve; });
    return { state: "READY", item: automatic };
  });
  const handle = await start("roomAgentSupervisorWorkflow", supervisorInput({ monitoringMissionIds: [automatic.missionId] }));
  await env.sleep("1 second");
  await eventually(() => Boolean(release));
  await handle.signal(ROOM_AGENT_STOP_SIGNAL, { missionId: null, reason: "Synthetic stop" });
  release();
  await eventually(() => activities.discardRoomAgentSupervisorTurn.mock.calls.length === 1);
  expect(activities.discardRoomAgentSupervisorTurn.mock.calls[0][0]).toMatchObject({ item: automatic, reason: "Superseded while preparing the automatic continuation." });
  expect(activities.runCodexAppServerTurn).not.toHaveBeenCalled();
  await terminate(handle);
});
it("retires a prepared automatic turn and executes the newer operator request", async () => {
  let release!: () => void;
  const automatic = item("automatic-followup");
  activities.prepareRoomAgentBackgroundContinuation.mockImplementationOnce(async () => {
    await new Promise<void>(resolve => { release = resolve; });
    return { state: "READY", item: automatic };
  });
  const handle = await start("roomAgentSupervisorWorkflow", supervisorInput({ monitoringMissionIds: [automatic.missionId] }));
  await env.sleep("1 second");
  await eventually(() => Boolean(release));
  const followup = item("operator-followup");
  await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, followup);
  release();
  await eventually(() => activities.runCodexAppServerTurn.mock.calls.length === 1);
  expect(activities.discardRoomAgentSupervisorTurn.mock.calls[0][0].item).toEqual(automatic);
  expect(activities.runCodexAppServerTurn.mock.calls[0][0].agentRunId).toBe(followup.turn.agentRunId);
  await terminate(handle);
});
it("drains excess pending state before continuing", async () => {
  let release!: () => void;
  activities.checkRoomAgentSupervisorTurn.mockImplementationOnce(async () => {
    await new Promise<void>(resolve => { release = resolve; });
    return "SKIP";
  }).mockResolvedValue("SKIP");
  const handle = await start("roomAgentSupervisorWorkflow", supervisorInput({ pending: [item("flood-0")], processedCount: 49 }), "sandbox-main", false);
  await eventually(() => Boolean(release));
  for (let index = 1; index <= 105; index++) await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, item(`flood-${index}`));
  release();
  const carried = await nextRunInput(handle);
  expect(carried.pending).toHaveLength(100);
  expect(() => roomAgentSupervisorInputSchema.parse(carried)).not.toThrow();
  // The first six items were processed, leaving all other 100 in the next run.
  const history = await handle.fetchHistory();
  const checks = history.events?.filter((e) => e.activityTaskScheduledEventAttributes?.activityType?.name === "checkRoomAgentSupervisorTurn");
  expect(checks).toHaveLength(6);
  await capture("room-supervisor-bounded-carry", handle);
});
it.each([["Error", "ROOM_AGENT_SUPERVISOR_FAILED"], ["CODEX_APP_SERVER_TURN_TIMEOUT", "CODEX_APP_SERVER_TURN_TIMEOUT"]])(
  "records a failed child with its safe cause code %s", async (type, code) => {
    activities.runCodexAppServerTurn.mockRejectedValue(ApplicationFailure.nonRetryable("Synthetic child failure", type));
    const handle = await start("roomAgentSupervisorWorkflow", supervisorInput({ pending: [item("failure")] }));
    await eventually(() => activities.finishRoomAgentSupervisorTurn.mock.calls.length === 1);
    expect(activities.finishRoomAgentSupervisorTurn.mock.calls[0][0]).toMatchObject({ status: "FAILED", errorCode: code });
    expect(activities.discardRoomAgentSupervisorTurn.mock.calls[0][0]).toMatchObject({ errorCode: code });
    await terminate(handle);
  });

function persistedDriver() {
  const c = continuationFixture();
  activities.prepareRoomAgentBackgroundContinuation.mockImplementation(input => prepareBackgroundContinuation(input, c.store));
  activities.checkRoomAgentSupervisorTurn.mockImplementation(input => supervisorTurnState(input, c.store));
  activities.discardRoomAgentSupervisorTurn.mockImplementation(({ item, reason, errorCode }) => discardSupervisorTurn(item, reason, c.store, errorCode));
  activities.finishRoomAgentSupervisorTurn.mockImplementation(input => finishSupervisorTurn(input, c.store));
  activities.runCodexAppServerTurn.mockImplementation(async input => {
    c.store.updateSpaceAgentRun(input.agentRunId, { status: "COMPLETED" });
    return completed;
  });
  const input = roomAgentSupervisorInputSchema.parse({ roomId: c.room.id, monitoringMissionIds: [c.initial.mission.id] });
  return { ...c, input };
}
it("executes allocation, send and verification once in one persisted mission", async () => {
  const c = persistedDriver();
  activities.runCodexAppServerTurn.mockImplementationOnce(async input => {
    expect(input.prompt).toContain("Open CLI and Chat, then send a story");
    c.store.createRoomAgentAction({ ...c.action, actionId: "action:send", idempotencyKey: "send-once", actionType: "SEND",
      evidence: { cli: "Story complete", chat: "Story complete" }, statusReason: "Both tasks completed." });
    c.store.updateSpaceAgentRun(input.agentRunId, { status: "COMPLETED" });
    return { ...completed, roomAgentOutcome: { ...completed.roomAgentOutcome, pendingActionIds: ["action:send"] } };
  });
  const handle = await start("roomAgentSupervisorWorkflow", c.input);
  await env.sleep("3 seconds");
  await eventually(() => activities.finishRoomAgentSupervisorTurn.mock.calls.length === 1);
  expect(activities.runCodexAppServerTurn).toHaveBeenCalledTimes(2);
  expect(c.store.listRoomAgentActions(c.initial.mission.id).filter(action => action.actionType === "SEND")).toHaveLength(1);
  expect(c.store.getRoomAgentMission(c.room.id, c.initial.mission.id)?.status).toBe("COMPLETED");
  await terminate(handle);
});
it("carries a prepared persisted continuation and starts it exactly once", async () => {
  const c = persistedDriver();
  const handle = await start("roomAgentSupervisorWorkflow", { ...c.input, processedCount: 50 }, "sandbox-main", false);
  const outcome = handle.result().catch(() => undefined);
  const carried = await nextRunInput(handle);
  expect(carried.pending).toHaveLength(1);
  expect(() => roomAgentSupervisorInputSchema.parse(carried)).not.toThrow();
  await outcome;
  await eventually(() => activities.finishRoomAgentSupervisorTurn.mock.calls.length === 1);
  expect(activities.runCodexAppServerTurn).toHaveBeenCalledOnce();
  expect(c.store.getRoomAgentMission(c.room.id, c.initial.mission.id)?.status).toBe("COMPLETED");
});
it.each(["stop", "follow-up"])("retires persisted prepared work when %s arrives before the reply", async kind => {
  const c = persistedDriver();
  let release!: () => void;
  let prepared: Extract<Awaited<ReturnType<typeof prepareBackgroundContinuation>>, { state: "READY" }>;
  activities.prepareRoomAgentBackgroundContinuation.mockImplementationOnce(async input => {
    const result = await prepareBackgroundContinuation(input, c.store);
    if (result.state !== "READY") throw Error("Expected a prepared synthetic continuation");
    prepared = result;
    await new Promise<void>(resolve => { release = resolve; });
    return prepared;
  });
  const handle = await start("roomAgentSupervisorWorkflow", c.input);
  await env.sleep("1 second");
  await eventually(() => Boolean(release));
  if (kind === "stop") {
    c.store.updateRoomAgentMission(c.initial.mission.id, { status: "INTERRUPTED" });
    await handle.signal(ROOM_AGENT_STOP_SIGNAL, { missionId: null, reason: "Stop" });
  } else {
    const followup = c.enqueue("follow-up", "Only inspect the room.");
    await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, followup.queueItem);
  }
  release();
  await eventually(() => activities.discardRoomAgentSupervisorTurn.mock.calls.length > 0);
  expect(c.store.getRoomAgentTurn(c.initial.mission.id, prepared.item.turn.agentRunId)?.run.status).toBe("INTERRUPTED");
  if (kind === "stop") expect(activities.runCodexAppServerTurn).not.toHaveBeenCalled();
  else {
    await eventually(() => activities.runCodexAppServerTurn.mock.calls.length > 0);
    expect(activities.runCodexAppServerTurn.mock.calls[0][0].roomAgentOperatorRequest).toBe("Only inspect the room.");
  }
  await terminate(handle);
});

it.each([
  ["plain failure", ApplicationFailure.nonRetryable("Synthetic failure", "Error"), "ROOM_AGENT_SUPERVISOR_FAILED"],
  ["timeout cause", ApplicationFailure.create({ message: "Synthetic failure", nonRetryable: true,
    cause: new TimeoutFailure("Synthetic timeout", undefined, TimeoutType.START_TO_CLOSE) }), "ROOM_AGENT_SUPERVISOR_TIMEOUT"],
  ["safe application code", ApplicationFailure.nonRetryable("Synthetic failure", "CODEX_APP_SERVER_TURN_TIMEOUT"), "CODEX_APP_SERVER_TURN_TIMEOUT"]
])("persists a child %s as FAILED with the cause code", async (_label, failure, code) => {
  const c = persistedDriver();
  activities.runCodexAppServerTurn.mockRejectedValue(failure);
  const handle = await start("roomAgentSupervisorWorkflow", c.input);
  await env.sleep("1 second");
  await eventually(() => activities.finishRoomAgentSupervisorTurn.mock.calls.length === 1);
  const prepared = await activities.prepareRoomAgentBackgroundContinuation.mock.results[0].value;
  expect(c.store.getRoomAgentTurn(c.initial.mission.id, prepared.item.turn.agentRunId)?.run).toMatchObject({ status: "FAILED", errorCode: code });
  expect(c.store.getRoomAgentMission(c.room.id, c.initial.mission.id)?.status).toBe("FAILED");
  expect(activities.finishRoomAgentSupervisorTurn.mock.calls[0][0]).toMatchObject({ status: "FAILED", errorCode: code });
  await terminate(handle);
});

it("keeps pending, interrupted and monitoring state together across continuation", async () => {
  const first = { item: item("stop-first"), reason: "Synthetic stop one" };
  const second = { item: item("stop-carried"), reason: "Synthetic stop two" };
  const handle = await start("roomAgentSupervisorWorkflow", supervisorInput({ pending: [item("pending-carried")],
    interrupted: [first, second], monitoringMissionIds: ["mission:monitor-carried"], processedCount: 49 }), "sandbox-main", false);
  const carried = await nextRunInput(handle);
  expect(carried.interrupted).toEqual([second]);
  expect(carried.pending).toEqual([item("pending-carried")]);
  expect(carried.monitoringMissionIds).toEqual(["mission:monitor-carried"]);
  expect(carried.processedCount).toBe(0);
});
it("observes a running goal turn on durable 15-second timers", async () => {
  const local = await TestWorkflowEnvironment.createTimeSkipping();
  const step = vi.fn().mockResolvedValueOnce({ state: "RUNNING", runId: "run:synthetic", goalStatus: "RUNNING" })
    .mockResolvedValue({ state: "DONE", runId: null, goalStatus: "COMPLETED" });
  const observe = vi.fn().mockResolvedValueOnce({ run: "RUNNING", goalStatus: "RUNNING" })
    .mockResolvedValue({ run: "COMPLETED", goalStatus: "RUNNING" });
  const worker = await Worker.create({ connection: local.nativeConnection, taskQueue: "sandbox-observe", workflowBundle: bundle,
    activities: { dotGoalStep: step, dotGoalObserve: observe } });
  try {
    await worker.runUntil(async () => {
      expect(await local.client.workflow.execute("dotGoalWorkflow", { workflowId: "synthetic-goal-observe", taskQueue: "sandbox-observe",
        args: [{ goalId: "goal:observe" }] })).toEqual({ goalStatus: "COMPLETED" });
      expect(observe).toHaveBeenCalledTimes(2);
      expect(observe.mock.calls[0]).toEqual(["goal:observe", "run:synthetic"]);
    });
  } finally { await local.teardown(); }
});