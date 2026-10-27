import { sleep } from "@temporalio/workflow";
import { dotGoalWorkflow as productionDotGoal } from "../../src/workflows.js";

// A deliberate command mismatch. Never registered by a production worker.
export async function dotGoalWorkflow(input: { goalId: string }) {
  await sleep("1 second");
  return productionDotGoal(input);
}
