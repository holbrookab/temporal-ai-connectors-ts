import { describe, expect, it } from "vitest";
import type {
  TaskLifecycleData,
  TaskLifecycleEvent,
  TaskResult,
  TaskStatus,
} from "../src/core";

describe("task lifecycle types", () => {
  it("accepts adaptive planning events and statuses", () => {
    const events = [
      "task-plan-created",
      "task-plan-updated",
      "task-started",
      "task-completed",
      "task-failed",
      "task-skipped",
      "task-blocked",
    ] satisfies TaskLifecycleEvent[];
    const statuses = [
      "planned",
      "active",
      "completed",
      "failed",
      "skipped",
      "blocked",
    ] satisfies TaskStatus[];
    const result = {
      taskId: "task-1",
      status: "alternate_path",
      summary: "Use the converted PDF for extraction.",
      blocker: "Waiting for approval",
    } satisfies TaskResult;
    const event = {
      event: "task-plan-updated",
      plan: {
        execution: "sequential",
        tasks: [{ id: "task-1", title: "Convert resume" }],
      },
      result,
    } satisfies TaskLifecycleData;

    expect(events).toContain("task-plan-updated");
    expect(statuses).toContain("blocked");
    expect(event.result?.status).toBe("alternate_path");
  });
});
