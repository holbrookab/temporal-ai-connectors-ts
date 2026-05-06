import { describe, expect, it } from "vitest";
import {
  applyAttemptManifests,
  applyDurableStreamData,
  selectActiveStreamObject,
  selectActiveStreamText,
  selectVisibleStreamObject,
  selectVisibleStreamText,
  selectTaskStepStreamObject,
  selectTaskStepStreamText,
  selectToolInputText,
  type DurableStreamState,
} from "../src/core";

describe("durable stream state", () => {
  it("applies snapshots, ignores duplicate deltas, and clears discarded attempts", () => {
    let state: DurableStreamState = {};
    state = applyDurableStreamData(state, {
      event: "snapshot",
      streamId: "s1",
      phase: "provider-live",
      lane: "text",
      attemptId: "a1",
      sequence: 2,
      snapshotText: "he",
    });
    state = applyDurableStreamData(state, {
      event: "text-delta",
      streamId: "s1",
      phase: "provider-live",
      lane: "text",
      attemptId: "a1",
      sequence: 3,
      delta: "llo",
    });
    state = applyDurableStreamData(state, {
      event: "text-delta",
      streamId: "s1",
      phase: "provider-live",
      lane: "text",
      attemptId: "a1",
      sequence: 3,
      delta: " duplicate",
    });

    expect(selectActiveStreamText(state, "s1", "text")).toBe("hello");

    state = applyDurableStreamData(state, {
      event: "attempt-discard",
      streamId: "s1",
      phase: "provider-live",
      lane: "text",
      attemptId: "a1",
      sequence: 4,
      status: "discarded",
    });

    expect(selectActiveStreamText(state, "s1", "text")).toBe("");
  });

  it("selects task step and tool input text by durable scope", () => {
    let state: DurableStreamState = {};
    state = applyDurableStreamData(state, {
      event: "text-delta",
      streamId: "s1",
      phase: "provider-live",
      lane: "text",
      attemptId: "agent:step-0:text",
      sequence: 1,
      delta: "task text",
      displayMode: "task",
      taskId: "task-1",
      stepId: "step-0",
      stepNumber: 0,
      stepType: "initial",
    });
    state = applyDurableStreamData(state, {
      event: "text-delta",
      streamId: "s1",
      phase: "provider-live",
      lane: "text",
      attemptId: "final:text",
      sequence: 1,
      delta: "assistant text",
      displayMode: "assistant",
    });
    state = applyDurableStreamData(state, {
      event: "tool-input-delta",
      streamId: "s1",
      phase: "provider-live",
      lane: "tool-input",
      attemptId: "agent:step-0:tool-input:call-1",
      toolCallId: "call-1",
      sequence: 1,
      delta: "{\"q\"",
      displayMode: "task",
      taskId: "task-1",
      stepId: "step-0",
    });
    state = applyDurableStreamData(state, {
      event: "tool-input-delta",
      streamId: "s1",
      phase: "provider-live",
      lane: "tool-input",
      attemptId: "agent:step-0:tool-input:call-1",
      toolCallId: "call-1",
      sequence: 2,
      delta: ":\"Ada\"}",
      displayMode: "task",
      taskId: "task-1",
      stepId: "step-0",
    });

    expect(selectTaskStepStreamText(state, "s1", { taskId: "task-1", stepId: "step-0" })).toBe(
      "task text",
    );
    expect(selectActiveStreamText(state, "s1", "text", { displayMode: "assistant" })).toBe(
      "assistant text",
    );
    expect(selectToolInputText(state, "s1", "call-1", { taskId: "task-1" })).toBe(
      "{\"q\":\"Ada\"}",
    );
  });

  it("hydrates committed task step text from durable attempt manifests", () => {
    const state = applyAttemptManifests(
      {},
      [
        {
          streamId: "s1",
          lane: "text",
          attemptId: "agent:step-0:text",
          status: "committed",
          snapshotText: "persisted task text",
          snapshotSequence: 4,
          updatedAt: 10,
          displayMode: "task",
          taskId: "task-1",
          stepId: "step-0",
          stepNumber: 0,
        },
        {
          streamId: "s1",
          lane: "text",
          attemptId: "discarded:text",
          status: "discarded",
          snapshotText: "discarded",
          snapshotSequence: 1,
          updatedAt: 11,
          displayMode: "task",
          taskId: "task-1",
          stepId: "step-1",
        },
      ],
    );

    expect(selectTaskStepStreamText(state, "s1", { taskId: "task-1", stepId: "step-0" })).toBe(
      "persisted task text",
    );
    expect(selectVisibleStreamText(state, "s1", "text", { stepId: "step-1" })).toBe("");
  });

  it("hydrates committed task step text from replayed snapshot events", () => {
    const state = applyDurableStreamData(
      {},
      {
        event: "snapshot",
        streamId: "s1",
        phase: "provider-live",
        lane: "text",
        attemptId: "agent:step-0:text",
        status: "committed",
        snapshotText: "replayed task text",
        sequence: 4,
        displayMode: "task",
        taskId: "task-1",
        stepId: "step-0",
        stepNumber: 0,
      },
    );

    expect(selectTaskStepStreamText(state, "s1", { taskId: "task-1", stepId: "step-0" })).toBe(
      "replayed task text",
    );
  });

  it("applies live, snapshot, and completion object state", () => {
    let state: DurableStreamState = {};
    state = applyDurableStreamData(state, {
      event: "text-delta",
      streamId: "s1",
      phase: "provider-live",
      lane: "object",
      attemptId: "agent:step-0:object",
      sequence: 1,
      snapshotObject: { status: "needs_user" },
      displayMode: "task",
      taskId: "task-1",
      stepId: "step-0",
    });

    expect(selectActiveStreamObject(state, "s1", "object", { taskId: "task-1" })).toEqual({
      status: "needs_user",
    });
    expect(selectTaskStepStreamObject(state, "s1", { taskId: "task-1", stepId: "step-0" })).toEqual(
      { status: "needs_user" },
    );

    state = applyDurableStreamData(state, {
      event: "attempt-commit",
      streamId: "s1",
      phase: "provider-live",
      lane: "object",
      attemptId: "agent:step-0:object",
      sequence: 2,
      status: "committed",
      snapshotObject: { status: "needs_user", reason: "approval required" },
      displayMode: "task",
      taskId: "task-1",
      stepId: "step-0",
    });

    expect(selectVisibleStreamObject(state, "s1", "object", { taskId: "task-1" })).toEqual({
      status: "needs_user",
      reason: "approval required",
    });
  });

  it("hydrates committed object state from durable attempt manifests", () => {
    const state = applyAttemptManifests(
      {},
      [
        {
          streamId: "s1",
          lane: "object",
          attemptId: "agent:step-0:object",
          status: "committed",
          snapshotObject: { status: "complete" },
          snapshotSequence: 3,
          updatedAt: 10,
          displayMode: "task",
          taskId: "task-1",
          stepId: "step-0",
        },
      ],
    );

    expect(selectTaskStepStreamObject(state, "s1", { taskId: "task-1", stepId: "step-0" })).toEqual(
      { status: "complete" },
    );
  });
});
