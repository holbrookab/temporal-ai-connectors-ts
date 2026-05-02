import { describe, expect, it } from "vitest";
import {
  applyDurableStreamData,
  selectActiveStreamText,
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
});
