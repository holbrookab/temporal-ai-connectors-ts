import { describe, expect, it } from "vitest";
import {
  createSubscribeFirstReplayStream,
  type DurableReplayResponse,
  type DurableStreamEvent,
} from "../src/core";

describe("createSubscribeFirstReplayStream", () => {
  it("subscribes before replay and dedupes replay/live races by event id", async () => {
    let onLive: ((event: DurableStreamEvent) => void) | undefined;
    const stream = createSubscribeFirstReplayStream<DurableStreamEvent>({
      drainDelayMs: 0,
      getEventId: (event) => event.eventId,
      getChunk: (event) => event.chunk,
      subscribe: async (handler) => {
        onLive = handler;
        handler({ eventId: "02", chunk: "live-duplicate" });
        handler({ eventId: "03", chunk: "live-only" });
        return { close() {} };
      },
      fetchReplay: async (): Promise<DurableReplayResponse> => ({
        streamId: "s1",
        events: [
          { eventId: "01", chunk: "replay-first" },
          { eventId: "02", chunk: "replay-duplicate" },
        ],
      }),
    });

    const reader = stream.getReader();
    const first = await reader.read();
    const second = await reader.read();
    const third = await reader.read();
    await reader.cancel();

    expect(onLive).toBeDefined();
    expect([first.value, second.value, third.value]).toEqual([
      { eventId: "01", chunk: "replay-first" },
      { eventId: "02", chunk: "live-duplicate" },
      { eventId: "03", chunk: "live-only" },
    ]);
  });

  it("hydrates active attempt snapshots before replayed chunks", async () => {
    const stream = createSubscribeFirstReplayStream<DurableStreamEvent>({
      drainDelayMs: 0,
      getEventId: (event) => event.eventId,
      getChunk: (event) => event.chunk,
      subscribe: async () => ({ close() {} }),
      fetchReplay: async () => ({
        streamId: "s1",
        events: [{ eventId: "01", chunk: { type: "data-llm-stream", data: { event: "text-delta" } } }],
        attempts: [
          {
            streamId: "s1",
            lane: "text",
            attemptId: "attempt:text",
            status: "active",
            snapshotText: "hello",
            snapshotSequence: 4,
            updatedAt: 10,
          },
        ],
      }),
    });

    const reader = stream.getReader();
    const first = await reader.read();
    const second = await reader.read();
    await reader.cancel();

    expect(first.value?.eventId).toBe("!attempt#s1#text#_#attempt:text");
    expect(first.value?.chunk).toMatchObject({
      type: "data-llm-stream",
      data: { event: "snapshot", snapshotText: "hello", sequence: 4 },
    });
    expect(second.value?.eventId).toBe("01");
  });

  it("does one final replay before emitting a live terminal event", async () => {
    let onLive: ((event: DurableStreamEvent) => void) | undefined;
    let replayCalls = 0;
    const stream = createSubscribeFirstReplayStream<DurableStreamEvent>({
      drainDelayMs: 0,
      getEventId: (event) => event.eventId,
      getChunk: (event) => event.chunk,
      isTerminalEvent: (event) =>
        typeof event.chunk === "object" &&
        event.chunk !== null &&
        (event.chunk as Record<string, unknown>).__control === "done",
      subscribe: async (handler) => {
        onLive = handler;
        return { close() {} };
      },
      fetchReplay: async (): Promise<DurableReplayResponse> => {
        replayCalls += 1;
        return {
          streamId: "s1",
          events:
            replayCalls === 1
              ? [{ eventId: "01", chunk: "initial" }]
              : [{ eventId: "02", chunk: "missed-terminal-tool" }],
        };
      },
    });

    const reader = stream.getReader();
    const first = await reader.read();
    onLive?.({ eventId: "03", chunk: { __control: "done" } });
    const second = await reader.read();
    const third = await reader.read();
    await reader.cancel();

    expect(first.value).toEqual({ eventId: "01", chunk: "initial" });
    expect(second.value).toEqual({ eventId: "02", chunk: "missed-terminal-tool" });
    expect(third.value).toEqual({ eventId: "03", chunk: { __control: "done" } });
    expect(replayCalls).toBe(2);
  });
});
