import { describe, expect, it } from "vitest";
import { createUIMessageChunkStreamFromDurableEvents } from "../src/ai-sdk";

describe("createUIMessageChunkStreamFromDurableEvents", () => {
  it("emits an AI SDK start chunk before durable events when startMessageId is provided", async () => {
    const durableEvents = new ReadableStream({
      start(controller) {
        controller.enqueue({
          eventId: "01",
          chunk: { type: "text-start", id: "text" },
        });
        controller.close();
      },
    });

    const chunks = createUIMessageChunkStreamFromDurableEvents(durableEvents, {
      startMessageId: "assistant-message-id",
    });
    const reader = chunks.getReader();

    const first = await reader.read();
    const second = await reader.read();

    expect(first.value).toEqual({
      type: "start",
      messageId: "assistant-message-id",
    });
    expect(second.value).toEqual({ type: "text-start", id: "text" });
  });
});
