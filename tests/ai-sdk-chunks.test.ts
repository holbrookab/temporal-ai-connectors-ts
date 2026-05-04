import { describe, expect, it } from "vitest";
import { createUIMessageChunkStreamFromDurableEvents, normalizeUIMessageChunks } from "../src/ai-sdk";

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

  it("maps Temporal tool lifecycle metadata to AI SDK providerMetadata", () => {
    const [chunk] = normalizeUIMessageChunks({
      type: "tool-input-available",
      toolCallId: "call-1",
      toolName: "extractDocument",
      input: { s3Uri: "s3://bucket/key.pdf" },
      metadata: { taskId: "task1", taskTitle: "Extract Resume" },
    });

    expect(chunk).toEqual({
      type: "tool-input-available",
      toolCallId: "call-1",
      toolName: "extractDocument",
      input: { s3Uri: "s3://bucket/key.pdf" },
      providerMetadata: {
        temporal: { taskId: "task1", taskTitle: "Extract Resume" },
      },
    });
    expect("metadata" in (chunk as Record<string, unknown>)).toBe(false);
  });
});
