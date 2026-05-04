import { describe, expect, it } from "vitest";
import {
  TemporalDurableChatTransport,
  createUIMessageChunkStreamFromDurableEvents,
  normalizeUIMessageChunks,
} from "../src/ai-sdk";

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

  it("keeps tool task metadata on output chunks that omit metadata", () => {
    const rememberedTools = new Map();
    const [inputChunk] = normalizeUIMessageChunks(
      {
        type: "tool-input-available",
        toolCallId: "call-1",
        toolName: "extractDocument",
        input: { s3Uri: "s3://bucket/key.pdf" },
        metadata: { taskId: "task1", taskTitle: "Resume Extraction" },
      },
      rememberedTools,
    );
    rememberedTools.set("call-1", {
      toolName: "extractDocument",
      providerMetadata: (inputChunk as { providerMetadata?: Record<string, unknown> }).providerMetadata,
    });

    const [outputChunk] = normalizeUIMessageChunks(
      {
        type: "tool-output-available",
        toolCallId: "call-1",
        output: { ok: true },
      },
      rememberedTools,
    );

    expect(outputChunk).toMatchObject({
      type: "tool-output-available",
      toolCallId: "call-1",
      toolName: "extractDocument",
      providerMetadata: {
        temporal: { taskId: "task1", taskTitle: "Resume Extraction" },
      },
    });
  });

  it("includes per-send request body fields", async () => {
    let requestBody: unknown;
    const transport = new TemporalDurableChatTransport({
      api: "/api/chat",
      fetch: async (_input, init) => {
        requestBody = JSON.parse(String(init?.body ?? "{}"));
        return Response.json({ streamId: "stream-1" });
      },
      streamFactory: () =>
        new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
    });

    await transport.sendMessages({
      trigger: "submit-message",
      chatId: "chat-1",
      messageId: "message-1",
      messages: [{ id: "message-1", role: "user", parts: [{ type: "text", text: "Hello" }] }],
      abortSignal: undefined,
      body: { conversationId: "conversation-1", userMessageId: "user-message-1" },
    });

    expect(requestBody).toMatchObject({
      conversationId: "conversation-1",
      userMessageId: "user-message-1",
    });
  });
});
