import { describe, expect, it } from "vitest";
import {
  TemporalDurableChatTransport,
  collectToolApprovals,
  createUIMessageChunkStreamFromDurableEvents,
  getActiveToolApproval,
  normalizeUIMessageChunks,
} from "../src/ai-sdk";

async function readStream<T>(stream: ReadableStream<T>): Promise<T[]> {
  const reader = stream.getReader();
  const chunks: T[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) return chunks;
    chunks.push(value);
  }
}

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

  it("normalizes tool approval chunks and keeps remembered tool metadata", () => {
    const rememberedTools = new Map();
    const [inputChunk] = normalizeUIMessageChunks(
      {
        type: "tool-input-available",
        toolCallId: "call-1",
        toolName: "create_worker",
        input: { name: "Ada" },
        metadata: { taskId: "task-write" },
      },
      rememberedTools,
    );
    rememberedTools.set("call-1", {
      toolName: "create_worker",
      input: { name: "Ada" },
      providerMetadata: (inputChunk as { providerMetadata?: Record<string, unknown> }).providerMetadata,
    });

    const [requestChunk] = normalizeUIMessageChunks(
      {
        type: "tool-approval-request",
        toolCallId: "call-1",
        toolName: "create_worker",
        approvalId: "approval-1",
      },
      rememberedTools,
    );
    const [responseChunk] = normalizeUIMessageChunks(
      {
        type: "tool-approval-response",
        approvalId: "approval-1",
        approved: true,
        reason: "looks good",
        metadata: { taskId: "task-write" },
      },
      rememberedTools,
    );

    expect(requestChunk).toMatchObject({
      type: "tool-approval-request",
      toolCallId: "call-1",
      toolName: "create_worker",
      approvalId: "approval-1",
      input: { name: "Ada" },
      providerMetadata: { temporal: { taskId: "task-write" } },
    });
    expect(responseChunk).toMatchObject({
      type: "tool-approval-response",
      approvalId: "approval-1",
      approved: true,
      reason: "looks good",
      providerMetadata: { temporal: { taskId: "task-write" } },
    });
  });

  it("emits human checkpoint data parts for tool approval lifecycle chunks", () => {
    const rememberedTools = new Map();
    const [inputChunk] = normalizeUIMessageChunks(
      {
        type: "tool-input-available",
        toolCallId: "call-1",
        toolName: "applicant_tracking_create_worker",
        input: { name: "Ada", email: "ada@example.com" },
        metadata: {
          approvalWorkflowId: "workflow-1",
          assistantMessageId: "assistant-1",
          assistantMessageCreatedAt: 100,
          taskId: "task-write",
          taskTitle: "Add worker",
        },
      },
      rememberedTools,
    );
    rememberedTools.set("call-1", {
      toolName: "applicant_tracking_create_worker",
      input: { name: "Ada", email: "ada@example.com" },
      providerMetadata: (inputChunk as { providerMetadata?: Record<string, unknown> }).providerMetadata,
    });

    const requestChunks = normalizeUIMessageChunks(
      {
        type: "tool-approval-request",
        toolCallId: "call-1",
        approvalId: "call-1:approval",
      },
      rememberedTools,
    );
    const responseChunks = normalizeUIMessageChunks(
      {
        type: "tool-approval-response",
        toolCallId: "call-1",
        approvalId: "call-1:approval",
        approved: true,
        reason: "confirmed",
      },
      rememberedTools,
    );

    expect(requestChunks[1]).toMatchObject({
      type: "data-human-checkpoint",
      id: "human-checkpoint-call-1:approval",
      data: {
        event: "checkpoint-created",
        checkpointId: "call-1:approval",
        approvalId: "call-1:approval",
        toolCallId: "call-1",
        toolName: "applicant_tracking_create_worker",
        status: "pending",
        questions: [{ id: "call-1:approval", required: true }],
        metadata: {
          approvalWorkflowId: "workflow-1",
          assistantMessageId: "assistant-1",
          assistantMessageCreatedAt: 100,
          taskId: "task-write",
          taskTitle: "Add worker",
          toolCallId: "call-1",
          toolName: "applicant_tracking_create_worker",
          input: { name: "Ada", email: "ada@example.com" },
        },
      },
    });
    expect(responseChunks[1]).toMatchObject({
      type: "data-human-checkpoint",
      data: {
        event: "checkpoint-submitted",
        checkpointId: "call-1:approval",
        status: "approved",
        approved: true,
        reason: "confirmed",
      },
    });
  });

  it("collects pending and submitted tool approvals from UI messages", () => {
    const messages = [
      {
        id: "assistant-1",
        role: "assistant" as const,
        parts: [
          {
            type: "tool-create_worker",
            toolCallId: "call-1",
            state: "approval-requested",
            input: { name: "Ada" },
            approval: { id: "approval-1" },
            callProviderMetadata: { temporal: { taskId: "task-write" } },
          },
          {
            type: "tool-update_worker",
            toolCallId: "call-2",
            state: "approval-responded",
            input: { id: "worker-1" },
            approval: { id: "approval-2", approved: false, reason: "wrong worker" },
          },
        ],
      },
    ];

    expect(getActiveToolApproval(messages)).toMatchObject({
      approvalId: "approval-1",
      toolCallId: "call-1",
      toolName: "create_worker",
      status: "pending",
      providerMetadata: { temporal: { taskId: "task-write" } },
    });
    expect(collectToolApprovals(messages)).toMatchObject([
      { approvalId: "approval-1", status: "pending" },
      { approvalId: "approval-2", status: "denied", reason: "wrong worker" },
    ]);
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

  it("uses durable assistant message id as the streamed UI message id when available", async () => {
    const transport = new TemporalDurableChatTransport({
      api: "/api/chat",
      fetch: async () =>
        Response.json({
          streamId: "workflow-stream-id",
          assistantMessageId: "assistant-message-id",
        }),
      streamFactory: () =>
        new ReadableStream({
          start(controller) {
            controller.close();
          },
        }),
    });

    const stream = await transport.sendMessages({
      trigger: "submit-message",
      chatId: "chat-1",
      messageId: "message-1",
      messages: [{ id: "message-1", role: "user", parts: [{ type: "text", text: "Hello" }] }],
      abortSignal: undefined,
    });

    const chunks = await readStream(stream);
    expect(chunks[0]).toMatchObject({
      type: "start",
      messageId: "assistant-message-id",
    });
  });
});
