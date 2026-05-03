import type { ChatTransport, UIMessage, UIMessageChunk, UITools } from "ai";
import type { DurableStreamData, DurableStreamEvent, TaskLifecycleData } from "../core/types";

export type DurableChatAck = {
  accepted?: boolean;
  streamId: string;
  userId?: string;
  scopeId?: string;
  channel?: string;
  redisStream?: string;
  [key: string]: unknown;
};

export type DurableChatControlFrame = {
  __control: "done" | "error";
  error?: string;
  message?: string;
};

export type DurableLlmStreamDataPart = {
  "llm-stream": DurableStreamData;
};

export type DurableTaskEventDataPart = {
  "task-event": TaskLifecycleData;
};

export type DurableUIMessage<
  METADATA = unknown,
  DATA_PARTS extends Record<string, unknown> = Record<string, unknown>,
  TOOLS extends UITools = UITools,
> = UIMessage<METADATA, DATA_PARTS & DurableLlmStreamDataPart, TOOLS>;

export type DurableTaskUIMessage<
  METADATA = unknown,
  DATA_PARTS extends Record<string, unknown> = Record<string, unknown>,
  TOOLS extends UITools = UITools,
> = UIMessage<METADATA, DATA_PARTS & DurableLlmStreamDataPart & DurableTaskEventDataPart, TOOLS>;

export type DurableStreamFactory<TAck extends DurableChatAck = DurableChatAck> = (
  ack: TAck,
  options: {
    abortSignal?: AbortSignal;
  },
) => ReadableStream<DurableStreamEvent>;

export type TemporalDurableChatTransportOptions<TAck extends DurableChatAck = DurableChatAck> = {
  api?: string;
  fetch?: typeof fetch;
  headers?: HeadersInit | (() => HeadersInit | Promise<HeadersInit>);
  credentials?: RequestCredentials;
  body?: Record<string, unknown> | (() => Record<string, unknown> | Promise<Record<string, unknown>>);
  prepareSendBody?: (
    options: Parameters<ChatTransport<UIMessage>["sendMessages"]>[0],
  ) => unknown | Promise<unknown>;
  streamFactory: DurableStreamFactory<TAck>;
  reconnectStreamFactory?: (
    options: Parameters<ChatTransport<UIMessage>["reconnectToStream"]>[0],
  ) => Promise<ReadableStream<DurableStreamEvent> | null> | ReadableStream<DurableStreamEvent> | null;
  onSendAck?: (ack: TAck) => void;
  notifyToolWrite?: (chunk: UIMessageChunk & { toolName?: string; toolCallId?: string }) => void;
  isToolWrite?: (toolName: string) => boolean;
};
