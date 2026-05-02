import type { UIMessageChunk } from "ai";
import type { DurableChatControlFrame } from "./types";
import type { DurableStreamEvent } from "../core/types";

type ToolChunkWithName = UIMessageChunk & {
  toolCallId?: string;
  toolName?: string;
  dynamic?: boolean;
};

export type UIChunkNormalizerOptions = {
  notifyToolWrite?: (chunk: ToolChunkWithName) => void;
  isToolWrite?: (toolName: string) => boolean;
  startMessageId?: string;
};

export function createUIMessageChunkStreamFromDurableEvents(
  durableEvents: ReadableStream<DurableStreamEvent>,
  options: UIChunkNormalizerOptions = {},
): ReadableStream<UIMessageChunk> {
  const toolNamesByCallId = new Map<string, { toolName: string; dynamic?: boolean }>();

  return new ReadableStream<UIMessageChunk>({
    async start(controller) {
      if (options.startMessageId) {
        controller.enqueue({
          type: "start",
          messageId: options.startMessageId,
        } as UIMessageChunk);
      }
      const reader = durableEvents.getReader();
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          const chunk = value.chunk;
          if (isControlFrame(chunk)) {
            if (chunk.__control === "done") {
              controller.close();
              return;
            }
            enqueueStreamError(controller, chunk.error ?? chunk.message ?? "stream error", value.eventId);
            controller.close();
            return;
          }

          for (const uiChunk of normalizeUIMessageChunks(chunk, toolNamesByCallId)) {
            const toolChunk = uiChunk as ToolChunkWithName;
            if (uiChunk.type === "tool-input-available" || uiChunk.type === "tool-input-error") {
              toolNamesByCallId.set(uiChunk.toolCallId, {
                toolName: uiChunk.toolName,
                dynamic: uiChunk.dynamic,
              });
            }
            if (uiChunk.type === "tool-output-available" || uiChunk.type === "tool-output-error") {
              const toolName =
                toolChunk.toolName ??
                (toolChunk.toolCallId ? toolNamesByCallId.get(toolChunk.toolCallId)?.toolName : undefined);
              if (toolName && options.isToolWrite?.(toolName)) {
                options.notifyToolWrite?.({ ...toolChunk, toolName });
              }
            }
            controller.enqueue(uiChunk);
          }
        }
        controller.close();
      } catch (error) {
        controller.error(error);
      } finally {
        reader.releaseLock();
      }
    },
    cancel(reason) {
      return durableEvents.cancel(reason);
    },
  });
}

export function normalizeUIMessageChunks(
  chunk: unknown,
  toolNamesByCallId = new Map<string, { toolName: string; dynamic?: boolean }>(),
): UIMessageChunk[] {
  if (!chunk || typeof chunk !== "object") return [];
  const value = chunk as Record<string, unknown>;

  if (value.type === "tool-output-available" || value.type === "tool-output-error") {
    const toolCallId = typeof value.toolCallId === "string" ? value.toolCallId : undefined;
    const remembered = toolCallId ? toolNamesByCallId.get(toolCallId) : undefined;
    if (remembered) {
      return [
        {
          ...value,
          toolName: typeof value.toolName === "string" ? value.toolName : remembered.toolName,
          dynamic: value.dynamic === undefined ? remembered.dynamic : value.dynamic,
        } as UIMessageChunk,
      ];
    }
  }

  return [chunk as UIMessageChunk];
}

export function isControlFrame(value: unknown): value is DurableChatControlFrame {
  return (
    !!value &&
    typeof value === "object" &&
    typeof (value as { __control?: unknown }).__control === "string"
  );
}

export function enqueueStreamError(
  controller: ReadableStreamDefaultController<UIMessageChunk>,
  error: string,
  id = "stream-error",
): void {
  controller.enqueue({ type: "text-start", id } as UIMessageChunk);
  controller.enqueue({ type: "text-delta", id, delta: `\n\n${error}` } as UIMessageChunk);
  controller.enqueue({ type: "text-end", id } as UIMessageChunk);
}
