import type { UIMessageChunk } from "ai";
import type { DurableChatControlFrame } from "./types";
import type { DurableStreamEvent } from "../core/types";

type ToolChunkWithName = UIMessageChunk & {
  toolCallId?: string;
  toolName?: string;
  dynamic?: boolean;
  providerMetadata?: Record<string, unknown>;
};

type RememberedToolChunk = {
  toolName: string;
  dynamic?: boolean;
  providerMetadata?: Record<string, unknown>;
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
  const toolNamesByCallId = new Map<string, RememberedToolChunk>();

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
                providerMetadata: isRecord(toolChunk.providerMetadata)
                  ? toolChunk.providerMetadata
                  : undefined,
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
  toolNamesByCallId = new Map<string, RememberedToolChunk>(),
): UIMessageChunk[] {
  if (!chunk || typeof chunk !== "object") return [];
  const value = chunk as Record<string, unknown>;
  const normalizedValue = normalizeProviderMetadata(value);

  if (value.type === "tool-output-available" || value.type === "tool-output-error") {
    const toolCallId = typeof value.toolCallId === "string" ? value.toolCallId : undefined;
    const remembered = toolCallId ? toolNamesByCallId.get(toolCallId) : undefined;
    if (remembered) {
      const outputValue = attachRememberedProviderMetadata(normalizedValue, remembered.providerMetadata);
      return [
        {
          ...outputValue,
          toolName: typeof value.toolName === "string" ? value.toolName : remembered.toolName,
          dynamic: value.dynamic === undefined ? remembered.dynamic : value.dynamic,
        } as UIMessageChunk,
      ];
    }
  }

  return [normalizedValue as UIMessageChunk];
}

function attachRememberedProviderMetadata(
  value: Record<string, unknown>,
  rememberedProviderMetadata?: Record<string, unknown>,
): Record<string, unknown> {
  if (!rememberedProviderMetadata) return value;
  const providerMetadata = isRecord(value.providerMetadata) ? value.providerMetadata : {};
  if (isRecord(providerMetadata.temporal)) return value;
  return {
    ...value,
    providerMetadata: {
      ...rememberedProviderMetadata,
      ...providerMetadata,
    },
  };
}

function normalizeProviderMetadata(value: Record<string, unknown>): Record<string, unknown> {
  if (!isToolLifecycleChunk(value)) return value;
  const metadata = value.metadata;
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) return value;

  const { metadata: _metadata, providerMetadata, ...rest } = value;
  return {
    ...rest,
    providerMetadata: {
      ...(isRecord(providerMetadata) ? providerMetadata : {}),
      temporal: metadata,
    },
  };
}

function isToolLifecycleChunk(value: Record<string, unknown>): boolean {
  return (
    value.type === "tool-input-available" ||
    value.type === "tool-input-error" ||
    value.type === "tool-output-available" ||
    value.type === "tool-output-error"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
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
