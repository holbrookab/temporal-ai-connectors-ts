import type { UIMessageChunk } from "ai";
import type { DurableChatControlFrame } from "./types";
import type { DurableStreamEvent, HumanCheckpointData } from "../core/types";

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
  input?: unknown;
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
                input: "input" in uiChunk ? uiChunk.input : undefined,
                providerMetadata: isRecord(toolChunk.providerMetadata)
                  ? toolChunk.providerMetadata
                  : undefined,
              });
            }
            if (
              uiChunk.type === "tool-output-available" ||
              uiChunk.type === "tool-output-error" ||
              uiChunk.type === "tool-output-denied"
            ) {
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

  if (value.type === "tool-approval-request") {
    const toolCallId = typeof value.toolCallId === "string" ? value.toolCallId : undefined;
    const remembered = toolCallId ? toolNamesByCallId.get(toolCallId) : undefined;
    const approvalValue = attachRememberedProviderMetadata(normalizedValue, remembered?.providerMetadata);
    const toolApprovalChunk = {
      ...approvalValue,
      toolName: typeof value.toolName === "string" ? value.toolName : remembered?.toolName,
      input: value.input === undefined ? remembered?.input : value.input,
      dynamic: value.dynamic === undefined ? remembered?.dynamic : value.dynamic,
    } as UIMessageChunk;
    return [
      toolApprovalChunk,
      humanCheckpointChunkFromApproval(toolApprovalChunk, "request"),
    ];
  }

  if (value.type === "tool-approval-response") {
    const approvalValue = normalizedValue;
    const toolApprovalChunk = approvalValue as UIMessageChunk;
    return [
      toolApprovalChunk,
      humanCheckpointChunkFromApproval(toolApprovalChunk, "response"),
    ];
  }

  if (
    value.type === "tool-output-available" ||
    value.type === "tool-output-error" ||
    value.type === "tool-output-denied"
  ) {
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
  const rememberedTemporal = isRecord(rememberedProviderMetadata.temporal)
    ? rememberedProviderMetadata.temporal
    : {};
  const valueTemporal = isRecord(providerMetadata.temporal) ? providerMetadata.temporal : {};
  return {
    ...value,
    providerMetadata: {
      ...rememberedProviderMetadata,
      ...providerMetadata,
      temporal: {
        ...rememberedTemporal,
        ...valueTemporal,
      },
    },
  };
}

function normalizeProviderMetadata(value: Record<string, unknown>): Record<string, unknown> {
  if (!isToolLifecycleChunk(value)) return value;
  const metadata = isRecord(value.metadata) ? value.metadata : {};
  const temporalScope = temporalScopeFromRecord(value);
  if (Object.keys(metadata).length === 0 && Object.keys(temporalScope).length === 0) return value;

  const { metadata: _metadata, providerMetadata, ...rest } = value;
  return {
    ...rest,
    providerMetadata: {
      ...(isRecord(providerMetadata) ? providerMetadata : {}),
      temporal: {
        ...metadata,
        ...temporalScope,
      },
    },
  };
}

function isToolLifecycleChunk(value: Record<string, unknown>): boolean {
  return (
    value.type === "tool-input-available" ||
    value.type === "tool-input-error" ||
    value.type === "tool-approval-request" ||
    value.type === "tool-approval-response" ||
    value.type === "tool-output-available" ||
    value.type === "tool-output-error" ||
    value.type === "tool-output-denied"
  );
}

function humanCheckpointChunkFromApproval(
  chunk: UIMessageChunk,
  phase: "request" | "response",
): UIMessageChunk {
  const value = chunk as Record<string, unknown>;
  const approvalId = stringValue(value.approvalId) ?? stringValue(value.toolCallId) ?? "approval";
  const toolCallId = stringValue(value.toolCallId) ?? approvalId;
  const toolName = stringValue(value.toolName);
  const input = value.input;
  const providerMetadata = isRecord(value.providerMetadata) ? value.providerMetadata : {};
  const temporalMetadata = isRecord(providerMetadata.temporal) ? providerMetadata.temporal : {};
  const temporalScope = temporalScopeFromRecord(value);
  const metadata = {
    ...temporalMetadata,
    ...temporalScope,
    approvalId,
    checkpointId: approvalId,
    toolCallId,
    ...(toolName ? { toolName } : {}),
    ...(input !== undefined ? { input } : {}),
  };
  const title = approvalTitle(toolName);
  const approved = typeof value.approved === "boolean" ? value.approved : undefined;
  const reason = stringValue(value.reason);
  const data: HumanCheckpointData & Record<string, unknown> = {
    event: phase === "request" ? "checkpoint-created" : "checkpoint-submitted",
    checkpointId: approvalId,
    approvalId,
    toolCallId,
    ...temporalScope,
    ...(toolName ? { toolName } : {}),
    title,
    summary: approvalSummary(title, input),
    status: phase === "request" ? "pending" : approved === false ? "denied" : "approved",
    metadata,
    ...(phase === "request"
      ? {
          questions: [
            {
              id: approvalId,
              title,
              prompt: approvalSummary(title, input),
              choices: [
                {
                  id: "approve",
                  label: "Approve",
                  description: "Continue this workflow with the proposed action.",
                  value: { approved: true },
                },
                {
                  id: "deny",
                  label: "Deny",
                  description: "Stop this action and ask the assistant to adjust.",
                  value: { approved: false },
                },
              ],
              allowCustom: true,
              required: true,
            },
          ],
        }
      : {
          approved,
          reason,
          answers: [
            {
              questionId: approvalId,
              choiceId: approved === false ? "deny" : "approve",
              value: approved,
              ...(reason ? { customText: reason } : {}),
            },
          ],
        }),
  };

  return {
    type: "data-human-checkpoint",
    id: `human-checkpoint-${approvalId}`,
    data,
  } as UIMessageChunk;
}

function temporalScopeFromRecord(value: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  copyStringField(out, value, "displayMode");
  copyStringField(out, value, "agentId");
  copyStringField(out, value, "taskId");
  copyStringField(out, value, "taskTitle");
  copyStringField(out, value, "skillName");
  copyStringField(out, value, "stepId");
  copyNumberField(out, value, "stepNumber");
  copyStringField(out, value, "stepType");
  return out;
}

function copyStringField(out: Record<string, unknown>, input: Record<string, unknown>, key: string): void {
  const value = input[key];
  if (typeof value === "string" && value) out[key] = value;
}

function copyNumberField(out: Record<string, unknown>, input: Record<string, unknown>, key: string): void {
  const value = input[key];
  if (typeof value === "number" && Number.isFinite(value)) out[key] = value;
}

function approvalTitle(toolName?: string): string {
  if (!toolName) return "Review action";
  return `Review ${toolName.replaceAll("_", " ")}`;
}

function approvalSummary(title: string, input: unknown): string {
  if (input === undefined) return `Review whether the assistant should continue with ${title}.`;
  return `Review whether the assistant should continue with ${title}.`;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
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
