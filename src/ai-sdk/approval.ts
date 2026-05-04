export type ToolApprovalStatus = "pending" | "approved" | "denied";

export type ToolApprovalMessage = {
  id?: string;
  parts?: unknown[];
};

export type ToolApprovalSnapshot = {
  messageId?: string;
  approvalId: string;
  toolCallId: string;
  toolName?: string;
  status: ToolApprovalStatus;
  reason?: string;
  input?: unknown;
  providerMetadata?: Record<string, unknown>;
  isAutomatic?: boolean;
};

export function collectToolApprovals(messages: ToolApprovalMessage[]): ToolApprovalSnapshot[] {
  const approvals = new Map<string, ToolApprovalSnapshot>();
  for (const message of messages) {
    for (const part of message.parts ?? []) {
      if (!isRecord(part)) continue;
      if (!isToolPart(part)) continue;
      const approval = isRecord(part.approval) ? part.approval : undefined;
      if (!approval || typeof approval.id !== "string") continue;

      const approved = typeof approval.approved === "boolean" ? approval.approved : undefined;
      approvals.set(approval.id, {
        messageId: message.id,
        approvalId: approval.id,
        toolCallId: typeof part.toolCallId === "string" ? part.toolCallId : "",
        toolName: toolNameFromPart(part),
        status: approved === undefined ? "pending" : approved ? "approved" : "denied",
        reason: typeof approval.reason === "string" ? approval.reason : undefined,
        input: "input" in part ? part.input : undefined,
        providerMetadata: providerMetadataFromPart(part),
        isAutomatic: typeof approval.isAutomatic === "boolean" ? approval.isAutomatic : undefined,
      });
    }
  }
  return [...approvals.values()];
}

export function getPendingToolApprovals(
  messages: ToolApprovalMessage[],
): ToolApprovalSnapshot[] {
  return collectToolApprovals(messages).filter((approval) => approval.status === "pending");
}

export function getSubmittedToolApprovals(
  messages: ToolApprovalMessage[],
): ToolApprovalSnapshot[] {
  return collectToolApprovals(messages).filter((approval) => approval.status !== "pending");
}

export function getActiveToolApproval(
  messages: ToolApprovalMessage[],
): ToolApprovalSnapshot | undefined {
  return getPendingToolApprovals(messages).at(-1);
}

export function hasPendingToolApproval(messages: ToolApprovalMessage[]): boolean {
  return getActiveToolApproval(messages) !== undefined;
}

function isToolPart(part: Record<string, unknown>): boolean {
  return typeof part.type === "string" && (part.type.startsWith("tool-") || part.type === "dynamic-tool");
}

function toolNameFromPart(part: Record<string, unknown>): string | undefined {
  if (typeof part.toolName === "string") return part.toolName;
  if (typeof part.type === "string" && part.type.startsWith("tool-")) {
    return part.type.slice("tool-".length);
  }
  return undefined;
}

function providerMetadataFromPart(part: Record<string, unknown>): Record<string, unknown> | undefined {
  if (isRecord(part.providerMetadata)) return part.providerMetadata;
  if (isRecord(part.callProviderMetadata)) return part.callProviderMetadata;
  return undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
