import type {
  AttemptStatus,
  DurableStreamAttempt,
  DurableStreamData,
  StreamLane,
} from "./types";

export type DurableStreamAttemptState = {
  streamId: string;
  lane: StreamLane;
  attemptId: string;
  partId?: string;
  toolCallId?: string;
  toolName?: string;
  status: AttemptStatus;
  text: string;
  sequence: number;
  updatedAt?: number;
};

export type DurableStreamState = Record<string, DurableStreamAttemptState>;

export function durableAttemptKey(input: {
  streamId: string;
  lane: StreamLane;
  attemptId: string;
  toolCallId?: string;
}): string {
  return `${input.streamId}:${input.lane}:${input.toolCallId ?? "_"}:${input.attemptId}`;
}

export function applyAttemptManifests(
  state: DurableStreamState,
  attempts: DurableStreamAttempt[],
): DurableStreamState {
  let next = state;
  for (const attempt of attempts) {
    const key = durableAttemptKey(attempt);
    const existing = next[key];
    if (existing && existing.sequence > attempt.snapshotSequence) continue;
    next = {
      ...next,
      [key]: {
        streamId: attempt.streamId,
        lane: attempt.lane,
        attemptId: attempt.attemptId,
        partId: attempt.partId,
        toolCallId: attempt.toolCallId,
        toolName: attempt.toolName,
        status: attempt.status,
        text: attempt.status === "active" ? (attempt.snapshotText ?? "") : "",
        sequence: attempt.snapshotSequence,
        updatedAt: attempt.updatedAt,
      },
    };
  }
  return pruneInactive(next);
}

export function applyDurableStreamData(
  state: DurableStreamState,
  data: DurableStreamData,
): DurableStreamState {
  const key = durableAttemptKey(data);
  if (data.event === "snapshot") {
    return {
      ...state,
      [key]: {
        streamId: data.streamId,
        lane: data.lane,
        attemptId: data.attemptId,
        partId: data.partId,
        toolCallId: data.toolCallId,
        toolName: data.toolName,
        status: data.status ?? "active",
        sequence: data.sequence,
        text: data.status && data.status !== "active" ? "" : (data.snapshotText ?? ""),
      },
    };
  }

  if (
    data.event === "attempt-commit" ||
    data.event === "attempt-discard" ||
    data.event === "attempt-cancel" ||
    data.event === "attempt-fail"
  ) {
    const existing = state[key];
    return {
      ...state,
      [key]: {
        ...(existing ?? {
          streamId: data.streamId,
          lane: data.lane,
          attemptId: data.attemptId,
          partId: data.partId,
          toolCallId: data.toolCallId,
          toolName: data.toolName,
          text: "",
        }),
        status: data.status,
        sequence: data.sequence,
        text: data.status === "discarded" || data.status === "failed" ? "" : (existing?.text ?? ""),
      },
    };
  }

  const existing = state[key] ?? {
    streamId: data.streamId,
    lane: data.lane,
    attemptId: data.attemptId,
    partId: data.partId,
    toolCallId: data.toolCallId,
    toolName: data.toolName,
    status: "active" as const,
    text: "",
    sequence: 0,
  };
  if (data.sequence <= existing.sequence) return state;

  const shouldAppend =
    data.event === "text-delta" ||
    data.event === "reasoning-delta" ||
    data.event === "tool-input-delta";

  return {
    ...state,
    [key]: {
      ...existing,
      partId: data.partId ?? existing.partId,
      toolCallId: data.toolCallId ?? existing.toolCallId,
      toolName: data.toolName ?? existing.toolName,
      status: "active",
      sequence: data.sequence,
      text: shouldAppend ? `${existing.text}${data.delta ?? ""}` : existing.text,
    },
  };
}

export function selectActiveStreamText(
  state: DurableStreamState,
  streamId: string,
  lane: StreamLane,
): string {
  return selectTextByStatus(state, streamId, lane, new Set(["active"]));
}

export function selectVisibleStreamText(
  state: DurableStreamState,
  streamId: string,
  lane: StreamLane,
): string {
  return selectTextByStatus(state, streamId, lane, new Set(["active", "committed"]));
}

function selectTextByStatus(
  state: DurableStreamState,
  streamId: string,
  lane: StreamLane,
  statuses: Set<AttemptStatus>,
): string {
  return Object.values(state)
    .filter((attempt) => attempt.streamId === streamId && attempt.lane === lane)
    .filter((attempt) => statuses.has(attempt.status))
    .sort((a, b) => (a.updatedAt ?? 0) - (b.updatedAt ?? 0))
    .map((attempt) => attempt.text)
    .filter(Boolean)
    .join("");
}

function pruneInactive(state: DurableStreamState): DurableStreamState {
  return Object.fromEntries(
    Object.entries(state).filter(([, attempt]) => attempt.status === "active"),
  );
}
