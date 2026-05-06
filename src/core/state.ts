import type {
  AttemptStatus,
  DurableStreamAttempt,
  DurableStreamData,
  DisplayMode,
  StreamLane,
  StreamScope,
} from "./types";

export type DurableStreamAttemptState = StreamScope & {
  streamId: string;
  lane: StreamLane;
  attemptId: string;
  partId?: string;
  toolCallId?: string;
  toolName?: string;
  status: AttemptStatus;
  text: string;
  object?: unknown;
  sequence: number;
  updatedAt?: number;
};

export type DurableStreamState = Record<string, DurableStreamAttemptState>;

export function durableAttemptKey(input: {
  streamId: string;
  lane: StreamLane;
  attemptId: string;
  stepId?: string;
  toolCallId?: string;
}): string {
  return `${input.streamId}:${input.lane}:${input.stepId ?? "_"}:${input.toolCallId ?? "_"}:${input.attemptId}`;
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
        ...scopeFrom(attempt),
        status: attempt.status,
        text:
          attempt.status === "active" || attempt.status === "committed"
            ? (attempt.snapshotText ?? "")
            : "",
        object:
          attempt.status === "active" || attempt.status === "committed"
            ? attempt.snapshotObject
            : undefined,
        sequence: attempt.snapshotSequence,
        updatedAt: attempt.updatedAt,
      },
    };
  }
  return pruneNonRenderable(next);
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
        ...scopeFrom(data),
        status: data.status ?? "active",
        sequence: data.sequence,
        text:
          data.status === "discarded" || data.status === "failed"
            ? ""
            : (data.snapshotText ?? ""),
        object:
          data.status === "discarded" || data.status === "failed"
            ? undefined
            : data.snapshotObject,
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
          ...scopeFrom(data),
          text: "",
          object: undefined,
        }),
        status: data.status,
        sequence: data.sequence,
        text:
          data.status === "discarded" || data.status === "failed"
            ? ""
            : (data.snapshotText ?? existing?.text ?? ""),
        object:
          data.status === "discarded" || data.status === "failed"
            ? undefined
            : (data.snapshotObject ?? existing?.object),
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
    ...scopeFrom(data),
    status: "active" as const,
    text: "",
    object: undefined,
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
      ...mergeScope(existing, scopeFrom(data)),
      status: "active",
      sequence: data.sequence,
      text: shouldAppend ? `${existing.text}${data.delta ?? ""}` : existing.text,
      object: data.snapshotObject ?? existing.object,
    },
  };
}

export function selectActiveStreamText(
  state: DurableStreamState,
  streamId: string,
  lane: StreamLane,
  filter: StreamSelector = {},
): string {
  return selectTextByStatus(state, streamId, lane, new Set(["active"]), filter);
}

export function selectVisibleStreamText(
  state: DurableStreamState,
  streamId: string,
  lane: StreamLane,
  filter: StreamSelector = {},
): string {
  return selectTextByStatus(state, streamId, lane, new Set(["active", "committed"]), filter);
}

export type StreamSelector = StreamScope & {
  displayMode?: DisplayMode;
  toolCallId?: string;
};

export function selectTaskStepStreamText(
  state: DurableStreamState,
  streamId: string,
  input: { taskId: string; stepId?: string; lane?: StreamLane },
): string {
  return selectVisibleStreamText(state, streamId, input.lane ?? "text", {
    displayMode: "task",
    taskId: input.taskId,
    stepId: input.stepId,
  });
}

export function selectToolInputText(
  state: DurableStreamState,
  streamId: string,
  toolCallId: string,
  filter: StreamSelector = {},
): string {
  return selectVisibleStreamText(state, streamId, "tool-input", {
    ...filter,
    toolCallId,
  });
}

export function selectActiveStreamObject(
  state: DurableStreamState,
  streamId: string,
  lane: StreamLane,
  filter: StreamSelector = {},
): unknown {
  return selectObjectByStatus(state, streamId, lane, new Set(["active"]), filter);
}

export function selectVisibleStreamObject(
  state: DurableStreamState,
  streamId: string,
  lane: StreamLane,
  filter: StreamSelector = {},
): unknown {
  return selectObjectByStatus(state, streamId, lane, new Set(["active", "committed"]), filter);
}

export function selectTaskStepStreamObject(
  state: DurableStreamState,
  streamId: string,
  input: { taskId: string; stepId?: string; lane?: StreamLane },
): unknown {
  return selectVisibleStreamObject(state, streamId, input.lane ?? "object", {
    displayMode: "task",
    taskId: input.taskId,
    stepId: input.stepId,
  });
}

function selectTextByStatus(
  state: DurableStreamState,
  streamId: string,
  lane: StreamLane,
  statuses: Set<AttemptStatus>,
  filter: StreamSelector,
): string {
  return Object.values(state)
    .filter((attempt) => attempt.streamId === streamId && attempt.lane === lane)
    .filter((attempt) => statuses.has(attempt.status))
    .filter((attempt) => matchesSelector(attempt, filter))
    .sort((a, b) => (a.updatedAt ?? 0) - (b.updatedAt ?? 0))
    .map((attempt) => attempt.text)
    .filter(Boolean)
    .join("");
}

function selectObjectByStatus(
  state: DurableStreamState,
  streamId: string,
  lane: StreamLane,
  statuses: Set<AttemptStatus>,
  filter: StreamSelector,
): unknown {
  const attempts = Object.values(state)
    .filter((attempt) => attempt.streamId === streamId && attempt.lane === lane)
    .filter((attempt) => statuses.has(attempt.status))
    .filter((attempt) => matchesSelector(attempt, filter))
    .filter((attempt) => attempt.object !== undefined)
    .sort((a, b) => (a.updatedAt ?? 0) - (b.updatedAt ?? 0) || a.sequence - b.sequence);
  return attempts.at(-1)?.object;
}

function matchesSelector(attempt: DurableStreamAttemptState, filter: StreamSelector): boolean {
  if (
    filter.displayMode !== undefined &&
    (attempt.displayMode ?? "assistant") !== filter.displayMode
  ) {
    return false;
  }
  if (filter.agentId !== undefined && attempt.agentId !== filter.agentId) return false;
  if (filter.taskId !== undefined && attempt.taskId !== filter.taskId) return false;
  if (filter.taskTitle !== undefined && attempt.taskTitle !== filter.taskTitle) return false;
  if (filter.skillName !== undefined && attempt.skillName !== filter.skillName) return false;
  if (filter.stepId !== undefined && attempt.stepId !== filter.stepId) return false;
  if (filter.stepNumber !== undefined && attempt.stepNumber !== filter.stepNumber) return false;
  if (filter.stepType !== undefined && attempt.stepType !== filter.stepType) return false;
  if (filter.toolCallId !== undefined && attempt.toolCallId !== filter.toolCallId) return false;
  return true;
}

function scopeFrom(input: StreamScope): StreamScope {
  return {
    ...(input.displayMode !== undefined ? { displayMode: input.displayMode } : {}),
    ...(input.agentId !== undefined ? { agentId: input.agentId } : {}),
    ...(input.taskId !== undefined ? { taskId: input.taskId } : {}),
    ...(input.taskTitle !== undefined ? { taskTitle: input.taskTitle } : {}),
    ...(input.skillName !== undefined ? { skillName: input.skillName } : {}),
    ...(input.stepId !== undefined ? { stepId: input.stepId } : {}),
    ...(input.stepNumber !== undefined ? { stepNumber: input.stepNumber } : {}),
    ...(input.stepType !== undefined ? { stepType: input.stepType } : {}),
  };
}

function mergeScope(base: StreamScope, next: StreamScope): StreamScope {
  return {
    ...scopeFrom(base),
    ...scopeFrom(next),
  };
}

function pruneNonRenderable(state: DurableStreamState): DurableStreamState {
  return Object.fromEntries(
    Object.entries(state).filter(([, attempt]) =>
      attempt.status === "active" || attempt.status === "committed"
    ),
  );
}
