export type StreamPhase = "provider-live" | "canonical";

export type StreamLane = "text" | "reasoning" | "object" | "tool-input";

export type StreamEvent =
  | "stream-start"
  | "response-metadata"
  | "text-delta"
  | "reasoning-delta"
  | "tool-input-delta"
  | "tool-input-end"
  | "tool-call"
  | "element"
  | "file"
  | "source"
  | "finish"
  | "abort"
  | "snapshot"
  | "attempt-commit"
  | "attempt-discard"
  | "attempt-cancel"
  | "attempt-fail";

export type ProviderStreamEvent = Exclude<
  StreamEvent,
  "snapshot" | "attempt-commit" | "attempt-discard" | "attempt-cancel" | "attempt-fail"
>;

export type AttemptStatus =
  | "active"
  | "committed"
  | "discarded"
  | "canceled"
  | "failed";

export type ToolLifecycleEvent =
  | "tool-input-available"
  | "tool-output-available"
  | "tool-output-error";

export type TaskLifecycleEvent =
  | "task-plan-created"
  | "task-started"
  | "task-completed"
  | "task-failed";

export type StreamOptions = {
  visible?: boolean;
  streamId?: string;
  lane?: StreamLane;
  attemptId?: string;
  snapshotEveryChunks?: number;
  snapshotEveryChars?: number;
  persistEphemeralChunks?: boolean;
};

export type AttemptRef = {
  streamId: string;
  phase: StreamPhase;
  lane: StreamLane;
  attemptId: string;
  partId?: string;
  toolCallId?: string;
  toolName?: string;
};

export type AttemptSnapshot = AttemptRef & {
  sequence: number;
  snapshotText?: string;
  snapshotObject?: unknown;
};

export type LiveChunk = AttemptRef & {
  event: ProviderStreamEvent;
  sequence: number;
  delta?: string;
  input?: unknown;
  element?: unknown;
  providerPart?: unknown;
};

export type EphemeralChunk = LiveChunk;

export type AttemptCompletion = AttemptRef & {
  sequence: number;
  status: AttemptStatus;
  reason?: string;
};

export type ToolLifecycleInput = {
  streamId: string;
  event: ToolLifecycleEvent;
  toolCallId: string;
  toolName: string;
  input?: unknown;
  output?: unknown;
  errorText?: string;
  dynamic?: boolean;
  providerExecuted?: boolean;
  preliminary?: boolean;
  metadata?: Record<string, unknown>;
};

export type PlannedTask = {
  id: string;
  title: string;
  objective?: string;
  skillNames?: string[];
  dependsOn?: string[];
  execution?: "parallel" | "sequential" | string;
};

export type TaskPlan = {
  planId?: string;
  summary?: string;
  execution: "parallel" | "sequential" | string;
  reason?: string;
  requiresSynthesis?: boolean;
  tasks?: PlannedTask[];
};

export type TaskResult = {
  taskId: string;
  title?: string;
  text?: string;
  agentResult?: unknown;
};

export type TaskLifecycleData = {
  event: TaskLifecycleEvent | string;
  id?: string;
  plan?: TaskPlan;
  task?: PlannedTask;
  result?: TaskResult;
  error?: string;
  [key: string]: unknown;
};

export type DurableStreamData =
  | (AttemptSnapshot & {
      event: "snapshot";
      status?: AttemptStatus;
    })
  | LiveChunk
  | (AttemptCompletion & {
      event: "attempt-commit" | "attempt-discard" | "attempt-cancel" | "attempt-fail";
    });

export type DurableStreamAttempt = {
  streamId: string;
  lane: StreamLane;
  attemptId: string;
  partId?: string;
  toolCallId?: string;
  toolName?: string;
  status: AttemptStatus;
  snapshotText?: string;
  snapshotObject?: unknown;
  snapshotSequence: number;
  updatedAt: number;
};

export type DurableStreamEvent<TChunk = unknown> = {
  eventId: string;
  chunk: TChunk;
  createdAt?: number;
};

export type DurableReplayResponse<TChunk = unknown> = {
  streamId: string;
  events: Array<DurableStreamEvent<TChunk>>;
  attempts?: DurableStreamAttempt[];
};

export type StreamLease = {
  streamId: string;
  channel?: string;
  redisStream?: string;
  ownerUserId?: string;
  scopeId?: string;
  parentConversationId?: string;
  conversationId?: string;
  replayAttributes?: Record<string, unknown>;
};

export type StreamResolver = (
  streamId: string,
  signal?: AbortSignal,
) => Promise<StreamLease | undefined>;
