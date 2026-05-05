export type StreamPhase = "provider-live" | "canonical";

export type StreamLane = "text" | "reasoning" | "object" | "tool-input";

export type StreamEvent =
  | "stream-start"
  | "start-step"
  | "response-metadata"
  | "text-delta"
  | "reasoning-delta"
  | "tool-input-delta"
  | "tool-input-end"
  | "tool-call"
  | "element"
  | "file"
  | "source"
  | "finish-step"
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
  | "tool-approval-request"
  | "tool-approval-response"
  | "tool-output-available"
  | "tool-output-error"
  | "tool-output-denied";

export type TaskLifecycleEvent =
  | "task-plan-created"
  | "task-plan-updated"
  | "task-started"
  | "task-completed"
  | "task-failed"
  | "task-skipped"
  | "task-blocked";

export type TaskStatus =
  | "planned"
  | "active"
  | "completed"
  | "failed"
  | "skipped"
  | "blocked";

export type TaskResultStatus =
  | "complete"
  | "blocked"
  | "needs_user"
  | "alternate_path";

export type DisplayMode = "assistant" | "task" | "hidden" | string;

export type StreamScope = {
  displayMode?: DisplayMode;
  agentId?: string;
  taskId?: string;
  taskTitle?: string;
  skillName?: string;
  stepId?: string;
  stepNumber?: number;
  stepType?: string;
};

export type StreamOptions = {
  visible?: boolean;
  streamId?: string;
  lane?: StreamLane;
  attemptId?: string;
  snapshotEveryChunks?: number;
  snapshotEveryChars?: number;
  persistEphemeralChunks?: boolean;
} & StreamScope;

export type AttemptRef = StreamScope & {
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
  snapshotText?: string;
  snapshotObject?: unknown;
};

export type ToolLifecycleInput = StreamScope & {
  streamId: string;
  event: ToolLifecycleEvent;
  toolCallId: string;
  toolName: string;
  approvalId?: string;
  approved?: boolean;
  reason?: string;
  isAutomatic?: boolean;
  input?: unknown;
  output?: unknown;
  errorText?: string;
  dynamic?: boolean;
  providerExecuted?: boolean;
  preliminary?: boolean;
  metadata?: Record<string, unknown>;
};

export type HumanCheckpointChoice = {
  id: string;
  label: string;
  description?: string;
  value?: unknown;
};

export type HumanCheckpointQuestion = {
  id: string;
  title?: string;
  prompt: string;
  choices?: HumanCheckpointChoice[];
  allowCustom?: boolean;
  required?: boolean;
};

export type HumanCheckpointAnswer = {
  questionId: string;
  choiceId?: string;
  customText?: string;
  value?: unknown;
};

export type HumanCheckpointData = {
  event:
    | "checkpoint-created"
    | "checkpoint-submitted"
    | "checkpoint-canceled"
    | "checkpoint-expired"
    | string;
  checkpointId: string;
  title?: string;
  summary?: string;
  status?: "pending" | "submitted" | "canceled" | "expired" | string;
  questions?: HumanCheckpointQuestion[];
  answers?: HumanCheckpointAnswer[];
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
  status?: TaskResultStatus | string;
  summary?: string;
  blocker?: string;
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
} & StreamScope;

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
