import type {
  DurableReplayResponse,
  DurableStreamAttempt,
  DurableStreamData,
  DurableStreamEvent,
  EphemeralChunk,
} from "./types";

export type LiveSubscription = {
  close(): void;
};

export type SubscribeFirstReplayOptions<TLiveEvent, TChunk = unknown> = {
  subscribe(onEvent: (event: TLiveEvent) => void): Promise<LiveSubscription>;
  fetchReplay(afterEventId: string): Promise<DurableReplayResponse<TChunk>>;
  fetchEphemeralChunks?(
    attempt: DurableStreamAttempt,
    afterSequence: number,
  ): Promise<Array<DurableStreamEvent<TChunk>>>;
  getEventId(event: TLiveEvent): string;
  getChunk(event: TLiveEvent): TChunk;
  isTerminalEvent?: (event: TLiveEvent) => boolean;
  drainDelayMs?: number;
};

export function createSubscribeFirstReplayStream<TLiveEvent, TChunk = unknown>({
  subscribe,
  fetchReplay,
  fetchEphemeralChunks,
  getEventId,
  getChunk,
  isTerminalEvent,
  drainDelayMs = 75,
}: SubscribeFirstReplayOptions<TLiveEvent, TChunk>): ReadableStream<DurableStreamEvent<TChunk>> {
  const seen = new Set<string>();
  const pending = new Map<string, PendingEvent<TChunk>>();
  let nextPendingOrder = 0;
  let replayCursor = "";
  let live: LiveSubscription | null = null;
  let initialReplayComplete = false;
  let finalReplayInFlight = false;
  let finalReplayComplete = false;
  let drainTimer: ReturnType<typeof setTimeout> | null = null;
  let closed = false;

  return new ReadableStream<DurableStreamEvent<TChunk>>({
    async start(controller) {
      try {
        live = await subscribe((event) => {
          const eventId = getEventId(event);
          addPending({ eventId, chunk: getChunk(event) });
          if (initialReplayComplete && isTerminalEvent?.(event) && !finalReplayInFlight && !finalReplayComplete) {
            finalReplayInFlight = true;
            void replayThenDrain(controller, true);
            return;
          }
          if (initialReplayComplete) scheduleDrain(controller);
        });

        await replayIntoPending(replayCursor);
        initialReplayComplete = true;
        scheduleDrain(controller);
      } catch (error) {
        closeLive();
        controller.error(error);
      }
    },
    cancel() {
      closed = true;
      if (drainTimer) clearTimeout(drainTimer);
      closeLive();
    },
  });

  async function replayThenDrain(
    controller: ReadableStreamDefaultController<DurableStreamEvent<TChunk>>,
    final: boolean,
  ) {
    try {
      await replayIntoPending(final ? "" : replayCursor);
      if (final) finalReplayComplete = true;
      scheduleDrain(controller);
    } catch (error) {
      closeLive();
      controller.error(error);
    } finally {
      if (final) finalReplayInFlight = false;
    }
  }

  async function replayIntoPending(afterEventId: string) {
    const replay = await fetchReplay(afterEventId);
    for (const event of attemptSnapshotEvents(replay.attempts ?? [])) {
      addPending(event as DurableStreamEvent<TChunk>);
    }
    for (const event of replay.events) {
      addPending(event);
    }
    if (fetchEphemeralChunks) {
      for (const attempt of replay.attempts ?? []) {
        if (attempt.status !== "active") continue;
        const chunks = await fetchEphemeralChunks(attempt, attempt.snapshotSequence);
        for (const chunk of chunks) {
          addPending(chunk);
        }
      }
    }
  }

  function addPending(event: DurableStreamEvent<TChunk>) {
    if (seen.has(event.eventId) || pending.has(event.eventId)) return;
    pending.set(event.eventId, { event, order: nextPendingOrder });
    nextPendingOrder += 1;
  }

  function scheduleDrain(controller: ReadableStreamDefaultController<DurableStreamEvent<TChunk>>) {
    if (closed || drainTimer) return;
    drainTimer = setTimeout(() => {
      drainTimer = null;
      drain(controller);
    }, drainDelayMs);
  }

  function drain(controller: ReadableStreamDefaultController<DurableStreamEvent<TChunk>>) {
    if (closed) return;
    const ordered = [...pending.entries()].sort(([, left], [, right]) => comparePendingEvents(left, right));
    for (const [eventId, pendingEvent] of ordered) {
      pending.delete(eventId);
      if (seen.has(eventId)) continue;
      seen.add(eventId);
      if (isReplayCursorEventId(eventId) && (replayCursor === "" || eventId > replayCursor)) {
        replayCursor = eventId;
      }
      controller.enqueue(pendingEvent.event);
    }
  }

  function closeLive() {
    try {
      live?.close();
    } finally {
      live = null;
    }
  }
}

type PendingEvent<TChunk> = {
  event: DurableStreamEvent<TChunk>;
  order: number;
};

function comparePendingEvents<TChunk>(left: PendingEvent<TChunk>, right: PendingEvent<TChunk>): number {
  const leftPriority = pendingPriority(left.event);
  const rightPriority = pendingPriority(right.event);
  if (leftPriority !== rightPriority) return leftPriority - rightPriority;

  if (isReplayCursorEventId(left.event.eventId) && isReplayCursorEventId(right.event.eventId)) {
    return left.event.eventId.localeCompare(right.event.eventId) || left.order - right.order;
  }

  return left.order - right.order;
}

function pendingPriority<TChunk>(event: DurableStreamEvent<TChunk>): number {
  if (event.eventId.startsWith("!attempt#")) return 0;
  if (isDoneChunk(event.chunk)) return 2;
  return 1;
}

function isReplayCursorEventId(eventId: string): boolean {
  return /^[0-9A-HJKMNP-TV-Z]+$/.test(eventId);
}

function isDoneChunk(chunk: unknown): boolean {
  return (
    typeof chunk === "object" &&
    chunk !== null &&
    "__control" in chunk &&
    (chunk as { __control?: unknown }).__control === "done"
  );
}

export function attemptSnapshotEvents(
  attempts: DurableStreamAttempt[],
): Array<DurableStreamEvent<{ type: "data-llm-stream"; id: string; data: DurableStreamData; transient: true }>> {
  return attempts
    .filter((attempt) => attempt.status === "active" || attempt.status === "committed")
    .map((attempt) => {
      const data: DurableStreamData = {
        event: "snapshot",
        streamId: attempt.streamId,
        phase: "provider-live",
        lane: attempt.lane,
        attemptId: attempt.attemptId,
        partId: attempt.partId,
        toolCallId: attempt.toolCallId,
        toolName: attempt.toolName,
        displayMode: attempt.displayMode,
        agentId: attempt.agentId,
        taskId: attempt.taskId,
        taskTitle: attempt.taskTitle,
        skillName: attempt.skillName,
        stepId: attempt.stepId,
        stepNumber: attempt.stepNumber,
        stepType: attempt.stepType,
        sequence: attempt.snapshotSequence,
        snapshotText: attempt.snapshotText,
        snapshotObject: attempt.snapshotObject,
        status: attempt.status,
      };
      return {
        eventId: snapshotEventId(attempt),
        chunk: {
          type: "data-llm-stream",
          id: [attempt.lane, attempt.toolCallId, attempt.attemptId].filter(Boolean).join(":"),
          data,
          transient: true,
        },
      };
    });
}

export function ephemeralChunkEvent(
  chunk: EphemeralChunk,
  eventId?: string,
): DurableStreamEvent<{ type: "data-llm-stream"; id: string; data: DurableStreamData; transient: true }> {
  return {
    eventId:
      eventId ??
      `ephemeral#${chunk.streamId}#${chunk.lane}#${chunk.toolCallId ?? "_"}#${chunk.attemptId}#${chunk.sequence}`,
    chunk: {
      type: "data-llm-stream",
      id: [chunk.lane, chunk.toolCallId, chunk.attemptId].filter(Boolean).join(":"),
      data: chunk,
      transient: true,
    },
  };
}

function snapshotEventId(attempt: DurableStreamAttempt): string {
  return `!attempt#${attempt.streamId}#${attempt.lane}#${attempt.toolCallId ?? "_"}#${attempt.attemptId}`;
}
