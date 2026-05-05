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
  const pending = new Map<string, DurableStreamEvent<TChunk>>();
  let lastEmitted = "";
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
          if (!seen.has(eventId) && !pending.has(eventId)) {
            pending.set(eventId, { eventId, chunk: getChunk(event) });
          }
          if (initialReplayComplete && isTerminalEvent?.(event) && !finalReplayInFlight && !finalReplayComplete) {
            finalReplayInFlight = true;
            void replayThenDrain(controller, true);
            return;
          }
          if (initialReplayComplete) scheduleDrain(controller);
        });

        await replayIntoPending(lastEmitted);
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
      await replayIntoPending(lastEmitted);
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
    for (const event of replay.events) {
      if (!seen.has(event.eventId) && !pending.has(event.eventId)) pending.set(event.eventId, event);
    }
    for (const event of attemptSnapshotEvents(replay.attempts ?? [])) {
      if (!seen.has(event.eventId) && !pending.has(event.eventId)) {
        pending.set(event.eventId, event as DurableStreamEvent<TChunk>);
      }
    }
    if (fetchEphemeralChunks) {
      for (const attempt of replay.attempts ?? []) {
        if (attempt.status !== "active") continue;
        const chunks = await fetchEphemeralChunks(attempt, attempt.snapshotSequence);
        for (const chunk of chunks) {
          if (!seen.has(chunk.eventId) && !pending.has(chunk.eventId)) pending.set(chunk.eventId, chunk);
        }
      }
    }
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
    const ordered = [...pending.keys()].sort();
    for (const eventId of ordered) {
      if (eventId <= lastEmitted) {
        pending.delete(eventId);
        seen.add(eventId);
        continue;
      }
      const event = pending.get(eventId);
      pending.delete(eventId);
      seen.add(eventId);
      lastEmitted = eventId;
      if (event) controller.enqueue(event);
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
