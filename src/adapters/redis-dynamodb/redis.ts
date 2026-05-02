import { createSubscribeFirstReplayStream, type LiveSubscription } from "../../core/replay";
import type { DurableReplayResponse, DurableStreamEvent } from "../../core/types";
import { TemporalDurableChatTransport } from "../../ai-sdk/transport";
import type {
  DurableChatAck,
  TemporalDurableChatTransportOptions,
} from "../../ai-sdk/types";
import { resolveRedisEnv } from "../../vercel/env";

export type RedisLiveClient = {
  publish?(channel: string, payload: string): Promise<unknown>;
  subscribe?(channel: string, onMessage: (payload: string) => void): Promise<LiveSubscription>;
  xadd?(stream: string, id: string, values: Record<string, string>): Promise<unknown>;
  xread?(
    stream: string,
    afterId: string,
    options?: { blockMs?: number; count?: number },
  ): Promise<Array<{ id: string; values: Record<string, string> }>>;
};

export type RedisDurableStreamOptions = {
  streamId: string;
  channel?: string;
  redisStream?: string;
  sseEndpoint?: string;
  sseEndpointPrefix?: string;
  replayEndpoint?: string;
  replayEndpointPrefix?: string;
  fetch?: typeof fetch;
  abortSignal?: AbortSignal;
  drainDelayMs?: number;
};

export function redisChannel(streamId: string, prefix = resolveRedisEnv().channelPrefix): string {
  return `${prefix}${streamId}`;
}

export function redisStreamKey(streamId: string, prefix = resolveRedisEnv().streamPrefix): string {
  return `${prefix}${streamId}`;
}

export function createRedisSSEDurableStream(
  options: RedisDurableStreamOptions,
): ReadableStream<DurableStreamEvent> {
  const env = resolveRedisEnv();
  const fetchImpl = options.fetch ?? fetch;
  const sseEndpoint =
    options.sseEndpoint ??
    `${options.sseEndpointPrefix ?? env.sseEndpointPrefix}/${encodeURIComponent(options.streamId)}/sse`;
  const replayEndpoint =
    options.replayEndpoint ??
    `${options.replayEndpointPrefix ?? "/api/chat-streams"}/${encodeURIComponent(options.streamId)}/events`;

  return createSubscribeFirstReplayStream<DurableStreamEvent>({
    drainDelayMs: options.drainDelayMs,
    getEventId: (event) => event.eventId,
    getChunk: (event) => event.chunk,
    subscribe: (onEvent) =>
      subscribeToDurableSSE({
        endpoint: sseEndpoint,
        fetch: fetchImpl,
        abortSignal: options.abortSignal,
        onEvent,
      }),
    fetchReplay: async (afterEventId) => {
      const url = new URL(replayEndpoint, "http://durable-stream.local");
      url.searchParams.set("after", afterEventId);
      const response = await fetchImpl(toRelativeUrl(url, replayEndpoint), {
        headers: { Accept: "application/json" },
        signal: options.abortSignal,
      });
      if (!response.ok) {
        throw new Error(`Replay failed: ${response.status} ${response.statusText}`);
      }
      return (await response.json()) as DurableReplayResponse;
    },
  });
}

export function createRedisTemporalChatTransport<TAck extends DurableChatAck = DurableChatAck>(
  options: Omit<TemporalDurableChatTransportOptions<TAck>, "streamFactory"> &
    Omit<RedisDurableStreamOptions, "streamId" | "abortSignal"> = {},
): TemporalDurableChatTransport<any, TAck> {
  return new TemporalDurableChatTransport<any, TAck>({
    ...options,
    streamFactory: (ack, streamOptions) =>
      createRedisSSEDurableStream({
        ...options,
        streamId: ack.streamId,
        channel: ack.channel ?? options.channel,
        redisStream: ack.redisStream ?? options.redisStream,
        abortSignal: streamOptions.abortSignal,
      }),
  });
}

export async function publishRedisDurableEvent(options: {
  redis: RedisLiveClient;
  streamId: string;
  event: DurableStreamEvent;
  channel?: string;
  redisStream?: string;
}): Promise<void> {
  const payload = JSON.stringify(options.event);
  const channel = options.channel ?? redisChannel(options.streamId);
  const stream = options.redisStream ?? redisStreamKey(options.streamId);
  await options.redis.publish?.(channel, payload);
  await options.redis.xadd?.(stream, options.event.eventId, { payload });
}

export function createUpstashRedisRestClient(options: {
  url?: string;
  token?: string;
  fetch?: typeof fetch;
} = {}): RedisLiveClient {
  const env = resolveRedisEnv();
  const url = (options.url ?? env.url)?.replace(/\/+$/, "");
  const token = options.token ?? env.token;
  const fetchImpl = options.fetch ?? fetch;
  if (!url || !token) {
    throw new Error("Upstash Redis REST url/token are required");
  }
  const endpoint = url;

  async function command(args: unknown[]): Promise<unknown> {
    const response = await fetchImpl(endpoint, {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(args),
    });
    if (!response.ok) {
      throw new Error(`Upstash Redis command failed: ${response.status} ${response.statusText}`);
    }
    const body = (await response.json()) as { result?: unknown; error?: string };
    if (body.error) throw new Error(body.error);
    return body.result;
  }

  return {
    publish: (channel, payload) => command(["PUBLISH", channel, payload]),
    xadd: (stream, id, values) => command(["XADD", stream, id, ...Object.entries(values).flat()]),
    async xread(stream, afterId, options = {}) {
      const result = await command([
        "XREAD",
        "COUNT",
        String(options.count ?? 100),
        "STREAMS",
        stream,
        afterId,
      ]);
      return parseXReadResult(result);
    },
  };
}

export async function subscribeToDurableSSE(options: {
  endpoint: string;
  fetch?: typeof fetch;
  abortSignal?: AbortSignal;
  onEvent: (event: DurableStreamEvent) => void;
}): Promise<LiveSubscription> {
  const controller = new AbortController();
  const signal = linkAbortSignals(controller.signal, options.abortSignal);
  const response = await (options.fetch ?? fetch)(options.endpoint, {
    headers: { Accept: "text/event-stream" },
    signal,
  });
  if (!response.ok || !response.body) {
    throw new Error(`SSE subscription failed: ${response.status} ${response.statusText}`);
  }

  void readSSE(response.body, options.onEvent, signal);
  return { close: () => controller.abort() };
}

export async function readSSE(
  body: ReadableStream<Uint8Array>,
  onEvent: (event: DurableStreamEvent) => void,
  abortSignal?: AbortSignal,
): Promise<void> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (!abortSignal?.aborted) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let index = buffer.indexOf("\n\n");
      while (index >= 0) {
        const frame = buffer.slice(0, index);
        buffer = buffer.slice(index + 2);
        const data = frame
          .split("\n")
          .filter((line) => line.startsWith("data:"))
          .map((line) => line.slice(5).trimStart())
          .join("\n");
        if (data) {
          const parsed = JSON.parse(data) as DurableStreamEvent;
          onEvent(parsed);
        }
        index = buffer.indexOf("\n\n");
      }
    }
  } finally {
    reader.releaseLock();
  }
}

function parseXReadResult(result: unknown): Array<{ id: string; values: Record<string, string> }> {
  if (!Array.isArray(result)) return [];
  const firstStream = result[0];
  if (!Array.isArray(firstStream) || !Array.isArray(firstStream[1])) return [];
  return firstStream[1].flatMap((entry: unknown) => {
    if (!Array.isArray(entry) || typeof entry[0] !== "string" || !Array.isArray(entry[1])) return [];
    const values: Record<string, string> = {};
    for (let i = 0; i < entry[1].length; i += 2) {
      const key = entry[1][i];
      const value = entry[1][i + 1];
      if (typeof key === "string" && typeof value === "string") values[key] = value;
    }
    return [{ id: entry[0], values }];
  });
}

function linkAbortSignals(primary: AbortSignal, secondary?: AbortSignal): AbortSignal {
  if (!secondary) return primary;
  if (secondary.aborted) return secondary;
  const controller = new AbortController();
  const abort = () => controller.abort();
  primary.addEventListener("abort", abort, { once: true });
  secondary.addEventListener("abort", abort, { once: true });
  return controller.signal;
}

function toRelativeUrl(url: URL, original: string): string {
  return original.startsWith("http://") || original.startsWith("https://")
    ? url.toString()
    : `${url.pathname}${url.search}`;
}
