import { createSubscribeFirstReplayStream, type LiveSubscription } from "../../core/replay";
import type { DurableReplayResponse, DurableStreamEvent } from "../../core/types";
import { TemporalDurableChatTransport } from "../../ai-sdk/transport";
import type {
  DurableChatAck,
  TemporalDurableChatTransportOptions,
} from "../../ai-sdk/types";
import { resolveAppSyncEnv } from "../../vercel/env";

export type AppSyncAuth = {
  token: string;
  httpDomain?: string;
  realtimeDomain?: string;
  namespace?: string;
};

export type AppSyncDataFrame = {
  type?: string;
  id?: string;
  event?: unknown;
  events?: unknown;
  data?: unknown;
  error?: unknown;
  errors?: unknown;
};

export type AppSyncEvent = DurableStreamEvent;

export type AppSyncDurableStreamOptions = {
  streamId: string;
  owner?: string;
  channel?: string;
  auth?: AppSyncAuth | (() => Promise<AppSyncAuth>);
  authEndpoint?: string;
  replayEndpoint?: string;
  replayEndpointPrefix?: string;
  fetch?: typeof fetch;
  WebSocket?: typeof WebSocket;
  abortSignal?: AbortSignal;
  drainDelayMs?: number;
};

export async function fetchAppSyncAuth(
  options: {
    endpoint?: string;
    fetch?: typeof fetch;
    abortSignal?: AbortSignal;
  } = {},
): Promise<AppSyncAuth> {
  const env = resolveAppSyncEnv();
  const endpoint = options.endpoint ?? env.authEndpoint;
  const fetchImpl = options.fetch ?? fetch;
  const response = await fetchImpl(endpoint, {
    method: "POST",
    headers: { Accept: "application/json" },
    signal: options.abortSignal,
  });
  if (!response.ok) {
    throw new Error(`Stream auth failed: ${response.status} ${response.statusText}`);
  }
  return (await response.json()) as AppSyncAuth;
}

export function createAppSyncDurableStream(
  options: AppSyncDurableStreamOptions,
): ReadableStream<DurableStreamEvent> {
  const env = resolveAppSyncEnv();
  const fetchImpl = options.fetch ?? fetch;
  const replayEndpoint =
    options.replayEndpoint ??
    `${options.replayEndpointPrefix ?? env.replayEndpointPrefix}/${encodeURIComponent(options.streamId)}/events`;

  return createSubscribeFirstReplayStream<AppSyncEvent>({
    drainDelayMs: options.drainDelayMs,
    getEventId: (event) => event.eventId,
    getChunk: (event) => event.chunk,
    subscribe: async (onEvent) =>
      connectAppSyncEvents({
        ...options,
        auth: options.auth ?? (() => fetchAppSyncAuth({
          endpoint: options.authEndpoint,
          fetch: fetchImpl,
          abortSignal: options.abortSignal,
        })),
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

export async function connectAppSyncEvents(options: AppSyncDurableStreamOptions & {
  onEvent: (event: AppSyncEvent) => void;
}): Promise<LiveSubscription> {
  const auth = typeof options.auth === "function" ? await options.auth() : options.auth;
  if (!auth?.httpDomain || !auth.realtimeDomain) {
    throw new Error("AppSync Events domains are not configured");
  }

  const WebSocketImpl = options.WebSocket ?? WebSocket;
  const httpHost = auth.httpDomain.replace(/^https?:\/\//, "");
  const realtimeHost = auth.realtimeDomain.replace(/^wss?:\/\//, "");
  const authorization = {
    Authorization: auth.token,
    host: httpHost,
  };
  const header = encodeBase64Url(authorization);
  const channel =
    options.channel ??
    `/${auth.namespace ?? "chat"}/${options.owner ?? options.streamId}/${options.streamId}`;
  const ws = new WebSocketImpl(`wss://${realtimeHost}/event/realtime`, [
    `header-${header}`,
    "aws-appsync-event-ws",
  ]);

  return new Promise((resolve, reject) => {
    let subscribed = false;

    const rejectOnce = (error: unknown) => {
      if (!subscribed) {
        try {
          ws.close();
        } catch {
          // noop
        }
        reject(error);
      }
    };

    ws.onopen = () => {
      ws.send(JSON.stringify({ type: "connection_init" }));
    };
    ws.onerror = () => rejectOnce(new Error("AppSync stream connection failed"));
    ws.onclose = () => {
      if (!subscribed) rejectOnce(new Error("AppSync stream closed before subscription"));
    };
    ws.onmessage = (message) => {
      let frame: AppSyncDataFrame;
      try {
        frame = JSON.parse(String(message.data));
      } catch {
        return;
      }
      if (frame.type === "connection_ack") {
        ws.send(
          JSON.stringify({
            type: "subscribe",
            id: crypto.randomUUID(),
            channel,
            authorization,
          }),
        );
        return;
      }
      if (frame.type === "subscribe_success") {
        subscribed = true;
        resolve({ close: () => ws.close() });
        return;
      }
      if ((frame.type === "subscribe_error" || frame.type === "error") && !subscribed) {
        rejectOnce(new Error(`AppSync subscribe failed: ${JSON.stringify(frame.error ?? frame.errors ?? frame)}`));
        return;
      }
      if (frame.type === "data") {
        for (const event of extractAppSyncEvents(frame, parseAppSyncEvent)) {
          options.onEvent(event);
        }
      }
    };
  });
}

export function createAppSyncTemporalChatTransport<TAck extends DurableChatAck = DurableChatAck>(
  options: Omit<TemporalDurableChatTransportOptions<TAck>, "streamFactory"> &
    Omit<AppSyncDurableStreamOptions, "streamId" | "owner" | "abortSignal"> = {},
): TemporalDurableChatTransport<any, TAck> {
  return new TemporalDurableChatTransport<any, TAck>({
    ...options,
    streamFactory: (ack, streamOptions) =>
      createAppSyncDurableStream({
        ...options,
        streamId: ack.streamId,
        owner: ack.userId ?? ack.scopeId,
        channel: ack.channel ?? options.channel,
        abortSignal: streamOptions.abortSignal,
      }),
  });
}

export function encodeBase64Url(value: unknown): string {
  const json = JSON.stringify(value);
  if (typeof btoa === "function") {
    return btoa(json).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  }
  return Buffer.from(json).toString("base64url");
}

export function extractAppSyncEvents<T>(
  frame: AppSyncDataFrame,
  parseEvent: (raw: unknown) => T[],
): T[] {
  const rawEvents =
    frame.event !== undefined ? frame.event : frame.events !== undefined ? frame.events : frame.data;
  return asArray(rawEvents).flatMap(parseEvent);
}

export function parseAppSyncEvent(raw: unknown): AppSyncEvent[] {
  if (typeof raw === "string") {
    try {
      return parseAppSyncEvent(JSON.parse(raw));
    } catch {
      return [];
    }
  }
  if (!raw || typeof raw !== "object") return [];
  if (Array.isArray(raw)) return raw.flatMap(parseAppSyncEvent);

  const value = raw as Record<string, unknown>;
  if (typeof value.eventId === "string" && "chunk" in value) {
    return [{ eventId: value.eventId, chunk: value.chunk, createdAt: numberField(value.createdAt) }];
  }
  if (value.event !== undefined) return asArray(value.event).flatMap(parseAppSyncEvent);
  if (value.events !== undefined) return asArray(value.events).flatMap(parseAppSyncEvent);
  if (value.data !== undefined) return asArray(value.data).flatMap(parseAppSyncEvent);
  return [];
}

export function asArray(value: unknown): unknown[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

function numberField(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}

function toRelativeUrl(url: URL, original: string): string {
  return original.startsWith("http://") || original.startsWith("https://")
    ? url.toString()
    : `${url.pathname}${url.search}`;
}
