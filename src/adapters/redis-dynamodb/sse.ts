import type { DurableStreamEvent } from "../../core/types";
import type { RedisLiveClient } from "./redis";
import { redisChannel, redisStreamKey } from "./redis";

export function createRedisSSERouteHandler(options: {
  redis: RedisLiveClient;
  authorize?: (request: Request, streamId: string) => boolean | Promise<boolean>;
  channelPrefix?: string;
  streamPrefix?: string;
  pollIntervalMs?: number;
}): (request: Request, context: { params: { streamId: string } | Promise<{ streamId: string }> }) => Promise<Response> {
  return async (request, context) => {
    const { streamId } = await context.params;
    if (options.authorize && !(await options.authorize(request, streamId))) {
      return new Response("Not found", { status: 404 });
    }
    const channel = redisChannel(streamId, options.channelPrefix);
    const stream = redisStreamKey(streamId, options.streamPrefix);
    const body = createRedisSSEBody({
      redis: options.redis,
      channel,
      stream,
      abortSignal: request.signal,
      pollIntervalMs: options.pollIntervalMs,
    });
    return new Response(body, {
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache, no-transform",
        connection: "keep-alive",
      },
    });
  };
}

export function createRedisSSEBody(options: {
  redis: RedisLiveClient;
  channel: string;
  stream: string;
  abortSignal?: AbortSignal;
  pollIntervalMs?: number;
}): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      let subscription: { close(): void } | undefined;
      const send = (event: DurableStreamEvent) => {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
      };
      const close = () => {
        subscription?.close();
        try {
          controller.close();
        } catch {
          // noop
        }
      };
      options.abortSignal?.addEventListener("abort", close, { once: true });

      if (options.redis.subscribe) {
        subscription = await options.redis.subscribe(options.channel, (payload) => {
          send(JSON.parse(payload) as DurableStreamEvent);
        });
        return;
      }

      if (!options.redis.xread) {
        throw new Error("Redis client must support subscribe or xread for SSE delivery");
      }

      let afterId = "$";
      while (!options.abortSignal?.aborted) {
        const entries = await options.redis.xread(options.stream, afterId, { count: 100 });
        for (const entry of entries) {
          afterId = entry.id;
          const payload = entry.values.payload;
          if (payload) send(JSON.parse(payload) as DurableStreamEvent);
        }
        await delay(options.pollIntervalMs ?? 1000, options.abortSignal);
      }
    },
  });
}

function delay(ms: number, abortSignal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (abortSignal?.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, ms);
    abortSignal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}
