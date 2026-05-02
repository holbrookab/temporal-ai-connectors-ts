import type { DynamoDBReplayStore } from "./dynamodb";
import { resolveAppSyncEnv } from "../../vercel/env";

export type StreamAuthorizer = (
  request: Request,
  streamId: string,
  lease: Awaited<ReturnType<DynamoDBReplayStore["getStreamLease"]>>,
) => boolean | Promise<boolean>;

export function createDynamoDBReplayRouteHandler(options: {
  store: DynamoDBReplayStore;
  authorize?: StreamAuthorizer;
}): (request: Request, context: { params: { streamId: string } | Promise<{ streamId: string }> }) => Promise<Response> {
  return async (request, context) => {
    const { streamId } = await context.params;
    const lease = await options.store.getStreamLease(streamId);
    if (!lease) return json({ error: "Stream not found" }, 404);
    if (options.authorize && !(await options.authorize(request, streamId, lease))) {
      return json({ error: "Stream not found" }, 404);
    }
    const url = new URL(request.url);
    const after = url.searchParams.get("after") ?? "";
    return json(await options.store.fetchReplay(streamId, after));
  };
}

export function createAppSyncAuthRouteHandler(options: {
  getToken(request: Request): string | Promise<string | undefined> | undefined;
  httpDomain?: string;
  realtimeDomain?: string;
  namespace?: string;
}): (request: Request) => Promise<Response> {
  return async (request) => {
    const token = await options.getToken(request);
    if (!token) return json({ error: "Not authenticated" }, 401);
    const env = resolveAppSyncEnv();
    return json({
      token,
      httpDomain: options.httpDomain ?? env.httpDomain,
      realtimeDomain: options.realtimeDomain ?? env.realtimeDomain,
      namespace: options.namespace ?? env.namespace,
    });
  };
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}
