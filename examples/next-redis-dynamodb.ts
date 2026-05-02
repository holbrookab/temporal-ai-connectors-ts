import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import {
  createDynamoDBReplayStore,
  createRedisSSERouteHandler,
  createRedisTemporalChatTransport,
  createUpstashRedisRestClient,
} from "@holbrookab/temporal-ai-connectors/adapters/redis-dynamodb";
import { getVercelAWSCredentials } from "@holbrookab/temporal-ai-connectors/vercel";

export function createClientTransport() {
  return createRedisTemporalChatTransport({
    api: "/api/chat",
  });
}

export function createReplayGET() {
  const documentClient = DynamoDBDocumentClient.from(
    new DynamoDBClient({
      credentials: getVercelAWSCredentials(),
    }),
  );
  const store = createDynamoDBReplayStore({ documentClient });
  return async (request: Request, context: { params: Promise<{ streamId: string }> }) => {
    const { streamId } = await context.params;
    const after = new URL(request.url).searchParams.get("after") ?? "";
    return Response.json(await store.fetchReplay(streamId, after));
  };
}

export function createSSEGET() {
  return createRedisSSERouteHandler({
    redis: createUpstashRedisRestClient(),
  });
}
