import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  createAppSyncAuthRouteHandler,
  createAppSyncTemporalChatTransport,
  createDynamoDBReplayRouteHandler,
  createDynamoDBReplayStore,
} from "@holbrookab/temporal-ai-connectors/adapters/appsync-dynamodb";
import { getVercelAWSCredentials } from "@holbrookab/temporal-ai-connectors/vercel";

export function createClientTransport() {
  return createAppSyncTemporalChatTransport({
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
  return createDynamoDBReplayRouteHandler({
    store,
    authorize: (_request, _streamId, lease) => Boolean(lease?.ownerUserId),
  });
}

export function createAuthPOST(getToken: (request: Request) => Promise<string | undefined>) {
  return createAppSyncAuthRouteHandler({ getToken });
}
