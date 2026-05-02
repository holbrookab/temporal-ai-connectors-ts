export type VercelAWSConfig = {
  region?: string;
  roleArn?: string;
};

export type DurableDynamoDBEnv = {
  tableName: string;
  partitionKeyName: string;
  sortKeyName: string;
  eventsIndexName: string;
  attemptsIndexName: string;
  ephemeralIndexName: string;
};

export type AppSyncEnv = {
  httpDomain?: string;
  realtimeDomain?: string;
  namespace: string;
  authEndpoint: string;
  replayEndpointPrefix: string;
};

export type RedisEnv = {
  url?: string;
  token?: string;
  channelPrefix: string;
  streamPrefix: string;
  sseEndpointPrefix: string;
};

export function resolveVercelAWSConfig(env: Record<string, string | undefined> = process.env): VercelAWSConfig {
  return {
    region: env.AWS_REGION ?? env.AWS_DEFAULT_REGION,
    roleArn: env.AWS_ROLE_ARN,
  };
}

export function resolveDurableDynamoDBEnv(
  env: Record<string, string | undefined> = process.env,
): DurableDynamoDBEnv {
  return {
    tableName: env.CHAT_TABLE_NAME ?? env.DURABLE_STREAM_TABLE_NAME ?? "chat-production",
    partitionKeyName: env.DURABLE_STREAM_PARTITION_KEY ?? "id",
    sortKeyName: env.DURABLE_STREAM_SORT_KEY ?? "createdAt",
    eventsIndexName: env.DURABLE_STREAM_EVENTS_INDEX ?? "durableStreamId-durableEventId-index",
    attemptsIndexName: env.DURABLE_STREAM_ATTEMPTS_INDEX ?? "attemptStreamId-attemptUpdatedAt-index",
    ephemeralIndexName: env.DURABLE_STREAM_EPHEMERAL_INDEX ?? "ephemeralAttemptId-ephemeralSequence-index",
  };
}

export function resolveAppSyncEnv(env: Record<string, string | undefined> = process.env): AppSyncEnv {
  return {
    httpDomain: env.NEXT_PUBLIC_APPSYNC_EVENTS_HTTP_DOMAIN ?? env.APPSYNC_EVENTS_HTTP_DOMAIN,
    realtimeDomain:
      env.NEXT_PUBLIC_APPSYNC_EVENTS_REALTIME_DOMAIN ?? env.APPSYNC_EVENTS_REALTIME_DOMAIN,
    namespace: env.NEXT_PUBLIC_APPSYNC_EVENTS_CHANNEL_NAMESPACE ?? env.APPSYNC_EVENTS_CHANNEL_NAMESPACE ?? "chat",
    authEndpoint: env.DURABLE_STREAM_APPSYNC_AUTH_ENDPOINT ?? "/api/chat-streams/auth",
    replayEndpointPrefix: env.DURABLE_STREAM_REPLAY_ENDPOINT_PREFIX ?? "/api/chat-streams",
  };
}

export function resolveRedisEnv(env: Record<string, string | undefined> = process.env): RedisEnv {
  return {
    url: env.KV_REST_API_URL ?? env.UPSTASH_REDIS_REST_URL ?? env.REDIS_REST_URL,
    token: env.KV_REST_API_TOKEN ?? env.UPSTASH_REDIS_REST_TOKEN ?? env.REDIS_REST_TOKEN,
    channelPrefix: env.DURABLE_STREAM_REDIS_CHANNEL_PREFIX ?? "temporal-ai:live:",
    streamPrefix: env.DURABLE_STREAM_REDIS_STREAM_PREFIX ?? "temporal-ai:stream:",
    sseEndpointPrefix: env.DURABLE_STREAM_REDIS_SSE_ENDPOINT_PREFIX ?? "/api/chat-streams",
  };
}
