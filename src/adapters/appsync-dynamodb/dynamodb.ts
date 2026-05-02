import { PutCommand, QueryCommand, type DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import type {
  DurableReplayResponse,
  DurableStreamAttempt,
  DurableStreamEvent,
  EphemeralChunk,
  StreamLease,
} from "../../core/types";
import { resolveDurableDynamoDBEnv } from "../../vercel/env";

export type DynamoDBReplayStoreOptions = {
  documentClient: DynamoDBDocumentClient;
  tableName?: string;
  partitionKeyName?: string;
  sortKeyName?: string;
  eventsIndexName?: string;
  attemptsIndexName?: string;
  ephemeralIndexName?: string;
};

export type ChatStreamLeaseInput = {
  streamId: string;
  ownerUserId?: string;
  scopeId?: string;
  parentConversationId?: string;
  conversationId?: string;
  channel?: string;
  redisStream?: string;
  replayAttributes?: Record<string, unknown>;
  ttlSeconds?: number;
  createdAt?: number;
  [key: string]: unknown;
};

export class DynamoDBReplayStore {
  private readonly options: Required<Omit<DynamoDBReplayStoreOptions, "documentClient">> & {
    documentClient: DynamoDBDocumentClient;
  };

  constructor(options: DynamoDBReplayStoreOptions) {
    const env = resolveDurableDynamoDBEnv();
    this.options = {
      documentClient: options.documentClient,
      tableName: options.tableName ?? env.tableName,
      partitionKeyName: options.partitionKeyName ?? env.partitionKeyName,
      sortKeyName: options.sortKeyName ?? env.sortKeyName,
      eventsIndexName: options.eventsIndexName ?? env.eventsIndexName,
      attemptsIndexName: options.attemptsIndexName ?? env.attemptsIndexName,
      ephemeralIndexName: options.ephemeralIndexName ?? env.ephemeralIndexName,
    };
  }

  async createStreamLease(input: ChatStreamLeaseInput): Promise<void> {
    const createdAt = input.createdAt ?? Date.now();
    const item = cleanRecord({
      ...input,
      [this.options.partitionKeyName]: input.streamId,
      [this.options.sortKeyName]: 0,
      createdAt: 0,
      updatedAt: createdAt,
      entityType: "CHAT_STREAM",
      streamId: input.streamId,
      expiresAt: input.ttlSeconds ? Math.floor(createdAt / 1000) + input.ttlSeconds : undefined,
    });
    await this.options.documentClient.send(
      new PutCommand({
        TableName: this.options.tableName,
        Item: item,
        ConditionExpression: `attribute_not_exists(#pk)`,
        ExpressionAttributeNames: { "#pk": this.options.partitionKeyName },
      }),
    );
  }

  async getStreamLease(streamId: string): Promise<StreamLease | undefined> {
    const out = await this.options.documentClient.send(
      new QueryCommand({
        TableName: this.options.tableName,
        KeyConditionExpression: "#pk = :id",
        ExpressionAttributeNames: { "#pk": this.options.partitionKeyName },
        ExpressionAttributeValues: { ":id": streamId },
        Limit: 1,
      }),
    );
    const item = out.Items?.[0] as Record<string, unknown> | undefined;
    if (!item) return undefined;
    return {
      streamId,
      channel: stringField(item.channel),
      redisStream: stringField(item.redisStream),
      ownerUserId: stringField(item.ownerUserId),
      scopeId: stringField(item.scopeId),
      parentConversationId: stringField(item.parentConversationId),
      conversationId: stringField(item.conversationId),
      replayAttributes: recordField(item.replayAttributes),
    };
  }

  async fetchReplay(streamId: string, afterEventId = ""): Promise<DurableReplayResponse> {
    const [events, attempts] = await Promise.all([
      this.listEvents(streamId, afterEventId),
      this.listAttempts(streamId),
    ]);
    return { streamId, events, attempts };
  }

  async listEvents(streamId: string, afterEventId?: string | null): Promise<Array<DurableStreamEvent>> {
    return this.queryAll<DurableStreamEvent>({
      IndexName: this.options.eventsIndexName,
      KeyConditionExpression: afterEventId
        ? "durableStreamId = :sid AND durableEventId > :after"
        : "durableStreamId = :sid",
      ExpressionAttributeValues: cleanRecord({
        ":sid": streamId,
        ":after": afterEventId || undefined,
      }),
    });
  }

  async listAttempts(streamId: string): Promise<DurableStreamAttempt[]> {
    return this.queryAll<DurableStreamAttempt>({
      IndexName: this.options.attemptsIndexName,
      KeyConditionExpression: "attemptStreamId = :sid",
      ExpressionAttributeValues: { ":sid": streamId },
    });
  }

  async listEphemeralChunks(
    ephemeralAttemptId: string,
    afterSequence?: number | null,
  ): Promise<Array<DurableStreamEvent>> {
    const chunks = await this.queryAll<EphemeralChunk & { chunk?: unknown; ephemeralSequence?: number }>({
      IndexName: this.options.ephemeralIndexName,
      KeyConditionExpression:
        afterSequence != null
          ? "ephemeralAttemptId = :id AND ephemeralSequence > :after"
          : "ephemeralAttemptId = :id",
      ExpressionAttributeValues: cleanRecord({
        ":id": ephemeralAttemptId,
        ":after": afterSequence ?? undefined,
      }),
    });
    return chunks.map((item) => ({
      eventId: `ephemeral#${ephemeralAttemptId}#${item.ephemeralSequence ?? item.sequence}`,
      chunk: item.chunk ?? item,
    }));
  }

  private async queryAll<T>(input: {
    IndexName: string;
    KeyConditionExpression: string;
    ExpressionAttributeValues: Record<string, unknown>;
  }): Promise<T[]> {
    const items: T[] = [];
    let ExclusiveStartKey: Record<string, unknown> | undefined;
    do {
      const out = await this.options.documentClient.send(
        new QueryCommand({
          TableName: this.options.tableName,
          ScanIndexForward: true,
          ExclusiveStartKey,
          ...input,
        }),
      );
      items.push(...((out.Items ?? []) as T[]));
      ExclusiveStartKey = out.LastEvaluatedKey as Record<string, unknown> | undefined;
    } while (ExclusiveStartKey);
    return items;
  }
}

export function createDynamoDBReplayStore(options: DynamoDBReplayStoreOptions): DynamoDBReplayStore {
  return new DynamoDBReplayStore(options);
}

function cleanRecord<T extends Record<string, unknown>>(record: T): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(record).filter(([, value]) => value !== undefined && value !== ""),
  );
}

function stringField(value: unknown): string | undefined {
  return typeof value === "string" && value !== "" ? value : undefined;
}

function recordField(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}
