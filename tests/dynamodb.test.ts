import { describe, expect, it } from "vitest";
import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import { createDynamoDBReplayStore } from "../src/adapters/appsync-dynamodb";

describe("DynamoDB replay store", () => {
  it("uses configurable durable event index names", async () => {
    const sent: unknown[] = [];
    const store = createDynamoDBReplayStore({
      documentClient: {
        send: async (command: unknown) => {
          sent.push(command);
          return { Items: [] };
        },
      } as never,
      tableName: "table",
      eventsIndexName: "events-index",
      attemptsIndexName: "attempts-index",
      ephemeralIndexName: "ephemeral-index",
    });

    await store.fetchReplay("stream-1", "01");

    expect(sent[0]).toBeInstanceOf(QueryCommand);
    expect((sent[0] as QueryCommand).input).toMatchObject({
      TableName: "table",
      IndexName: "events-index",
      KeyConditionExpression: "durableStreamId = :sid AND durableEventId > :after",
    });
    expect((sent[1] as QueryCommand).input).toMatchObject({
      IndexName: "attempts-index",
    });
  });
});
