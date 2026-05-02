import { describe, expect, it } from "vitest";
import { encodeBase64Url, extractAppSyncEvents, parseAppSyncEvent } from "../src/adapters/appsync-dynamodb";

describe("AppSync adapter", () => {
  it("parses nested AppSync event frame shapes", () => {
    const events = extractAppSyncEvents(
      {
        type: "data",
        event: JSON.stringify({
          events: [
            { eventId: "01", chunk: { type: "text-start" } },
            { data: { eventId: "02", chunk: { type: "text-delta" } } },
          ],
        }),
      },
      parseAppSyncEvent,
    );

    expect(events).toEqual([
      { eventId: "01", chunk: { type: "text-start" }, createdAt: undefined },
      { eventId: "02", chunk: { type: "text-delta" }, createdAt: undefined },
    ]);
  });

  it("encodes AppSync realtime headers as base64url", () => {
    expect(encodeBase64Url({ Authorization: "token", host: "example.com" })).not.toContain("=");
  });
});
