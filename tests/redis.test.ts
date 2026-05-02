import { describe, expect, it } from "vitest";
import { readSSE } from "../src/adapters/redis-dynamodb";

describe("Redis SSE adapter", () => {
  it("parses durable events from SSE frames", async () => {
    const encoder = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          encoder.encode('data: {"eventId":"01","chunk":{"type":"text-delta"}}\n\n'),
        );
        controller.close();
      },
    });
    const events: unknown[] = [];
    await readSSE(body, (event) => events.push(event));
    expect(events).toEqual([{ eventId: "01", chunk: { type: "text-delta" } }]);
  });
});
