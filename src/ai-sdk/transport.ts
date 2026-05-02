import type { ChatTransport, UIMessage, UIMessageChunk } from "ai";
import { createUIMessageChunkStreamFromDurableEvents } from "./chunks";
import type {
  DurableChatAck,
  TemporalDurableChatTransportOptions,
} from "./types";

export class TemporalDurableChatTransport<
  UI_MESSAGE extends UIMessage,
  TAck extends DurableChatAck = DurableChatAck,
> implements ChatTransport<UI_MESSAGE> {
  private readonly api: string;
  private readonly fetchImpl: typeof fetch;
  private readonly options: TemporalDurableChatTransportOptions<TAck>;

  constructor(options: TemporalDurableChatTransportOptions<TAck>) {
    this.api = options.api ?? "/api/chat";
    this.fetchImpl = options.fetch ?? fetch;
    this.options = options;
  }

  async sendMessages(
    options: Parameters<ChatTransport<UI_MESSAGE>["sendMessages"]>[0],
  ): Promise<ReadableStream<UIMessageChunk>> {
    const { abortSignal } = options;
    const response = await this.fetchImpl(this.api, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        ...(await resolveValue(this.options.headers)),
      },
      credentials: this.options.credentials,
      body: JSON.stringify(await this.createSendBody(options)),
      signal: abortSignal,
    });

    if (!response.ok) {
      throw new Error(`Chat request failed: ${response.status} ${response.statusText}`);
    }

    const ack = (await response.json()) as TAck;
    if (!ack.streamId) {
      throw new Error("Chat request response did not include streamId");
    }
    this.options.onSendAck?.(ack);

    return createUIMessageChunkStreamFromDurableEvents(
      this.options.streamFactory(ack, { abortSignal }),
      {
        isToolWrite: this.options.isToolWrite,
        notifyToolWrite: this.options.notifyToolWrite,
      },
    );
  }

  async reconnectToStream(
    options: Parameters<ChatTransport<UI_MESSAGE>["reconnectToStream"]>[0],
  ): Promise<ReadableStream<UIMessageChunk> | null> {
    if (!this.options.reconnectStreamFactory) return null;
    const stream = await this.options.reconnectStreamFactory(options);
    if (!stream) return null;
    return createUIMessageChunkStreamFromDurableEvents(stream, {
      isToolWrite: this.options.isToolWrite,
      notifyToolWrite: this.options.notifyToolWrite,
    });
  }

  private async createSendBody(
    options: Parameters<ChatTransport<UI_MESSAGE>["sendMessages"]>[0],
  ): Promise<unknown> {
    if (this.options.prepareSendBody) {
      return this.options.prepareSendBody(options as Parameters<ChatTransport<UIMessage>["sendMessages"]>[0]);
    }
    const extraBody = await resolveValue(this.options.body);
    return {
      messages: options.messages,
      ...extraBody,
    };
  }
}

async function resolveValue<T>(
  value: T | (() => T | Promise<T>) | undefined,
): Promise<T | undefined> {
  return typeof value === "function" ? (value as () => T | Promise<T>)() : value;
}
