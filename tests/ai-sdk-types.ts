import type { UIMessage } from "ai";
import {
  TemporalDurableChatTransport,
  type DurableUIMessage,
} from "../src/ai-sdk";

type AppData = {
  notice: { text: string };
};

type AppMessage = DurableUIMessage<{ traceId?: string }, AppData>;

const transport = new TemporalDurableChatTransport<AppMessage>({
  streamFactory: () =>
    new ReadableStream({
      start(controller) {
        controller.close();
      },
    }),
});

const _message: UIMessage = {} as AppMessage;
void transport;
void _message;
