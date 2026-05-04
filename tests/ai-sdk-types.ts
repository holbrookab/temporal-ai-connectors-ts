import type { UIMessage } from "ai";
import {
  TemporalDurableChatTransport,
  type DurableUIMessage,
  type DurableWorkflowUIMessage,
} from "../src/ai-sdk";

type AppData = {
  notice: { text: string };
};

type AppMessage = DurableUIMessage<{ traceId?: string }, AppData>;
type WorkflowMessage = DurableWorkflowUIMessage<{ traceId?: string }, AppData>;

const transport = new TemporalDurableChatTransport<AppMessage>({
  streamFactory: () =>
    new ReadableStream({
      start(controller) {
        controller.close();
      },
    }),
});

const _message: UIMessage = {} as AppMessage;
const _workflowMessage: UIMessage = {} as WorkflowMessage;
void transport;
void _message;
void _workflowMessage;
