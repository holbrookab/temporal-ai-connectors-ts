# @holbrookab/temporal-ai-connectors

Durable chat streaming connectors for Temporal AI workers and AI SDK UI clients.

The package treats "AI SDK" and "Vercel" as separate concerns:

- AI SDK support is a transport/codec layer that turns durable stream events into
  `UIMessageChunk`s for `useChat`.
- Vercel support is deployment glue: OIDC credentials, environment resolution,
  and route-handler helpers.
- The core durable stream protocol does not depend on either one.

The package is split into a provider-neutral durable stream core and opt-in
adapters for Vercel deployments:

- `@holbrookab/temporal-ai-connectors/core`
- `@holbrookab/temporal-ai-connectors/ai-sdk`
- `@holbrookab/temporal-ai-connectors/adapters/appsync-dynamodb`
- `@holbrookab/temporal-ai-connectors/adapters/redis-dynamodb`
- `@holbrookab/temporal-ai-connectors/vercel`

The initial AI SDK target is `ai@7` / `@ai-sdk/react@4` beta. The runtime model
matches the Go worker connector shape: workers publish live chunks, snapshots,
attempt completions, and tool lifecycle events; frontend clients subscribe
first, replay durable state, and dedupe live/replay races by event id.

```ts
import { createAppSyncTemporalChatTransport } from "@holbrookab/temporal-ai-connectors/adapters/appsync-dynamodb";

const transport = createAppSyncTemporalChatTransport({
  api: "/api/chat",
});
```

Vercel/AWS helpers default to environment variables, but every endpoint, table,
index, namespace, credential provider, and route path can be passed explicitly.
