# nodejs-sse-live-streaming-demo

A small Node.js + Express app for learning **Server-Sent Events (SSE)**. You publish mock events through one endpoint, and every connected browser receives them live through another.

## 1. What is SSE?

Server-Sent Events let a server **push** messages to the browser over a normal HTTP response that stays open. The browser opens the connection with the built-in [`EventSource`](https://developer.mozilla.org/en-US/docs/Web/API/EventSource) API, and the server writes messages to it whenever it has something new.

- **It's plain HTTP.** There's no new protocol or upgrade handshake, so it works through proxies, load balancers and auth middleware that already understand HTTP.
- **It's one-way (server → client).** The response is the only channel. When the client needs to send something, it makes a regular HTTP request. In this demo that's the `POST /publish` call.
- **Reconnection is automatic.** If the connection drops, the browser reconnects on its own and sends the `Last-Event-ID` header so the server can try to catch it up.

## 2. What this project demonstrates

- A **producer** endpoint that publishes events (`POST /api/stream/publish`)
- A **consumer** endpoint that streams them via SSE (`GET /api/stream/events`)
- An in-memory broadcaster (Node `EventEmitter`) that connects the two, so **every** connected client receives **every** event
- SSE fields `id`, `event`, `data` and `retry`, plus heartbeats, disconnect cleanup and `Last-Event-ID` replay
- A browser UI that shows the whole flow live

## 3. Architecture

```
 Browser / curl                 Node.js server                          Browser tab(s)
┌──────────────┐  POST    ┌─────────────────────────┐   text/event-stream  ┌──────────────┐
│   Producer   │ ───────▶ │  Broadcaster            │ ───────────────────▶ │  EventSource │
│  (publish)   │  JSON    │  (EventEmitter + last   │   one open HTTP      │   consumer   │
└──────────────┘          │   50 events in memory)  │   response per tab   └──────────────┘
                          └─────────────────────────┘
```

```
src/
  server.js          starts the HTTP server, graceful shutdown
  app.js             Express app: JSON body limit, optional CORS, error handling
  routes/stream.js   producer + consumer endpoints (the interesting part)
  broadcaster.js     in-memory EventEmitter broadcaster + small replay buffer
  sse.js             formats messages in the text/event-stream format
  mockEvents.js      sample event types and random mock data
public/              index.html, app.js, styles.css (no build step)
test/                end-to-end tests with Node's built-in test runner
```

## 4. Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/api/stream/publish` | **Producer.** Body: `{ "type": "...", "payload": { ... } }`. Both fields are optional. Leave out `payload` to get generated mock data, and leave out `type` to get a random type. Returns `201` with the event and `deliveredTo` (the number of connected clients). |
| `GET` | `/api/stream/events` | **Consumer.** SSE stream. It sends `retry`, then a `connected` event, then any missed events (when `Last-Event-ID` or `?lastEventId=` is given), then live events plus a `heartbeat` every 15s. |
| `GET` | `/api/stream/types` | Event types with an example payload (used by the UI). |
| `GET` | `/api/stream/status` | Number of connected SSE clients and the last event id. |

Event types: `user-created`, `order-created`, `payment-updated`, `notification`.

Validation: an unknown `type` or a `payload` that isn't a JSON object returns `400`, a payload over 4 KB returns `400`, a request body over 10 KB returns `413`, malformed JSON returns `400`, and a non-JSON content type returns `415`. At most 100 SSE clients can connect at once; extra connections get `503`.

## 5. Install and run

Requires Node.js 18.18 or newer.

```bash
npm install
npm start          # http://localhost:3000
npm run dev        # same, restarts on file changes
npm test           # run the tests
```

Optional environment variables: `PORT` (default 3000), `HOST` (default 127.0.0.1), `HEARTBEAT_MS` (default 15000), and `CORS_ORIGIN`. You only need `CORS_ORIGIN` if you serve the UI from a different origin, e.g. `CORS_ORIGIN=http://localhost:5173`.

## 6. Try it in the browser

1. Open http://localhost:3000 and click **Connect**. This opens **one** SSE connection, and the line from *SSE Stream* to *Consumer* turns green.
2. Click **Publish event** a few times, or **Start auto publish** to publish a mock event every 2 seconds. Each event travels *Producer → Backend → SSE Stream → Consumer* and appears with its id, timestamp and JSON payload.
3. Compare the counters: **SSE connections opened** stays at `1` while **Events received** keeps climbing. That's the core idea: one long-lived `GET`, many events. You can confirm it in the DevTools Network tab, where there's a single `events` request with an *EventStream* tab.
4. Click **Disconnect** while auto publish is running. Publishing continues, but this tab stops receiving. Click **Connect** again and the missed events are replayed.
5. Open a **second tab**. Both tabs receive every event, each over its own connection.
6. Stop the server (Ctrl+C) and watch the status change to *Reconnecting…*. Start it again and the browser reconnects by itself.

## 7. Try it with curl

Terminal 1 (consumer). `-N` turns off curl's buffering:

```bash
curl -N http://localhost:3000/api/stream/events
```

Terminal 2 (producer):

```bash
# mock data generated by the server
curl -X POST http://localhost:3000/api/stream/publish \
  -H "Content-Type: application/json" -d '{"type":"order-created"}'

# your own payload
curl -X POST http://localhost:3000/api/stream/publish \
  -H "Content-Type: application/json" \
  -d '{"type":"notification","payload":{"message":"Hello SSE"}}'

# catch up on events after id 3 (what the browser does on reconnect)
curl -N http://localhost:3000/api/stream/events -H "Last-Event-ID: 3"
```

## 8. Example SSE stream

What terminal 1 prints. Each message is a set of `field: value` lines followed by a **blank line**:

```
retry: 3000

event: connected
data: {"clientId":1,"subscribers":1,"replayed":0,"heartbeatMs":15000}

id: 1
event: order-created
data: {"id":"1","type":"order-created","timestamp":"2026-11-18T16:33:56.379Z","payload":{"orderId":"ORD-8501","product":"Headphones","quantity":2,"total":171,"currency":"USD"}}

event: heartbeat
data: {"time":"2026-11-18T16:34:11.380Z"}
```

| Field | Meaning |
| --- | --- |
| `data` | The message body (JSON here). The browser hands it to you as `event.data`. |
| `event` | The event name. Listen for it with `source.addEventListener('order-created', ...)`. Messages without a name go to `onmessage`. |
| `id` | The browser remembers it and sends it back as the `Last-Event-ID` header when it reconnects. |
| `retry` | How many milliseconds the browser waits before reconnecting. |

The response headers that make this work are `Content-Type: text/event-stream` and `Cache-Control: no-cache`. The connection is never closed, so the server can keep writing.

The client side is just this:

```js
const source = new EventSource('/api/stream/events');
source.addEventListener('order-created', (e) => console.log(JSON.parse(e.data)));
source.onerror = () => console.log('lost connection, browser will retry');
```

### Connection lifecycle

1. The browser sends `GET /api/stream/events`, and the server answers `200` with `text/event-stream` headers but never ends the response.
2. The server subscribes this response to the broadcaster and sends `connected`.
3. Every `publish` is written to all subscribed responses. A heartbeat is sent every 15s, which keeps idle proxies from cutting the connection and exposes dead sockets.
4. When the tab closes or the network drops, the server's `close` handler unsubscribes the client and clears its timer, so nothing leaks. Clients that stop reading are dropped once more than 1 MB is buffered for them.
5. If the drop wasn't intentional, the browser waits `retry` ms and reconnects with `Last-Event-ID`.

## 9. SSE vs WebSockets

| | SSE | WebSockets |
| --- | --- | --- |
| Direction | Server → client only | Both directions |
| Protocol | Plain HTTP | Separate `ws://` protocol, after an HTTP upgrade |
| Data | Text (usually JSON) | Text or binary |
| Reconnect | Built into `EventSource` | You write it yourself |
| Resume after a drop | `Last-Event-ID`, built in | You design it yourself |
| Good for | Feeds, notifications, progress, dashboards, LLM token streaming | Chat, games, collaborative editing |

If the server does most of the talking, SSE is usually the simpler choice.

## 10. Limitations of this demo

- **Everything is in memory.** Events and the replay buffer disappear when the server restarts, and event ids start again at 1.
- **Replay is best effort.** Only the last 50 events are kept, so a client that's offline for longer misses events. There's **no guaranteed delivery** and **no exactly-once delivery**.
- **One process only.** If you run several server instances, each has its own broadcaster. Sharing events between them would need something like Redis Pub/Sub, which is deliberately left out here.
- **No authentication.** Anyone who can reach the server can publish and subscribe.
- **Browser connection limits.** Over HTTP/1.1, browsers allow about 6 connections per domain, and each SSE tab uses one. HTTP/2 removes this limit.
