// /api/stream routes
//
//   POST /api/stream/publish  -> producer: put an event into the broadcaster
//   GET  /api/stream/events   -> consumer: SSE connection that receives events
//   GET  /api/stream/types    -> event types + example payloads (used by the UI)
//   GET  /api/stream/status   -> how many SSE clients are connected right now
const express = require('express');
const { EVENT_TYPES, generateMockPayload, randomEventType } = require('../mockEvents');
const { formatSseMessage, formatBroadcastEvent } = require('../sse');

const MAX_PAYLOAD_BYTES = 4 * 1024; // size limit for a custom payload
const MAX_BUFFERED_BYTES = 1024 * 1024; // drop clients that stop reading
const RETRY_MS = 3000; // tells the browser how long to wait before reconnecting

function validatePublishBody(body) {
  if (body === undefined) body = {}; // empty request = random mock event
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    return { error: 'Body must be a JSON object like {"type": "order-created"}' };
  }

  const type = body.type === undefined ? randomEventType() : body.type;
  if (!EVENT_TYPES.includes(type)) {
    return { error: `"type" must be one of: ${EVENT_TYPES.join(', ')}` };
  }

  if (body.payload === undefined) {
    return { type, payload: generateMockPayload(type) };
  }

  const { payload } = body;
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    return { error: '"payload" must be a JSON object' };
  }
  if (Buffer.byteLength(JSON.stringify(payload)) > MAX_PAYLOAD_BYTES) {
    return { error: `"payload" must be at most ${MAX_PAYLOAD_BYTES} bytes` };
  }
  return { type, payload };
}

function createStreamRouter({ broadcaster, heartbeatMs = 15000, maxClients = 100 }) {
  const router = express.Router();
  let clientCounter = 0;

  router.get('/types', (req, res) => {
    res.json({
      types: EVENT_TYPES.map((type) => ({ type, example: generateMockPayload(type) })),
    });
  });

  router.get('/status', (req, res) => {
    res.json({
      subscribers: broadcaster.subscriberCount(),
      lastEventId: broadcaster.lastEventId(),
    });
  });

  // ---------- Producer ----------
  router.post('/publish', (req, res) => {
    if (req.get('content-length') > 0 && !req.is('application/json')) {
      return res.status(415).json({ error: 'Content-Type must be application/json' });
    }

    const result = validatePublishBody(req.body);
    if (result.error) return res.status(400).json({ error: result.error });

    const event = broadcaster.publish(result.type, result.payload);
    res.status(201).json({ event, deliveredTo: broadcaster.subscriberCount() });
  });

  // ---------- Consumer (SSE) ----------
  router.get('/events', (req, res) => {
    if (broadcaster.subscriberCount() >= maxClients) {
      return res.status(503).json({ error: 'Too many SSE clients, try again later' });
    }

    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform', // never cache a live stream
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no', // ask proxies like nginx not to buffer
    });

    const clientId = ++clientCounter;

    // Writes to the socket. If a client stops reading, data piles up in
    // memory, so we disconnect it once too much is buffered.
    const send = (chunk) => {
      if (res.writableEnded || res.destroyed) return;
      res.write(chunk);
      if (res.writableLength > MAX_BUFFERED_BYTES) res.destroy();
    };

    // 1. How long the browser should wait before auto-reconnecting.
    send(`retry: ${RETRY_MS}\n\n`);

    // 2. Catch up on missed events. Browsers send the Last-Event-ID header
    //    automatically when they reconnect; ?lastEventId= covers manual reconnects.
    const lastEventId = req.get('Last-Event-ID') ?? req.query.lastEventId;
    const missed = lastEventId ? broadcaster.eventsSince(lastEventId) : [];

    // 3. Subscribe to live events. Everything here runs synchronously, so no
    //    event can slip in between the replay and the subscription.
    const unsubscribe = broadcaster.subscribe((event) => send(formatBroadcastEvent(event)));

    send(
      formatSseMessage({
        event: 'connected',
        data: {
          clientId,
          subscribers: broadcaster.subscriberCount(),
          replayed: missed.length,
          heartbeatMs,
        },
      })
    );
    missed.forEach((event) => send(formatBroadcastEvent(event)));

    // 4. Heartbeat keeps proxies from closing an idle connection and lets the
    //    server notice dead clients (a write to a dead socket fails -> 'close').
    const heartbeat = setInterval(() => {
      send(formatSseMessage({ event: 'heartbeat', data: { time: new Date().toISOString() } }));
    }, heartbeatMs);

    console.log(`[sse] client ${clientId} connected (${broadcaster.subscriberCount()} total)`);

    // 5. Cleanup. Without this every closed tab would leak a listener + timer.
    res.on('close', () => {
      clearInterval(heartbeat);
      unsubscribe();
      console.log(`[sse] client ${clientId} disconnected (${broadcaster.subscriberCount()} total)`);
    });
  });

  return router;
}

module.exports = { createStreamRouter, validatePublishBody, MAX_PAYLOAD_BYTES };
