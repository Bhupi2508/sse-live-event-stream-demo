// Run with: npm test   (uses Node's built-in test runner, no extra deps)
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { createApp } = require('../src/app');
const { formatSseMessage } = require('../src/sse');

let server;
let baseUrl;

before(async () => {
  const app = createApp({ heartbeatMs: 200, maxClients: 5 });
  server = app.listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

after(() => {
  server.closeAllConnections();
  server.close();
});

// ---------- helpers ----------

// Minimal SSE client on top of fetch, so tests see the real wire format.
async function openStream(path = '/api/stream/events', headers = {}) {
  const controller = new AbortController();
  const res = await fetch(baseUrl + path, { headers, signal: controller.signal });
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  const queue = [];
  let buffer = '';

  async function next(timeoutMs = 2000) {
    const deadline = Date.now() + timeoutMs;
    while (queue.length === 0) {
      if (Date.now() > deadline) throw new Error('Timed out waiting for SSE message');
      const { value, done } = await reader.read();
      if (done) throw new Error('Stream ended');
      buffer += decoder.decode(value, { stream: true });
      let index;
      while ((index = buffer.indexOf('\n\n')) !== -1) {
        const raw = buffer.slice(0, index);
        buffer = buffer.slice(index + 2);
        const msg = {};
        for (const line of raw.split('\n')) {
          const [field, ...rest] = line.split(': ');
          msg[field] = rest.join(': ');
        }
        if (msg.data !== undefined) msg.data = JSON.parse(msg.data);
        queue.push(msg);
      }
    }
    return queue.shift();
  }

  // Skip heartbeats / retry hints until a message matching `predicate` arrives.
  async function nextWhere(predicate) {
    for (;;) {
      const msg = await next();
      if (predicate(msg)) return msg;
    }
  }

  return { res, next, nextWhere, close: () => controller.abort() };
}

const publish = (body, headers = { 'Content-Type': 'application/json' }) =>
  fetch(`${baseUrl}/api/stream/publish`, {
    method: 'POST',
    headers,
    body: typeof body === 'string' ? body : JSON.stringify(body),
  });

const subscriberCount = async () =>
  (await (await fetch(`${baseUrl}/api/stream/status`)).json()).subscribers;

async function waitFor(check, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error('Condition not met in time');
    await new Promise((r) => setTimeout(r, 20));
  }
}

// ---------- tests ----------

test('SSE endpoint sends correct headers, retry hint and a connected event', async () => {
  const stream = await openStream();
  assert.equal(stream.res.status, 200);
  assert.match(stream.res.headers.get('content-type'), /^text\/event-stream/);
  assert.match(stream.res.headers.get('cache-control'), /no-cache/);

  assert.equal((await stream.next()).retry, '3000');
  const connected = await stream.next();
  assert.equal(connected.event, 'connected');
  assert.equal(typeof connected.data.clientId, 'number');
  stream.close();
});

test('producer publishes a mock event when no payload is given', async () => {
  const res = await publish({ type: 'order-created' });
  assert.equal(res.status, 201);
  const body = await res.json();
  assert.equal(body.event.type, 'order-created');
  assert.ok(body.event.id);
  assert.ok(body.event.payload.orderId);
  assert.ok(body.event.timestamp);
});

test('an empty POST publishes a random event type', async () => {
  const res = await fetch(`${baseUrl}/api/stream/publish`, { method: 'POST' });
  assert.equal(res.status, 201);
});

test('a connected client receives the published event with id/event/data', async () => {
  const stream = await openStream();
  await stream.nextWhere((m) => m.event === 'connected');

  const { event } = await (await publish({ type: 'notification', payload: { message: 'hi' } })).json();
  const msg = await stream.nextWhere((m) => m.id === event.id);

  assert.equal(msg.event, 'notification');
  assert.deepEqual(msg.data.payload, { message: 'hi' });
  stream.close();
});

test('multiple consumers receive the same event', async () => {
  const a = await openStream();
  const b = await openStream();
  await a.nextWhere((m) => m.event === 'connected');
  await b.nextWhere((m) => m.event === 'connected');

  const body = await (await publish({ type: 'user-created' })).json();
  assert.ok(body.deliveredTo >= 2);

  const [msgA, msgB] = await Promise.all([
    a.nextWhere((m) => m.id === body.event.id),
    b.nextWhere((m) => m.id === body.event.id),
  ]);
  assert.deepEqual(msgA.data, msgB.data);
  a.close();
  b.close();
});

test('disconnected consumers are cleaned up', async () => {
  await waitFor(async () => (await subscriberCount()) === 0);
  const stream = await openStream();
  await stream.nextWhere((m) => m.event === 'connected');
  assert.equal(await subscriberCount(), 1);

  stream.close();
  await waitFor(async () => (await subscriberCount()) === 0);
});

test('heartbeats are sent periodically', async () => {
  const stream = await openStream();
  const hb = await stream.nextWhere((m) => m.event === 'heartbeat');
  assert.ok(hb.data.time);
  stream.close();
});

test('reconnecting with Last-Event-ID replays missed events', async () => {
  const first = await (await publish({ type: 'notification' })).json();
  const missed1 = await (await publish({ type: 'order-created' })).json();
  const missed2 = await (await publish({ type: 'payment-updated' })).json();

  const stream = await openStream('/api/stream/events', { 'Last-Event-ID': first.event.id });
  const connected = await stream.nextWhere((m) => m.event === 'connected');
  assert.equal(connected.data.replayed, 2);
  assert.equal((await stream.next()).id, missed1.event.id);
  assert.equal((await stream.next()).id, missed2.event.id);
  stream.close();
});

test('?lastEventId= query parameter also replays (manual reconnect)', async () => {
  const first = await (await publish({ type: 'notification' })).json();
  const missed = await (await publish({ type: 'notification' })).json();

  const stream = await openStream(`/api/stream/events?lastEventId=${first.event.id}`);
  await stream.nextWhere((m) => m.event === 'connected');
  assert.equal((await stream.next()).id, missed.event.id);
  stream.close();
});

test('too many SSE clients get 503', async () => {
  await waitFor(async () => (await subscriberCount()) === 0);
  const streams = [];
  for (let i = 0; i < 5; i++) streams.push(await openStream());
  const res = await fetch(`${baseUrl}/api/stream/events`);
  assert.equal(res.status, 503);
  streams.forEach((s) => s.close());
});

test('invalid payloads are rejected', async (t) => {
  const cases = [
    ['unknown type', { type: 'hack' }, 400],
    ['non-string type', { type: 42 }, 400],
    ['array body', [1, 2], 400],
    ['array payload', { type: 'notification', payload: [1] }, 400],
    ['null payload', { type: 'notification', payload: null }, 400],
    ['string payload', { type: 'notification', payload: 'x' }, 400],
    ['payload over 4 KB', { type: 'notification', payload: { big: 'x'.repeat(5000) } }, 400],
    ['body over 10 KB', { type: 'notification', payload: { big: 'x'.repeat(20000) } }, 413],
    ['malformed JSON', '{"type": ', 400],
  ];
  for (const [name, body, status] of cases) {
    await t.test(name, async () => {
      const res = await publish(body);
      assert.equal(res.status, status);
      assert.ok((await res.json()).error);
    });
  }

  await t.test('wrong content type', async () => {
    const res = await publish('type=notification', { 'Content-Type': 'text/plain' });
    assert.equal(res.status, 415);
  });
});

test('payloads with newlines cannot break the SSE framing', () => {
  const wire = formatSseMessage({ id: '1', event: 'notification', data: { msg: 'a\n\ndata: injected' } });
  assert.equal(wire.split('\n\n').length, 2, 'exactly one message terminator');
  assert.equal(wire.match(/^data: /gm).length, 1, 'exactly one data line');
});

test('unknown API routes return JSON 404', async () => {
  const res = await fetch(`${baseUrl}/api/nope`);
  assert.equal(res.status, 404);
});
