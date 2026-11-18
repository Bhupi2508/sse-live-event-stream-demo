// Browser side of the demo. No framework, no build step.
//
// Producer: fetch('POST /api/stream/publish')       -> one short HTTP request per event
// Consumer: new EventSource('/api/stream/events')   -> ONE long-lived HTTP response for all events
//
// Everything shown in the event list comes from the real SSE stream.
// The journey animation is triggered by real moments: the POST being sent,
// the POST response, and the SSE message arriving.

const $ = (id) => document.getElementById(id);
const MAX_RENDERED_EVENTS = 100;
const HOP_MS = 350; // how long a packet takes to travel one link
const AUTO_PUBLISH_MS = 2000;

const state = {
  source: null, // the one EventSource
  clientId: null, // id the server gave this connection
  openedAt: null,
  connectionsOpened: 0, // how many times GET /events succeeded
  received: 0, // events received over SSE
  published: 0, // events this tab published
  lastEventId: null, // remembered so a manual reconnect can catch up
  autoTimer: null,
  producerTrip: null, // promise for a packet still travelling Producer -> Backend
  examples: {}, // event type -> example payload
};

// ---------------------------------------------------------------------------
// Consumer: one EventSource, opened once
// ---------------------------------------------------------------------------
function connect() {
  if (state.source) return; // already connected: never open a second stream

  // Browsers send Last-Event-ID by themselves only on *automatic* reconnects.
  // For a manual reconnect we pass it as a query parameter.
  const url =
    '/api/stream/events' +
    (state.lastEventId ? `?lastEventId=${encodeURIComponent(state.lastEventId)}` : '');
  state.source = new EventSource(url);
  setStatus('connecting', 'Connecting…');
  updateControls();

  state.source.onopen = () => {
    state.connectionsOpened += 1;
    state.openedAt = Date.now();
    $('connections').textContent = state.connectionsOpened;
    setStatus('open', 'Connected');
    setPipe(true);
  };

  // The `event:` field on the wire decides which listener runs.
  state.source.addEventListener('connected', (e) => {
    const info = JSON.parse(e.data);
    state.clientId = info.clientId;
    setPipe(true);
    setMessage(
      `Connection #${info.clientId} is open.` +
        (info.replayed ? ` ${info.replayed} missed event(s) replayed.` : '') +
        ` The server sends a heartbeat every ${info.heartbeatMs / 1000}s.`,
      'ok'
    );
    setCaption(
      `<strong>Connection #${info.clientId} is open.</strong> Now publish events. Every one of them arrives over this same connection.`
    );
  });

  state.source.addEventListener('heartbeat', (e) => {
    $('heartbeat').textContent = formatTime(JSON.parse(e.data).time);
  });

  Object.keys(state.examples).forEach((type) => state.source.addEventListener(type, onAppEvent));

  state.source.onerror = () => {
    setPipe(false);
    state.openedAt = null;
    if (state.source.readyState === EventSource.CONNECTING) {
      // The browser retries on its own and sends Last-Event-ID.
      setStatus('connecting', 'Reconnecting…');
      setMessage('Connection lost. The browser is reconnecting automatically.', 'error');
    } else {
      // CLOSED: the browser gave up (for example, the server returned an error status).
      setStatus('error', 'Error');
      setMessage('Could not connect. Is the server running? Click Connect to try again.', 'error');
      state.source = null;
      updateControls();
    }
  };
}

function disconnect() {
  if (!state.source) return;
  state.source.close(); // ends the HTTP response; the server cleans up
  state.source = null;
  state.openedAt = null;
  setStatus('closed', 'Disconnected');
  setPipe(false);
  setMessage('Disconnected. Events published now reach the server, but this tab misses them until you reconnect.');
  setCaption('<strong>No SSE connection.</strong> Published events stop at the backend for this tab.');
  updateControls();
}

function onAppEvent(e) {
  const event = JSON.parse(e.data);
  state.received += 1;
  state.lastEventId = e.lastEventId;
  $('count').textContent = state.received;
  renderEvent(e, event);
  animateDelivery(event);
}

// ---------------------------------------------------------------------------
// Producer: a plain HTTP POST per event
// ---------------------------------------------------------------------------
async function publish({ useCustom = false } = {}) {
  const body = {};
  const type = $('event-type').value;
  if (type) body.type = type;

  if (useCustom) {
    try {
      body.payload = JSON.parse($('payload').value);
    } catch {
      return showPublishResult('Custom payload is not valid JSON.', 'error');
    }
  }

  // Animate Producer -> Backend while the request is in flight.
  activate('node-producer');
  const trip = travel('link-1', 'POST').then(() => activate('node-backend'));
  state.producerTrip = trip;
  trip.then(() => {
    if (state.producerTrip === trip) state.producerTrip = null;
  });

  try {
    const res = await fetch('/api/stream/publish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const json = await res.json();
    if (!res.ok) throw new Error(json.error || `HTTP ${res.status}`);

    state.published += 1;
    $('published').textContent = state.published;
    $('delivered').textContent = `${json.deliveredTo} client${json.deliveredTo === 1 ? '' : 's'}`;
    showPublishResult(`Published ${json.event.type} #${json.event.id}.`, 'ok');

    if (!state.source) {
      await trip;
      setCaption(
        `Event <strong>#${json.event.id}</strong> reached the backend, but this tab has no SSE connection, so it was not delivered here.`
      );
    }
  } catch (err) {
    showPublishResult(`Publish failed: ${err.message}`, 'error');
    stopAutoPublish();
  }
}

function toggleAutoPublish() {
  if (state.autoTimer) return stopAutoPublish();
  publish();
  state.autoTimer = setInterval(() => publish(), AUTO_PUBLISH_MS);
  $('auto').textContent = 'Stop auto publish';
  $('auto').setAttribute('aria-pressed', 'true');
  $('auto-pill').hidden = false;
}

function stopAutoPublish() {
  clearInterval(state.autoTimer);
  state.autoTimer = null;
  $('auto').textContent = 'Start auto publish (2s)';
  $('auto').setAttribute('aria-pressed', 'false');
  $('auto-pill').hidden = true;
}

// ---------------------------------------------------------------------------
// Journey animation
// ---------------------------------------------------------------------------

// Moves a small labelled packet along one link. Resolves when it arrives.
function travel(linkId, label) {
  const packet = document.createElement('span');
  packet.className = 'packet';
  packet.textContent = label;
  packet.style.animationDuration = `${HOP_MS}ms`;
  $(linkId).append(packet);
  return new Promise((resolve) =>
    setTimeout(() => {
      packet.remove();
      resolve();
    }, HOP_MS)
  );
}

const activeTimers = {};
function activate(nodeId) {
  const node = $(nodeId);
  node.classList.add('active');
  clearTimeout(activeTimers[nodeId]);
  activeTimers[nodeId] = setTimeout(() => node.classList.remove('active'), 700);
}

async function animateDelivery(event) {
  // If this tab published the event, let that packet reach the backend first.
  // Events from other tabs or curl start their journey at the backend.
  if (state.producerTrip) await state.producerTrip;
  else activate('node-backend');

  await travel('link-2', `#${event.id}`);
  activate('node-stream');
  await travel('link-3', `#${event.id}`);
  activate('node-consumer');

  setCaption(
    `Event <strong>#${event.id}</strong> (${event.type}) arrived over connection <strong>#${state.clientId}</strong>. ` +
      `<strong>${state.received}</strong> event${state.received === 1 ? '' : 's'} so far, ` +
      `<strong>${state.connectionsOpened}</strong> connection${state.connectionsOpened === 1 ? '' : 's'} opened.`
  );
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------
function renderEvent(e, event) {
  const li = document.createElement('li');

  const meta = document.createElement('div');
  meta.className = 'event-meta';
  meta.append(
    el('span', 'event-id', `#${e.lastEventId}`),
    el('span', 'badge', e.type),
    Object.assign(el('time', '', formatTime(event.timestamp)), { dateTime: event.timestamp }),
    el('span', '', `via connection #${state.clientId}`)
  );

  const payload = el('pre', '', JSON.stringify(event.payload, null, 2));

  // What actually came over the wire for this message.
  const raw = document.createElement('details');
  raw.append(
    el('summary', '', 'Raw SSE message'),
    el('pre', '', `id: ${e.lastEventId}\nevent: ${e.type}\ndata: ${e.data}\n`)
  );

  li.append(meta, payload, raw);
  const list = $('events');
  list.prepend(li);
  while (list.children.length > MAX_RENDERED_EVENTS) list.lastElementChild.remove();
  $('empty').hidden = true;
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  node.textContent = text; // textContent: payloads can never inject HTML
  return node;
}

function clearEvents() {
  $('events').replaceChildren();
  $('empty').hidden = false;
  state.received = 0;
  $('count').textContent = '0';
}

function setStatus(kind, text) {
  $('status').className = `pill status-${kind}`;
  $('status').textContent = text;
}

function setPipe(open) {
  $('link-3').classList.toggle('open', open);
  $('node-stream').classList.toggle('live', open);
  $('node-consumer').classList.toggle('live', open);
  $('pipe-label').textContent = open
    ? `open connection${state.clientId ? ` #${state.clientId}` : ''}`
    : 'no connection';
}

function setMessage(text, kind = '') {
  $('connection-message').className = `result ${kind}`;
  $('connection-message').textContent = text;
}

// Only ever called with our own fixed strings plus numbers/ids from the server.
function setCaption(html) {
  $('journey-caption').innerHTML = html;
}

function showPublishResult(text, kind) {
  $('publish-result').className = `result ${kind}`;
  $('publish-result').textContent = text;
}

function updateControls() {
  $('connect').disabled = Boolean(state.source);
  $('disconnect').disabled = !state.source;
}

function formatTime(iso) {
  return new Date(iso).toLocaleTimeString();
}

function formatDuration(ms) {
  const s = Math.floor(ms / 1000);
  return s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${s % 60}s`;
}

function fillExample() {
  const type = $('event-type').value || Object.keys(state.examples)[0];
  $('payload').value = JSON.stringify(state.examples[type], null, 2);
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------
async function init() {
  $('connect').addEventListener('click', connect);
  $('disconnect').addEventListener('click', disconnect);
  $('publish').addEventListener('click', () => publish({ useCustom: $('use-custom').checked }));
  $('auto').addEventListener('click', toggleAutoPublish);
  $('clear').addEventListener('click', clearEvents);
  $('event-type').addEventListener('change', fillExample);
  updateControls();

  setInterval(() => {
    $('uptime').textContent = state.openedAt ? formatDuration(Date.now() - state.openedAt) : '–';
  }, 1000);

  try {
    const res = await fetch('/api/stream/types');
    const { types } = await res.json();
    types.forEach(({ type, example }) => {
      state.examples[type] = example;
      $('event-type').append(new Option(type, type));
    });
    fillExample();
  } catch {
    showPublishResult('Could not load event types. Is the server running?', 'error');
    $('connect').disabled = true;
  }
}

init();
