// In-memory event broadcaster built on Node's EventEmitter.
//
// Producer  -> broadcaster.publish()  -> 'event' is emitted
// Consumers -> broadcaster.subscribe() -> each SSE connection gets a listener
//
// Everything lives in process memory: a server restart loses all events.
const { EventEmitter } = require('node:events');

class Broadcaster {
  constructor({ historySize = 50 } = {}) {
    this.emitter = new EventEmitter();
    // Every SSE client adds one listener. The route caps how many clients
    // can connect, so we turn off EventEmitter's "possible leak" warning here.
    this.emitter.setMaxListeners(0);
    this.historySize = historySize;
    this.history = []; // small ring buffer so reconnecting clients can catch up
    this.nextId = 1;
  }

  publish(type, payload) {
    const event = {
      id: String(this.nextId++),
      type,
      timestamp: new Date().toISOString(),
      payload,
    };

    this.history.push(event);
    if (this.history.length > this.historySize) this.history.shift();

    this.emitter.emit('event', event); // synchronous: every listener runs now
    return event;
  }

  // Returns an unsubscribe function. Callers MUST call it on disconnect.
  subscribe(listener) {
    this.emitter.on('event', listener);
    return () => this.emitter.off('event', listener);
  }

  subscriberCount() {
    return this.emitter.listenerCount('event');
  }

  lastEventId() {
    const last = this.history[this.history.length - 1];
    return last ? last.id : null;
  }

  // Events newer than `lastEventId` that are still in the buffer.
  // Best effort only: older events have been dropped from the buffer.
  eventsSince(lastEventId) {
    const since = Number(lastEventId);
    if (!Number.isInteger(since) || since < 0) return [];
    return this.history.filter((event) => Number(event.id) > since);
  }
}

module.exports = { Broadcaster };
