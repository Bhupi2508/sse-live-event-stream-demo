// Mock data so you can publish events without writing any JSON.

const EVENT_TYPES = ['user-created', 'order-created', 'payment-updated', 'notification'];

const pick = (list) => list[Math.floor(Math.random() * list.length)];
const randomInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

const NAMES = ['Asha', 'Ravi', 'Meera', 'Arjun', 'Sara', 'Kabir'];
const PRODUCTS = ['Keyboard', 'Monitor', 'Headphones', 'Webcam', 'Desk Lamp'];

const generators = {
  'user-created': () => {
    const name = pick(NAMES);
    return {
      userId: `USR-${randomInt(1000, 9999)}`,
      name,
      email: `${name.toLowerCase()}@example.com`,
    };
  },
  'order-created': () => ({
    orderId: `ORD-${randomInt(1000, 9999)}`,
    product: pick(PRODUCTS),
    quantity: randomInt(1, 3),
    total: randomInt(20, 500),
    currency: 'USD',
  }),
  'payment-updated': () => ({
    paymentId: `PAY-${randomInt(1000, 9999)}`,
    orderId: `ORD-${randomInt(1000, 9999)}`,
    status: pick(['pending', 'succeeded', 'failed', 'refunded']),
  }),
  notification: () => ({
    level: pick(['info', 'warning', 'success']),
    message: pick([
      'Nightly backup finished',
      'New comment on your post',
      'Disk usage is above 80%',
      'Weekly report is ready',
    ]),
  }),
};

function generateMockPayload(type) {
  return generators[type]();
}

function randomEventType() {
  return pick(EVENT_TYPES);
}

module.exports = { EVENT_TYPES, generateMockPayload, randomEventType };
