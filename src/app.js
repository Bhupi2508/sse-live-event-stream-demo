const path = require('node:path');
const express = require('express');
const { Broadcaster } = require('./broadcaster');
const { createStreamRouter } = require('./routes/stream');

// Only needed when the page is served from a different origin than the API
// (e.g. a separate dev server). Set CORS_ORIGIN=http://localhost:5173 to enable.
function cors(allowedOrigin) {
  return (req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', allowedOrigin);
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Last-Event-ID');
    res.setHeader('Vary', 'Origin');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    next();
  };
}

function createApp({ broadcaster = new Broadcaster(), heartbeatMs, maxClients, corsOrigin } = {}) {
  const app = express();
  app.disable('x-powered-by');

  if (corsOrigin) app.use(cors(corsOrigin));
  app.use(express.json({ limit: '10kb' })); // reject huge request bodies early

  app.use('/api/stream', createStreamRouter({ broadcaster, heartbeatMs, maxClients }));
  app.use(express.static(path.join(__dirname, '..', 'public')));

  app.use('/api', (req, res) => res.status(404).json({ error: 'Not found' }));

  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, next) => {
    if (err.type === 'entity.parse.failed') {
      return res.status(400).json({ error: 'Request body is not valid JSON' });
    }
    if (err.type === 'entity.too.large') {
      return res.status(413).json({ error: 'Request body is too large (max 10kb)' });
    }
    console.error(err);
    if (res.headersSent) return res.end();
    res.status(500).json({ error: 'Internal server error' });
  });

  app.locals.broadcaster = broadcaster;
  return app;
}

module.exports = { createApp };
