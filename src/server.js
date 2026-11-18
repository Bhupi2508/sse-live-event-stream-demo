const { createApp } = require('./app');

const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || '127.0.0.1';

const app = createApp({
  heartbeatMs: Number(process.env.HEARTBEAT_MS) || 15000,
  corsOrigin: process.env.CORS_ORIGIN,
});

const server = app.listen(PORT, HOST, () => {
  console.log(`SSE demo running at http://localhost:${PORT}`);
});

// SSE connections never end on their own, so close them explicitly on shutdown.
function shutdown() {
  console.log('Shutting down...');
  server.close(() => process.exit(0));
  server.closeAllConnections();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
