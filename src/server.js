'use strict';

const { getConfig } = require('./config');
const { parseRedisUrl, createQueues } = require('./jobs/queue');
const { createApp } = require('./app');

async function start() {
  const config = getConfig();
  const connection = parseRedisUrl(config.redisUrl);
  const queues = createQueues({ connection });

  const where = `${connection.host}:${connection.port}`;
  if (config.redisSource === 'local') {
    console.log(`[API] UPSTASH_REDIS_URL is not set - using local Redis at ${where}`);
  }
  const ready = await queues.waitUntilReady(5000);
  console.log(
    ready
      ? `[API] Redis connected (${config.redisSource}: ${where})`
      : `[API] Redis not reachable at ${where} yet - requests that need the queue return 503 until it is`
  );

  const app = createApp({ queues, invoicesDir: config.invoicesDir });
  const server = app.listen(config.port, () => {
    console.log(`[API] Listening on http://localhost:${config.port}`);
    console.log('[API] Start the worker in another terminal: npm run workers (or run both with npm run dev:all)');
  });

  server.on('error', (err) => {
    console.error(err.code === 'EADDRINUSE' ? `[API] Port ${config.port} is already in use` : err);
    process.exit(1);
  });

  const shutdown = (signal) => {
    console.log(`[API] ${signal} received, shutting down...`);
    server.close(async () => {
      await queues.close();
      process.exit(0);
    });
    setTimeout(() => process.exit(1), 5000).unref();
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

if (require.main === module) {
  start().catch((err) => {
    console.error('[API] Failed to start:', err.message);
    process.exit(1);
  });
}

module.exports = { start };
