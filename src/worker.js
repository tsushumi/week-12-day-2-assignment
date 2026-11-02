'use strict';

/**
 * Worker process. Runs separately from the API (npm run workers) and handles
 * the "email", "tasks" and "pdf" queues.
 */

const fs = require('node:fs');
const { getConfig } = require('./config');
const { parseRedisUrl, createQueues } = require('./jobs/queue');
const { startWorkers } = require('./workers');

async function main() {
  const config = getConfig();
  const connection = parseRedisUrl(config.redisUrl);
  fs.mkdirSync(config.invoicesDir, { recursive: true });

  // The PDF worker also needs to add notification jobs to the email queue.
  const queues = createQueues({ connection });

  const where = `${connection.host}:${connection.port}`;
  console.log(
    config.redisSource === 'local'
      ? `[Worker] UPSTASH_REDIS_URL is not set - using local Redis at ${where}`
      : `[Worker] Connecting to Redis at ${where}...`
  );

  const workers = await startWorkers({
    connection,
    queues,
    invoicesDir: config.invoicesDir,
    publicBaseUrl: config.publicBaseUrl,
  });
  console.log('[Worker] Ready - waiting for jobs on queues: email, tasks, pdf');

  let shuttingDown = false;
  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[Worker] ${signal} received, finishing running jobs...`);
    const force = setTimeout(() => process.exit(1), 10_000);
    force.unref();
    await workers.close();
    await queues.close();
    console.log('[Worker] Stopped');
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('[Worker] Failed to start:', err.message);
  process.exit(1);
});
