'use strict';

const { createEmailWorker } = require('./email.worker');
const { createTasksWorker } = require('./tasks.worker');
const { createPdfWorker } = require('./pdf.worker');

// Starts every worker and resolves once they are connected to Redis.
async function startWorkers({ connection, prefix, queues, invoicesDir, publicBaseUrl, log = console.log, options = {} }) {
  const email = createEmailWorker({ connection, prefix, log, ...options.email });
  const tasks = createTasksWorker({ connection, prefix, log, ...options.tasks });
  const pdf = createPdfWorker({ connection, prefix, queues, invoicesDir, publicBaseUrl, log, ...options.pdf });
  const all = [email, tasks, pdf];

  await Promise.all(all.map((worker) => worker.waitUntilReady()));

  // close() waits for jobs that are running right now to finish.
  async function close() {
    await Promise.allSettled(all.map((worker) => worker.close()));
  }

  return { email, tasks, pdf, close };
}

module.exports = { startWorkers };
