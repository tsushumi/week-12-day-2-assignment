'use strict';

const { Worker } = require('bullmq');
const { QUEUE_NAMES } = require('../jobs/queue');
const { sleep, describeFailure, attachErrorLogging } = require('./common');

const LABEL = 'Task Worker';

/**
 * Job types (job.name):
 *   "flaky"        fails ~50% of the time
 *   "always-fail"  always throws (exhausts every retry)
 *   anything else  succeeds after a short delay
 */
function createTaskProcessor({ log = console.log, random = Math.random, workMs = 200, onProcess } = {}) {
  return async function processTask(job) {
    const attempt = job.attemptsMade + 1; // attemptsMade counts finished attempts
    const max = job.opts.attempts ?? 1;
    const priority = job.opts.priority ? `, priority ${job.opts.priority}` : '';
    log(`[${LABEL}] Job ${job.id}: processing (${job.name}, attempt ${attempt}/${max}${priority})`);
    if (onProcess) onProcess(job);

    if (job.name === 'always-fail') throw new Error('Simulated permanent failure (always-fail job)');
    if (job.name === 'flaky' && random() < 0.5) throw new Error('Simulated random failure (flaky job)');

    await sleep(workMs);
    return { type: job.name, handledAt: new Date().toISOString() };
  };
}

function createTasksWorker({ connection, prefix, log = console.log, random, workMs, onProcess } = {}) {
  // jobId -> transitions seen by this worker, printed as one line when the job ends.
  const timelines = new Map();
  const timelineOf = (jobId) => {
    if (!timelines.has(jobId)) timelines.set(jobId, ['queued']);
    return timelines.get(jobId);
  };
  const finish = (job) => {
    log(`[${LABEL}] Job ${job.id}: ${timelineOf(job.id).join(' -> ')}`);
    timelines.delete(job.id);
  };

  const processor = createTaskProcessor({
    log,
    random,
    workMs,
    onProcess: (job) => {
      timelineOf(job.id).push('processing');
      if (onProcess) onProcess(job);
    },
  });

  // Sequential on purpose: one job at a time, in priority order.
  const worker = new Worker(QUEUE_NAMES.tasks, processor, {
    connection,
    concurrency: 1,
    ...(prefix ? { prefix } : {}),
  });

  worker.on('completed', (job) => {
    log(`[${LABEL}] Job ${job.id}: completed`);
    timelineOf(job.id).push('completed');
    finish(job);
  });

  worker.on('failed', (job, err) => {
    if (!job) return log(`[${LABEL}] A job failed: ${err.message}`);
    const f = describeFailure(job, err);
    if (f.willRetry) {
      const step = `failed (attempt ${f.attempt}/${f.max}, retrying in ${f.retryDelayMs}ms)`;
      log(`[${LABEL}] Job ${job.id}: ${step} - ${err.message}`);
      timelineOf(job.id).push(step);
    } else {
      log(`[${LABEL}] Job ${job.id}: failed permanently after ${f.attempt}/${f.max} attempts - ${err.message}`);
      timelineOf(job.id).push(`failed (attempt ${f.attempt}/${f.max})`, 'failed permanently');
      finish(job);
    }
  });

  attachErrorLogging(worker, LABEL, log);
  return worker;
}

module.exports = { createTasksWorker, createTaskProcessor };
