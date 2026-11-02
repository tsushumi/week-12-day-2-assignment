'use strict';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Mirrors how BullMQ schedules a retry: exponential => delay * 2^(attemptsMade-1).
function computeBackoffDelay(backoff, attemptsMade) {
  if (!backoff) return 0;
  const options = typeof backoff === 'number' ? { type: 'fixed', delay: backoff } : backoff;
  const delay = options.delay || 0;
  return options.type === 'exponential' ? Math.round(delay * 2 ** (attemptsMade - 1)) : delay;
}

// Describes a failed attempt. In BullMQ's "failed" event, job.attemptsMade
// already counts the attempt that just failed.
function describeFailure(job, err) {
  const attempt = job.attemptsMade;
  const max = job.opts.attempts ?? 1;
  const unrecoverable = err?.name === 'UnrecoverableError';
  const willRetry = !unrecoverable && attempt < max;
  return {
    attempt,
    max,
    willRetry,
    retryDelayMs: willRetry ? computeBackoffDelay(job.opts.backoff, attempt) : 0,
    unrecoverable,
  };
}

// Connection problems must be logged, not thrown: an "error" event with no
// listener would crash the process.
function attachErrorLogging(worker, label, log) {
  let lastAt = 0;
  worker.on('error', (err) => {
    const now = Date.now();
    if (now - lastAt > 10_000) {
      lastAt = now;
      log(`[${label}] Worker error: ${err.code || err.message}`);
    }
  });
  worker.on('stalled', (jobId) => log(`[${label}] Job ${jobId} stalled and will be re-processed`));
}

module.exports = { sleep, computeBackoffDelay, describeFailure, attachErrorLogging };
