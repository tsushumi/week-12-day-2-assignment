'use strict';

/**
 * Queue setup shared by the API process (producer) and the worker process.
 *
 * Queues: "email", "tasks", "pdf"  (all on the same Redis, Upstash in production)
 */

const { Queue } = require('bullmq');

const QUEUE_NAMES = Object.freeze({ email: 'email', tasks: 'tasks', pdf: 'pdf' });

// BullMQ: lower number = higher priority. A job with NO priority is treated as
// the highest priority of all and is processed before every prioritized job, so
// jobs that don't ask for a priority get a middle value instead.
const MIN_PRIORITY = 1;
const MAX_PRIORITY = 10;
const DEFAULT_PRIORITY = 5;
const FIXED_PRIORITIES = Object.freeze({
  'payment-notification': 1,
  'generate-report': 10,
});

// Redis connection
// Parses redis:// / rediss:// URLs into BullMQ connection options.
// (Lesson pattern, plus: percent-decoding of credentials, IPv6 hosts, and
// error messages that never echo the URL because it contains the password.)
function parseRedisUrl(url) {
  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    throw new Error('Invalid Redis URL (expected redis://user:password@host:port or rediss://...)');
  }
  if (parsed.protocol !== 'redis:' && parsed.protocol !== 'rediss:') {
    throw new Error(`Invalid Redis URL protocol "${parsed.protocol}" (use redis:// or rediss://)`);
  }
  return {
    host: parsed.hostname.replace(/^\[|\]$/g, ''),
    port: parseInt(parsed.port, 10) || 6379,
    password: decodeURIComponent(parsed.password) || undefined,
    username: decodeURIComponent(parsed.username) || 'default',
    tls: parsed.protocol === 'rediss:' ? {} : undefined,
    maxRetriesPerRequest: null, // required by BullMQ workers
  };
}

// Producers (the API) must answer quickly when Redis is down instead of
// hanging until it comes back, and must not queue commands that would be
// replayed later, because that would send an email the user was told failed.
function producerConnection(connection) {
  return { ...connection, maxRetriesPerRequest: 1, enableOfflineQueue: false };
}

// Job options & priorities
function buildJobOptions({ backoffDelayMs = 1000 } = {}) {
  return {
    attempts: 3,
    backoff: { type: 'exponential', delay: backoffDelayMs }, // delay * 2^(attempt-1): 1s, 2s, ...
    removeOnComplete: { count: 100 },
    removeOnFail: { count: 500 },
  };
}

function isValidPriority(value) {
  return Number.isInteger(value) && value >= MIN_PRIORITY && value <= MAX_PRIORITY;
}

// Payment notifications are always 1 and reports always 10, whatever the
// client sends; everything else uses the requested priority or the default.
function resolvePriority(type, requested) {
  if (Object.hasOwn(FIXED_PRIORITIES, type)) return FIXED_PRIORITIES[type];
  return requested === undefined ? DEFAULT_PRIORITY : requested;
}

// Queues
function createQueues({ connection, prefix, backoffDelayMs, log = console.warn } = {}) {
  const defaultJobOptions = buildJobOptions({ backoffDelayMs });
  const options = {
    connection: producerConnection(connection),
    defaultJobOptions,
    ...(prefix ? { prefix } : {}),
  };

  let lastWarnAt = 0;
  const onError = (err) => {
    const now = Date.now();
    if (now - lastWarnAt > 10_000) {
      lastWarnAt = now;
      log(`[Queue] Redis connection problem: ${err.code || err.message}`);
    }
  };

  const email = new Queue(QUEUE_NAMES.email, options);
  const tasks = new Queue(QUEUE_NAMES.tasks, options);
  const pdf = new Queue(QUEUE_NAMES.pdf, options);
  const all = [email, tasks, pdf];
  all.forEach((queue) => queue.on('error', onError));

  // A cheap round trip to Redis using only BullMQ's public API (the raw client
  // moved between BullMQ versions: `queue.client` no longer exists in v6).
  // Rejects when Redis is unreachable, because producers do not queue commands.
  async function ping() {
    await email.waitUntilReady();
    await email.getJobCounts('waiting');
    return true;
  }

  // Resolves true as soon as Redis answers, false after timeoutMs.
  async function waitUntilReady(timeoutMs = 5000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      try {
        if (await ping()) return true;
      } catch {
        /* not up yet */
      }
      await new Promise((resolve) => setTimeout(resolve, 150));
    }
    return false;
  }

  async function close() {
    await Promise.allSettled(all.map((queue) => queue.close()));
  }

  return { email, tasks, pdf, ping, waitUntilReady, close, defaultJobOptions };
}

module.exports = {
  QUEUE_NAMES,
  MIN_PRIORITY,
  MAX_PRIORITY,
  DEFAULT_PRIORITY,
  FIXED_PRIORITIES,
  parseRedisUrl,
  producerConnection,
  buildJobOptions,
  isValidPriority,
  resolvePriority,
  createQueues,
};
