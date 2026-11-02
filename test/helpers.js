'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const net = require('node:net');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');
const IORedis = require('ioredis');
const { parseRedisUrl, createQueues } = require('../src/jobs/queue');
const { createApp } = require('../src/app');
const { startWorkers } = require('../src/workers');

const Redis = IORedis.default || IORedis;
const hasRedis = spawnSync('redis-server', ['--version']).status === 0;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.once('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

// A real redis-server on a free port (BullMQ needs real Lua scripting).
async function startRedis(port) {
  port = port || (await freePort());
  const proc = spawn('redis-server', ['--port', String(port), '--bind', '127.0.0.1', '--save', '', '--appendonly', 'no'], {
    stdio: 'ignore',
  });
  for (let i = 0; i < 60; i++) {
    const probe = new Redis(port, '127.0.0.1', { lazyConnect: true, retryStrategy: () => null, maxRetriesPerRequest: 0 });
    probe.on('error', () => {});
    try {
      await probe.connect();
      await probe.ping();
      probe.disconnect();
      return {
        port,
        proc,
        stop: () =>
          new Promise((resolve) => {
            if (proc.exitCode !== null) return resolve();
            proc.once('exit', resolve);
            proc.kill('SIGKILL');
          }),
      };
    } catch {
      probe.disconnect();
      await sleep(100);
    }
  }
  proc.kill();
  throw new Error('redis-server did not start');
}

async function waitFor(predicate, { timeout = 10_000, interval = 25, message = 'condition' } = {}) {
  const end = Date.now() + timeout;
  for (;;) {
    const value = await predicate();
    if (value) return value;
    if (Date.now() > end) throw new Error(`Timed out waiting for ${message}`);
    await sleep(interval);
  }
}

/**
 * API + queues (+ optionally workers) against a given Redis port.
 * Every stack gets its own BullMQ key prefix, so tests never see each other's jobs.
 */
async function bootStack({ redisPort, backoffDelayMs = 50, invoicesDir, workerInvoicesDir } = {}) {
  const prefix = `t${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
  const connection = parseRedisUrl(`redis://127.0.0.1:${redisPort}`);
  const lines = [];
  const log = (message) => lines.push(message);

  const queues = createQueues({ connection, prefix, backoffDelayMs, log });
  const dir = invoicesDir || fs.mkdtempSync(path.join(os.tmpdir(), 'invoices-test-'));
  let server;
  try {
    const app = createApp({ queues, invoicesDir: dir, log });
    server = await new Promise((resolve) => {
      const s = app.listen(0, () => resolve(s));
    });
    assert.ok(await queues.waitUntilReady(5000), 'test Redis should be reachable');
  } catch (err) {
    // Do not leave open handles behind, they would keep the test process alive.
    if (server) await new Promise((resolve) => server.close(resolve));
    await queues.close();
    throw err;
  }
  const base = `http://127.0.0.1:${server.address().port}`;

  const stack = {
    base,
    queues,
    lines,
    dir,
    prefix,
    connection,
    workers: null,
    get: (url, init) => fetch(base + url, init),
    post: (url, body) =>
      fetch(base + url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
      }),
    async json(response) {
      return response.json();
    },
    async startWorkers(options = {}) {
      stack.workers = await startWorkers({
        connection,
        prefix,
        queues,
        invoicesDir: workerInvoicesDir || dir,
        publicBaseUrl: base,
        log,
        options,
      });
      return stack.workers;
    },
    linesMatching: (re) => lines.filter((line) => re.test(line)),
    async close() {
      if (stack.workers) await stack.workers.close();
      await new Promise((resolve) => server.close(resolve));
      await queues.close();
      fs.rmSync(dir, { recursive: true, force: true });
    },
  };
  return stack;
}

module.exports = { hasRedis, sleep, freePort, startRedis, waitFor, bootStack };
