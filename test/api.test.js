'use strict';

// API behaviour with queues on a real Redis but NO workers running, so jobs
// stay in the queue and can be inspected.

const test = require('node:test');
const assert = require('node:assert/strict');
const { hasRedis, startRedis, bootStack, waitFor } = require('./helpers');

const skip = !hasRedis && 'redis-server not installed';

test('API (real Redis, no workers)', { skip }, async (t) => {
  const redis = await startRedis();
  const s = await bootStack({ redisPort: redis.port });
  t.after(async () => {
    await s.close();
    await redis.stop();
  });

  await t.test('GET /health reports Redis up', async () => {
    const res = await s.get('/health');
    assert.equal(res.status, 200);
    assert.deepEqual(await res.json(), { status: 'ok', redis: 'up' });
  });

  await t.test('Task 1: POST /api/invoices/:id/send returns 202 immediately and queues the email', async () => {
    const started = Date.now();
    const res = await s.post('/api/invoices/1/send');
    const elapsed = Date.now() - started;
    assert.equal(res.status, 202);
    const body = await res.json();
    assert.equal(body.success, true);
    assert.equal(body.message, 'Invoice email queued for delivery');
    assert.match(body.jobId, /^\d+$/);
    assert.ok(elapsed < 400, `must not wait for the email (took ${elapsed}ms)`);

    const job = await s.queues.email.getJob(body.jobId);
    assert.equal(job.name, 'invoice-email');
    assert.deepEqual(job.data, {
      invoiceId: 1,
      to: 'client@example.com',
      clientName: 'Savannah Coffee Traders Ltd',
      invoiceNumber: 'INV-2026-001',
      amount: 150800,
      currency: 'KES',
      dueDate: '2026-11-30',
    });
    assert.equal(await job.getState(), 'waiting', 'nothing consumed it: the API only enqueues');
    assert.equal(job.opts.attempts, 3);
    assert.deepEqual(job.opts.backoff, { type: 'exponential', delay: 50 });
    assert.ok(s.linesMatching(new RegExp(`\\[API\\] Job ${body.jobId} queued: email "Invoice INV-2026-001" -> client@example.com`)).length === 1);
  });

  await t.test('Task 1: unknown / malformed invoice ids', async () => {
    assert.equal((await s.post('/api/invoices/999/send')).status, 404);
    assert.equal((await s.post('/api/invoices/abc/send')).status, 400);
    assert.equal((await s.post('/api/invoices/0/send')).status, 400);
    assert.equal((await s.post('/api/invoices/1.5/generate-pdf')).status, 400);
    assert.equal((await s.get('/api/invoices/999/pdf-status')).status, 404);
  });

  await t.test('invoice read endpoints', async () => {
    const list = await (await s.get('/api/invoices')).json();
    assert.equal(list.length, 5);
    const one = await (await s.get('/api/invoices/3')).json();
    assert.equal(one.invoiceNumber, 'INV-2026-003');
    assert.equal((await s.get('/api/invoices/42')).status, 404);
  });

  await t.test('Task 2: POST /api/jobs queues a job and returns 201 { jobId, status }', async () => {
    const res = await s.post('/api/jobs', { type: 'reliable', data: { message: 'Hello' } });
    assert.equal(res.status, 201);
    const body = await res.json();
    assert.equal(body.status, 'queued');
    assert.match(body.jobId, /^\d+$/);
    const job = await s.queues.tasks.getJob(body.jobId);
    assert.equal(job.name, 'reliable');
    assert.deepEqual(job.data, { message: 'Hello' });
    assert.equal(job.opts.attempts, 3);
    assert.deepEqual(job.opts.removeOnComplete, { count: 100 });
    assert.deepEqual(job.opts.removeOnFail, { count: 500 });
  });

  await t.test('Task 2: validation errors', async () => {
    for (const bad of [{}, { type: '' }, { type: 5 }, { type: 'x', data: 'no' }, { type: 'x', data: [1] }, { type: 'x', priority: 0 }, { type: 'x', priority: 11 }, { type: 'x', priority: 'high' }]) {
      const res = await s.post('/api/jobs', bad);
      assert.equal(res.status, 400, JSON.stringify(bad));
      assert.equal((await res.json()).success, false);
    }
    assert.equal((await s.post('/api/jobs', '{ not json')).status, 400);
    assert.equal((await s.post('/api/jobs')).status, 400, 'missing body');
    assert.equal((await s.post('/api/jobs', '[]')).status, 400);
  });

  await t.test('Bonus: priorities are applied, fixed types cannot be overridden, default is 5', async () => {
    const priorityOf = async (body) => {
      const res = await (await s.post('/api/jobs', body)).json();
      const job = await s.queues.tasks.getJob(res.jobId);
      assert.equal(res.priority, job.opts.priority, 'response reports the priority actually used');
      return job.opts.priority;
    };
    assert.equal(await priorityOf({ type: 'payment-notification', data: { amount: 5000, currency: 'KES' } }), 1);
    assert.equal(await priorityOf({ type: 'payment-notification', priority: 9 }), 1);
    assert.equal(await priorityOf({ type: 'generate-report', data: { report: 'monthly' } }), 10);
    assert.equal(await priorityOf({ type: 'generate-report', priority: 1 }), 10);
    assert.equal(await priorityOf({ type: 'anything' }), 5);
    assert.equal(await priorityOf({ type: 'anything', priority: 2 }), 2);
  });

  await t.test('Task 2: GET /api/jobs/status counts prioritized jobs as waiting', async () => {
    const counts = await (await s.get('/api/jobs/status')).json();
    assert.deepEqual(Object.keys(counts).sort(), ['active', 'completed', 'delayed', 'failed', 'waiting']);
    const raw = await s.queues.tasks.getJobCounts('waiting', 'prioritized');
    assert.ok(raw.prioritized > 0, 'jobs added with a priority live in BullMQ\'s "prioritized" state');
    assert.equal(counts.waiting, raw.waiting + raw.prioritized);
    assert.ok(counts.waiting >= 7);
    assert.equal(counts.active + counts.completed + counts.failed + counts.delayed, 0);

    const email = await (await s.get('/api/jobs/status?queue=email')).json();
    assert.equal(email.waiting, 1);
    assert.equal((await s.get('/api/jobs/status?queue=nope')).status, 400);
    assert.equal((await s.get('/api/jobs/status?queue=a&queue=b')).status, 400);
  });

  await t.test('GET /api/jobs/:id', async () => {
    const { jobId } = await (await s.post('/api/jobs', { type: 'lookup-me', data: { a: 1 } })).json();
    const body = await (await s.get(`/api/jobs/${jobId}`)).json();
    assert.equal(body.jobId, jobId);
    assert.equal(body.type, 'lookup-me');
    assert.equal(body.maxAttempts, 3);
    assert.equal(body.priority, 5);
    assert.ok(['waiting', 'prioritized'].includes(body.state));
    assert.equal((await s.get('/api/jobs/987654')).status, 404);
    assert.equal((await s.get('/api/jobs/bad%20id')).status, 400);
    assert.equal((await s.get('/api/jobs/status')).status, 200, '"status" is not treated as a job id');
  });

  await t.test('Task 3: POST /api/invoices/:id/generate-pdf queues one job per invoice', async () => {
    const res = await s.post('/api/invoices/1/generate-pdf');
    assert.equal(res.status, 202);
    const body = await res.json();
    assert.deepEqual(body, { success: true, message: 'PDF generation queued', jobId: 'pdf-invoice-1' });

    const job = await s.queues.pdf.getJob('pdf-invoice-1');
    for (const key of ['invoiceId', 'clientName', 'lineItems', 'subtotal', 'taxAmount', 'total', 'currency', 'dueDate']) {
      assert.ok(key in job.data, `job data has ${key}`);
    }
    assert.equal(job.data.taxAmount, 20800);
    assert.equal(job.data.total, 150800);
    assert.equal(job.data.currency, 'KES');

    const again = await s.post('/api/invoices/1/generate-pdf');
    assert.equal(again.status, 202);
    assert.deepEqual(await again.json(), { success: true, message: 'PDF generation already in progress', jobId: 'pdf-invoice-1' });
    assert.equal((await s.queues.pdf.getJobCounts('waiting')).waiting, 1, 'no duplicate job');
  });

  await t.test('Task 3: pdf-status for queued and never-requested invoices', async () => {
    const queued = await (await s.get('/api/invoices/1/pdf-status')).json();
    assert.equal(queued.status, 'queued');
    assert.equal(queued.jobId, 'pdf-invoice-1');

    const none = await s.get('/api/invoices/2/pdf-status');
    assert.equal(none.status, 404);
    assert.equal((await none.json()).status, 'not_requested');
  });

  await t.test('misc: unknown route and CORS', async () => {
    const res = await s.get('/nope');
    assert.equal(res.status, 404);
    assert.match(res.headers.get('content-type'), /json/);
    assert.equal(res.headers.get('x-powered-by'), null);
    assert.equal((await s.get('/api/jobs', { method: 'DELETE' })).status, 404);
  });

  await t.test('static: only finished PDFs are served; temp files and traversal are not', async () => {
    const fs = require('node:fs');
    const path = require('node:path');
    fs.writeFileSync(path.join(s.dir, 'INV-X.pdf'), '%PDF-1.4 test');
    fs.writeFileSync(path.join(s.dir, '.INV-X.pdf.123.tmp'), 'partial');
    const ok = await s.get('/invoices/INV-X.pdf');
    assert.equal(ok.status, 200);
    assert.match(ok.headers.get('content-type'), /application\/pdf/);
    assert.equal((await s.get('/invoices/.INV-X.pdf.123.tmp')).status, 404, 'dot-file temp files are never served');
    assert.equal((await s.get('/invoices/..%2f..%2fpackage.json')).status, 404);
    assert.equal((await s.get('/invoices/')).status, 404, 'no directory listing');
  });
});

test('API fails fast with 503 when Redis is down and recovers when it returns', { skip }, async (t) => {
  const redis = await startRedis();
  const s = await bootStack({ redisPort: redis.port });
  let current = redis;
  t.after(async () => {
    await s.close();
    await current.stop();
  });

  assert.equal((await s.post('/api/invoices/1/send')).status, 202, 'works while Redis is up');
  await waitFor(async () => (await s.get('/health')).status === 200);

  await current.stop();
  await waitFor(async () => (await s.get('/health')).status === 503, { message: 'health to report Redis down' });

  const started = Date.now();
  const res = await s.post('/api/invoices/1/send');
  const elapsed = Date.now() - started;
  assert.equal(res.status, 503);
  assert.equal(res.headers.get('retry-after'), '5');
  assert.equal((await res.json()).success, false);
  assert.ok(elapsed < 1500, `must fail fast, not hang until Redis returns (took ${elapsed}ms)`);

  for (const [method, url, body] of [
    ['POST', '/api/jobs', { type: 'x' }],
    ['POST', '/api/invoices/1/generate-pdf'],
    ['GET', '/api/jobs/status'],
    ['GET', '/api/invoices/1/pdf-status'],
    ['GET', '/api/jobs/1'],
  ]) {
    const r = method === 'POST' ? await s.post(url, body) : await s.get(url);
    assert.equal(r.status, 503, `${method} ${url}`);
  }
  assert.equal((await s.post('/api/invoices/999/send')).status, 404, 'validation still works without Redis');

  current = await startRedis(redis.port); // Redis comes back on the same port
  await waitFor(async () => (await s.post('/api/invoices/1/send')).status === 202, {
    timeout: 15_000,
    message: 'API to recover after Redis returns',
  });
  assert.equal((await s.get('/health')).status, 200);
});
