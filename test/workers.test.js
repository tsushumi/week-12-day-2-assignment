'use strict';

// End-to-end: API + queues + workers against a real Redis.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { hasRedis, sleep, startRedis, bootStack, waitFor } = require('./helpers');

const skip = !hasRedis && 'redis-server not installed';

async function withStack(t, options, fn) {
  const redis = await startRedis();
  let s;
  // One hook, in the right order: stack first (workers need Redis to close),
  // then Redis. Registered before booting so a failed boot still stops Redis.
  t.after(async () => {
    if (s) await s.close();
    await redis.stop();
  });
  s = await bootStack({ redisPort: redis.port, ...options });
  return fn(s, redis);
}

const taskLines = (s) => s.linesMatching(/^\[Task Worker\]/);

// ------------------------------------------------------------ Task 1 ---
test('Task 1: email job lifecycle queued -> processing -> completed', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    await s.startWorkers({ email: { sendDelayMs: 150 } });

    const { jobId } = await (await s.post('/api/invoices/1/send')).json();
    await waitFor(() => s.linesMatching(new RegExp(`\\[Email Worker\\] Job ${jobId} completed successfully`)).length === 1, {
      message: 'email job to complete',
    });

    const lifecycle = s.lines.filter((l) => new RegExp(`[Jj]ob ${jobId}\\b`).test(l) && (l.startsWith('[API]') || l.startsWith('[Email Worker]')));
    assert.equal(lifecycle.length, 3, lifecycle.join('\n'));
    assert.match(lifecycle[0], new RegExp(`^\\[API\\] Job ${jobId} queued: email "Invoice INV-2026-001" -> client@example.com$`));
    assert.equal(lifecycle[1], `[Email Worker] Processing job ${jobId}: Sending "Invoice INV-2026-001" to client@example.com`);
    assert.equal(lifecycle[2], `[Email Worker] Job ${jobId} completed successfully`);

    const job = await s.queues.email.getJob(jobId);
    assert.equal(await job.getState(), 'completed');
    assert.equal(job.returnvalue.to, 'client@example.com');
    assert.equal(job.returnvalue.simulated, true);
  })
);

test('Task 1: the worker takes about the simulated 500ms send time, the request does not', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    await s.startWorkers(); // default 500ms send delay
    const t0 = Date.now();
    const res = await s.post('/api/invoices/2/send');
    const requestMs = Date.now() - t0;
    assert.equal(res.status, 202);
    assert.ok(requestMs < 300, `request took ${requestMs}ms`);
    const { jobId } = await res.json();
    await waitFor(() => s.linesMatching(new RegExp(`Job ${jobId} completed successfully`)).length === 1);
    const totalMs = Date.now() - t0;
    assert.ok(totalMs >= 500, `email send should take >= 500ms, took ${totalMs}ms`);
  })
);

test('Task 1: failed emails are retried with backoff, unknown email jobs are not', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    await s.startWorkers({ email: { sendDelayMs: 5 } });
    // no recipient => UnrecoverableError, must not be retried
    const bad = await s.queues.email.add('invoice-email', { invoiceNumber: 'X' });
    const unknown = await s.queues.email.add('mystery', { to: 'a@b.c' });
    await waitFor(() => s.linesMatching(/failed permanently/).length === 2, { message: 'both jobs to fail' });
    for (const job of [bad, unknown]) {
      const fresh = await s.queues.email.getJob(job.id);
      assert.equal(fresh.attemptsMade, 1, 'unrecoverable errors skip the remaining attempts');
      assert.equal(await fresh.getState(), 'failed');
    }
    assert.match(s.linesMatching(/Unknown email job type/)[0], /failed permanently/);
  })
);

// ------------------------------------------------------------ Task 2 ---
test('Task 2: reliable job: queued -> processing -> completed', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    await s.startWorkers();
    const { jobId } = await (await s.post('/api/jobs', { type: 'reliable', data: { message: 'Hello' } })).json();
    await waitFor(() => taskLines(s).some((l) => l.includes(`Job ${jobId}: queued -> processing -> completed`)), { message: 'timeline' });
    const lines = taskLines(s).filter((l) => l.includes(`Job ${jobId}:`));
    assert.match(lines[0], new RegExp(`Job ${jobId}: processing \\(reliable, attempt 1/3, priority 5\\)`));
    assert.equal(lines[1], `[Task Worker] Job ${jobId}: completed`);
    assert.equal(lines[2], `[Task Worker] Job ${jobId}: queued -> processing -> completed`);
  })
);

test('Task 2: always-fail exhausts 3 attempts with exponential backoff, then fails permanently', { skip }, async (t) =>
  withStack(t, { backoffDelayMs: 100 }, async (s) => {
    await s.startWorkers();
    const startedAt = Date.now();
    const { jobId } = await (await s.post('/api/jobs', { type: 'always-fail', data: { message: 'Will fail' } })).json();
    await waitFor(() => taskLines(s).some((l) => l.includes('failed permanently after')), { message: 'permanent failure' });
    const elapsed = Date.now() - startedAt;

    const lines = taskLines(s).filter((l) => l.includes(`Job ${jobId}:`));
    assert.match(lines.find((l) => l.includes('attempt 1/3, retrying')), /attempt 1\/3, retrying in 100ms\)/);
    assert.match(lines.find((l) => l.includes('attempt 2/3, retrying')), /attempt 2\/3, retrying in 200ms\)/);
    assert.equal(lines.filter((l) => l.includes('processing (')).length, 3, 'exactly 3 attempts');
    assert.match(lines.find((l) => l.includes('failed permanently')), /failed permanently after 3\/3 attempts - Simulated permanent failure/);
    assert.equal(
      lines.at(-1),
      `[Task Worker] Job ${jobId}: queued -> processing -> failed (attempt 1/3, retrying in 100ms) -> processing -> failed (attempt 2/3, retrying in 200ms) -> processing -> failed (attempt 3/3) -> failed permanently`
    );
    assert.ok(elapsed >= 300, `backoff must actually delay retries (100ms + 200ms), took ${elapsed}ms`);

    const job = await s.queues.tasks.getJob(jobId);
    assert.equal(await job.getState(), 'failed');
    assert.equal(job.attemptsMade, 3);

    const status = await (await s.get('/api/jobs/status')).json();
    assert.deepEqual(status, { waiting: 0, active: 0, completed: 0, failed: 1, delayed: 0 });
    const detail = await (await s.get(`/api/jobs/${jobId}`)).json();
    assert.equal(detail.state, 'failed');
    assert.match(detail.failedReason, /Simulated permanent failure/);
  })
);

test('Task 2: flaky job retries and then completes (deterministic coin flips)', { skip }, async (t) =>
  withStack(t, { backoffDelayMs: 50 }, async (s) => {
    const flips = [0.1, 0.9]; // < 0.5 fails, otherwise succeeds
    await s.startWorkers({ tasks: { random: () => flips.shift() } });
    const { jobId } = await (await s.post('/api/jobs', { type: 'flaky', data: { message: 'Might fail' } })).json();
    await waitFor(() => taskLines(s).some((l) => l.includes(`Job ${jobId}: queued ->`)), { message: 'timeline' });
    assert.equal(
      taskLines(s).at(-1),
      `[Task Worker] Job ${jobId}: queued -> processing -> failed (attempt 1/3, retrying in 50ms) -> processing -> completed`
    );
    // BullMQ counts every finished attempt, the successful one included
    assert.equal((await s.queues.tasks.getJob(jobId)).attemptsMade, 2, 'failed once, succeeded on the second attempt');
  })
);

test('Task 2: real Math.random flaky jobs always end up completed or failed after at most 3 attempts', { skip }, async (t) =>
  withStack(t, { backoffDelayMs: 20 }, async (s) => {
    await s.startWorkers({ tasks: { workMs: 5 } });
    const ids = [];
    for (let i = 0; i < 12; i++) ids.push((await (await s.post('/api/jobs', { type: 'flaky' })).json()).jobId);
    await waitFor(async () => {
      const c = await (await s.get('/api/jobs/status')).json();
      return c.completed + c.failed === 12;
    }, { message: 'all flaky jobs to settle' });
    for (const id of ids) {
      const job = await s.queues.tasks.getJob(id);
      assert.ok(job.attemptsMade <= 3);
    }
    const processed = taskLines(s).filter((l) => l.includes('processing (')).length;
    assert.ok(processed >= 12 && processed <= 36);
  })
);

test('Task 2: the tasks worker is sequential (one job at a time)', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    for (let i = 0; i < 4; i++) await s.post('/api/jobs', { type: 'reliable', data: { i } });
    await s.startWorkers({ tasks: { workMs: 60 } });
    await waitFor(() => taskLines(s).filter((l) => l.endsWith(': completed')).length === 4);
    const events = taskLines(s)
      .filter((l) => l.includes('processing (') || l.endsWith(': completed'))
      .map((l) => (l.includes('processing (') ? 'start' : 'done'));
    assert.deepEqual(events, ['start', 'done', 'start', 'done', 'start', 'done', 'start', 'done'], 'never two jobs at once');
  })
);

test('Task 2: status endpoint reflects waiting / active / delayed / completed', { skip }, async (t) =>
  withStack(t, { backoffDelayMs: 600 }, async (s) => {
    for (let i = 0; i < 3; i++) await s.post('/api/jobs', { type: 'reliable' });
    await s.post('/api/jobs', { type: 'always-fail' });
    assert.equal((await (await s.get('/api/jobs/status')).json()).waiting, 4);

    await s.startWorkers({ tasks: { workMs: 150 } });
    await waitFor(async () => (await (await s.get('/api/jobs/status')).json()).active === 1, { message: 'a job to be active' });
    await waitFor(async () => {
      const c = await (await s.get('/api/jobs/status')).json();
      return c.completed === 3 && c.delayed === 1;
    }, { message: '3 completed + the failing job waiting for its retry' });
    const c = await (await s.get('/api/jobs/status')).json();
    assert.deepEqual(c, { waiting: 0, active: 0, completed: 3, failed: 0, delayed: 1 });
  })
);

// ------------------------------------------------------------- Bonus ---
test('Bonus: high priority jobs are processed first even when added last', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    // Everything is queued BEFORE a worker exists, in the worst order for the bonus.
    const add = async (body) => (await (await s.post('/api/jobs', body)).json()).jobId;
    const report = await add({ type: 'generate-report', data: { report: 'monthly' } });
    const plain = await add({ type: 'reliable', data: { n: 1 } }); // no priority given
    const low = await add({ type: 'reliable', priority: 8 });
    const payment = await add({ type: 'payment-notification', data: { amount: 5000, currency: 'KES' } });
    const payment2 = await add({ type: 'payment-notification', data: { amount: 700, currency: 'KES' } });
    const mid = await add({ type: 'reliable', priority: 3 });
    const report2 = await add({ type: 'generate-report', data: { report: 'weekly' } });

    const order = [];
    await s.startWorkers({ tasks: { workMs: 5, onProcess: (job) => order.push(job.id) } });
    await waitFor(() => order.length === 7, { message: 'all 7 jobs processed' });

    assert.deepEqual(order, [payment, payment2, mid, plain, low, report, report2], [
      'expected: payment (1), payment2 (1, FIFO), priority 3, default 5, priority 8, report (10), report2 (10, FIFO)',
      `got: ${order.join(', ')}`,
    ].join('\n'));
  })
);

test('Bonus: a job queued while the worker is busy still jumps ahead of waiting low-priority jobs', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    const order = [];
    await s.startWorkers({ tasks: { workMs: 150, onProcess: (job) => order.push(job.name) } });
    await s.post('/api/jobs', { type: 'busy' }); // occupies the worker
    await waitFor(() => order.length === 1);
    await s.post('/api/jobs', { type: 'generate-report', data: { report: 'monthly' } });
    await s.post('/api/jobs', { type: 'reliable' });
    await s.post('/api/jobs', { type: 'payment-notification', data: { amount: 5000, currency: 'KES' } });
    await waitFor(() => order.length === 4);
    assert.deepEqual(order, ['busy', 'payment-notification', 'reliable', 'generate-report']);
  })
);

// ------------------------------------------------------------ Task 3 ---
test('Task 3: PDF generation end to end (queue -> worker -> file -> notification email)', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    await s.startWorkers({ email: { sendDelayMs: 10 } });

    const res = await s.post('/api/invoices/1/generate-pdf');
    assert.equal(res.status, 202);
    const { jobId } = await res.json();

    const status = await waitFor(async () => {
      const body = await (await s.get('/api/invoices/1/pdf-status')).json();
      return body.status === 'completed' && body;
    }, { message: 'pdf-status to become completed' });
    assert.equal(status.pdfUrl, '/invoices/INV-2026-001.pdf');
    assert.ok(!Number.isNaN(Date.parse(status.generatedAt)));
    assert.equal(status.jobId, jobId);

    // the file really exists and is a PDF
    const file = path.join(s.dir, 'INV-2026-001.pdf');
    assert.ok(fs.existsSync(file));
    const bytes = fs.readFileSync(file);
    assert.equal(bytes.subarray(0, 5).toString(), '%PDF-');
    const download = await s.get(status.pdfUrl);
    assert.equal(download.status, 200);
    assert.equal(download.headers.get('content-type'), 'application/pdf');
    assert.equal(Buffer.from(await download.arrayBuffer()).length, bytes.length);

    // worker log lines from the assignment's expected output
    assert.ok(s.lines.includes('[PDF Worker] Generating PDF for invoice INV-2026-001...'));
    assert.ok(s.linesMatching(/^\[PDF Worker\] PDF saved to .*INV-2026-001\.pdf \(\d+\.\d KB, \d+ms\)$/).length === 1);
    assert.ok(s.lines.includes('[PDF Worker] Notification email queued for client@example.com'));

    // ...and the notification was picked up by the EMAIL worker with the download URL
    await waitFor(() => s.linesMatching(/Sending "Your invoice INV-2026-001 PDF is ready" to client@example.com/).length === 1, {
      message: 'notification email to be processed',
    });
    const notification = (await s.queues.email.getJobs(['completed', 'waiting', 'active'])).find((j) => j.name === 'pdf-ready');
    assert.equal(notification.data.downloadUrl, `${s.base}/invoices/INV-2026-001.pdf`);
    assert.equal(notification.data.to, 'client@example.com');
  })
);

test('Task 3: regenerating after completion works and notifies again; no temp files remain', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    await s.startWorkers({ email: { sendDelayMs: 5 } });
    const done = async () => (await (await s.get('/api/invoices/3/pdf-status')).json()).status === 'completed';

    assert.equal((await (await s.post('/api/invoices/3/generate-pdf')).json()).message, 'PDF generation queued');
    await waitFor(done, { message: 'first generation' });
    await sleep(20);

    const second = await (await s.post('/api/invoices/3/generate-pdf')).json();
    assert.equal(second.message, 'PDF generation queued', 'a finished job does not block regeneration');
    await waitFor(() => s.linesMatching(/Generating PDF for invoice INV-2026-003/).length === 2, { message: 'second generation' });
    await waitFor(done);

    await waitFor(() => s.linesMatching(/Sending "Your invoice INV-2026-003 PDF is ready"/).length === 2, {
      message: 'a notification for each generation',
    });
    assert.deepEqual(fs.readdirSync(s.dir), ['INV-2026-003.pdf']);
  })
);

test('Task 3: pdf-status shows in_progress while the worker is generating and queued before', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    await s.post('/api/invoices/4/generate-pdf');
    assert.equal((await (await s.get('/api/invoices/4/pdf-status')).json()).status, 'queued');

    let release;
    const gate = new Promise((resolve) => (release = resolve));
    const { createPdfProcessor } = require('../src/workers/pdf.worker');
    const { Worker } = require('bullmq');
    const inner = createPdfProcessor({ queues: s.queues, invoicesDir: s.dir, publicBaseUrl: s.base, log: () => {} });
    const worker = new Worker('pdf', async (job) => { await gate; return inner(job); }, { connection: s.connection, prefix: s.prefix });
    // Closed in try/finally, not t.after: after-hooks run in registration order,
    // so Redis would already be stopped and worker.close() would wait forever.
    // The gate is opened first so a failed assertion cannot leave a job running.
    try {
      await waitFor(async () => (await (await s.get('/api/invoices/4/pdf-status')).json()).status === 'in_progress', { message: 'in_progress' });
      release();
      await waitFor(async () => (await (await s.get('/api/invoices/4/pdf-status')).json()).status === 'completed', { message: 'completed' });
    } finally {
      release();
      await worker.close();
    }
  })
);

test('Task 3: invalid payload fails once (no pointless retries) and pdf-status reports failed', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    await s.startWorkers();
    await s.queues.pdf.add('generate-pdf', { invoiceId: 2, invoiceNumber: 'INV-2026-002', clientName: 'X', lineItems: [] }, { jobId: 'pdf-invoice-2' });
    const status = await waitFor(async () => {
      const body = await (await s.get('/api/invoices/2/pdf-status')).json();
      return body.status === 'failed' && body;
    }, { message: 'failed status' });
    assert.equal(status.attemptsMade, 1);
    assert.match(status.error, /Invalid PDF job payload: lineItems/);
    assert.match(s.linesMatching(/^\[PDF Worker\].*failed permanently/)[0], /Invalid PDF job payload/);

    // a failed job can be retried by asking again
    const again = await (await s.post('/api/invoices/2/generate-pdf')).json();
    assert.equal(again.message, 'PDF generation queued');
    await waitFor(async () => (await (await s.get('/api/invoices/2/pdf-status')).json()).status === 'completed', { message: 'recovered' });
  })
);

test('Task 3: infrastructure errors (disk) are retried with backoff, then reported as failed', { skip }, async (t) => {
  const blocker = path.join(os.tmpdir(), `not-a-dir-${Date.now()}`);
  fs.writeFileSync(blocker, 'x');
  t.after(() => fs.rmSync(blocker, { force: true }));
  // The API gets a normal folder; only the worker's output path is unusable.
  return withStack(t, { workerInvoicesDir: blocker, backoffDelayMs: 40 }, async (s) => {
    await s.startWorkers();
    await s.post('/api/invoices/5/generate-pdf');
    const status = await waitFor(async () => {
      const body = await (await s.get('/api/invoices/5/pdf-status')).json();
      return body.status === 'failed' && body;
    }, { message: 'failed after retries' });
    assert.equal(status.attemptsMade, 3);
    assert.ok(s.linesMatching(/retrying in 40ms/).length >= 1);
    assert.ok(s.linesMatching(/retrying in 80ms/).length >= 1);
    assert.equal(s.linesMatching(/Notification email queued/).length, 0, 'no notification for a PDF that was never created');
  });
});

test('pdf-status still reports completed when only the file remains, and "missing" when the file is gone', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    fs.writeFileSync(path.join(s.dir, 'INV-2026-005.pdf'), '%PDF-1.4');
    const orphan = await (await s.get('/api/invoices/5/pdf-status')).json();
    assert.equal(orphan.status, 'completed');
    assert.equal(orphan.pdfUrl, '/invoices/INV-2026-005.pdf');

    await s.startWorkers({ email: { sendDelayMs: 5 } });
    await s.post('/api/invoices/1/generate-pdf');
    await waitFor(async () => (await (await s.get('/api/invoices/1/pdf-status')).json()).status === 'completed');
    fs.rmSync(path.join(s.dir, 'INV-2026-001.pdf'));
    const missing = await (await s.get('/api/invoices/1/pdf-status')).json();
    assert.equal(missing.status, 'missing');
    assert.equal((await (await s.post('/api/invoices/1/generate-pdf')).json()).message, 'PDF generation queued');
  })
);

// ---------------------------------------------------------- lifecycle ---
test('graceful shutdown lets the running job finish', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    await s.startWorkers({ email: { sendDelayMs: 400 } });
    const { jobId } = await (await s.post('/api/invoices/1/send')).json();
    await waitFor(() => s.linesMatching(new RegExp(`Processing job ${jobId}`)).length === 1);
    await s.workers.close(); // mid-send
    s.workers = null;
    assert.equal(s.linesMatching(new RegExp(`Job ${jobId} completed successfully`)).length, 1, 'finished before close() resolved');
    assert.equal(await (await s.queues.email.getJob(jobId)).getState(), 'completed');
  })
);

test('jobs queued while no worker is running are processed when it starts', { skip }, async (t) =>
  withStack(t, {}, async (s) => {
    const { jobId } = await (await s.post('/api/invoices/1/send')).json();
    await sleep(150);
    assert.equal(await (await s.queues.email.getJob(jobId)).getState(), 'waiting');
    await s.startWorkers({ email: { sendDelayMs: 5 } });
    await waitFor(() => s.linesMatching(new RegExp(`Job ${jobId} completed successfully`)).length === 1);
  })
);
