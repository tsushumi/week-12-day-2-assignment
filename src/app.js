'use strict';

const fs = require('node:fs');
const path = require('node:path');
const express = require('express');
const cors = require('cors');
const {
  invoices: seedInvoices,
  findInvoice,
  isValidInvoiceId,
  buildEmailPayload,
  buildPdfPayload,
} = require('./data/invoices');
const { MIN_PRIORITY, MAX_PRIORITY, isValidPriority, resolvePriority } = require('./jobs/queue');

const JOB_TYPE_RE = /^[A-Za-z0-9._-]{1,64}$/;
const JOB_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;
const ACTIVE_STATES = new Set(['waiting', 'active', 'delayed', 'prioritized', 'waiting-children']);

class QueueUnavailableError extends Error {
  constructor(cause) {
    super('Job queue is unavailable');
    this.name = 'QueueUnavailableError';
    this.cause = cause;
  }
}

// Any failure talking to Redis/BullMQ becomes a 503 (the request was valid;
// the infrastructure is the problem) instead of a generic 500.
async function queueOp(fn) {
  try {
    return await fn();
  } catch (err) {
    throw new QueueUnavailableError(err);
  }
}

const pdfJobId = (invoiceId) => `pdf-invoice-${invoiceId}`;

function safeFileName(invoiceNumber) {
  return `${String(invoiceNumber).replace(/[^A-Za-z0-9._-]/g, '_')}.pdf`;
}

function validateJobBody(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return { error: 'Request body must be a JSON object like { "type": "reliable", "data": {} }' };
  }
  const { type, data, priority } = body;
  if (typeof type !== 'string' || !JOB_TYPE_RE.test(type)) {
    return { error: '"type" is required: a string of 1-64 letters, digits, ".", "_" or "-"' };
  }
  if (data !== undefined && (data === null || typeof data !== 'object' || Array.isArray(data))) {
    return { error: '"data" must be a JSON object' };
  }
  if (priority !== undefined && !isValidPriority(priority)) {
    return { error: `"priority" must be an integer from ${MIN_PRIORITY} (highest) to ${MAX_PRIORITY} (lowest)` };
  }
  return { value: { type, data: data ?? {}, priority } };
}

function createApp({
  queues,
  invoices = seedInvoices,
  invoicesDir = path.resolve(process.cwd(), 'invoices'),
  log = console.log,
} = {}) {
  if (!queues) throw new Error('createApp requires { queues }');

  const app = express();
  app.disable('x-powered-by');
  app.use(cors());
  app.use(express.json({ limit: '100kb' }));

  app.use((req, res, next) => {
    const start = process.hrtime.bigint();
    res.on('finish', () => {
      const ms = Number(process.hrtime.bigint() - start) / 1e6;
      log(`[API] ${req.method} ${req.originalUrl} -> ${res.statusCode} (${ms.toFixed(1)}ms)`);
    });
    next();
  });

  fs.mkdirSync(invoicesDir, { recursive: true });
  // Generated PDFs. Temp files are dot-files, which are never served.
  app.use('/invoices', express.static(invoicesDir, { index: false, dotfiles: 'ignore', redirect: false }));

  const lookup = (req, res) => {
    if (!isValidInvoiceId(req.params.id)) {
      res.status(400).json({ success: false, error: 'Invoice id must be a positive integer' });
      return null;
    }
    const invoice = findInvoice(req.params.id, invoices);
    if (!invoice) {
      res.status(404).json({ success: false, error: `Invoice ${req.params.id} not found` });
      return null;
    }
    return invoice;
  };

  // ---------------------------------------------------------------- health ---
  app.get('/health', async (req, res) => {
    let redis = 'down';
    try {
      if (await queues.ping()) redis = 'up';
    } catch {
      /* stays down */
    }
    res.status(redis === 'up' ? 200 : 503).json({ status: redis === 'up' ? 'ok' : 'degraded', redis });
  });

  // -------------------------------------------------------------- invoices ---
  app.get('/api/invoices', (req, res) => {
    res.json(
      invoices.map(({ id, invoiceNumber, clientName, clientEmail, total, currency, dueDate }) => ({
        id, invoiceNumber, clientName, clientEmail, total, currency, dueDate,
      }))
    );
  });

  app.get('/api/invoices/:id', (req, res) => {
    const invoice = lookup(req, res);
    if (invoice) res.json(invoice);
  });

  // Task 1: queue the email instead of sending it in the request.
  app.post('/api/invoices/:id/send', async (req, res) => {
    const invoice = lookup(req, res);
    if (!invoice) return;

    const payload = buildEmailPayload(invoice);
    const job = await queueOp(() => queues.email.add('invoice-email', payload));
    log(`[API] Job ${job.id} queued: email "Invoice ${payload.invoiceNumber}" -> ${payload.to}`);

    res.status(202).json({
      success: true,
      message: 'Invoice email queued for delivery',
      jobId: job.id,
    });
  });

  // Task 3: queue PDF generation.
  app.post('/api/invoices/:id/generate-pdf', async (req, res) => {
    const invoice = lookup(req, res);
    if (!invoice) return;

    // One job per invoice: asking again while it is queued/running returns the
    // same job; once it has finished (or failed) the old record is replaced so
    // the PDF can be regenerated.
    const jobId = pdfJobId(invoice.id);
    const existing = await queueOp(() => queues.pdf.getJob(jobId));
    if (existing) {
      const state = await queueOp(() => existing.getState());
      if (ACTIVE_STATES.has(state)) {
        return res.status(202).json({ success: true, message: 'PDF generation already in progress', jobId });
      }
      await queueOp(() => existing.remove());
    }

    const job = await queueOp(() => queues.pdf.add('generate-pdf', buildPdfPayload(invoice), { jobId }));
    log(`[API] Job ${job.id} queued: PDF for invoice ${invoice.invoiceNumber}`);

    res.status(202).json({ success: true, message: 'PDF generation queued', jobId: job.id });
  });

  app.get('/api/invoices/:id/pdf-status', async (req, res) => {
    const invoice = lookup(req, res);
    if (!invoice) return;

    const fileName = safeFileName(invoice.invoiceNumber);
    const filePath = path.join(invoicesDir, fileName);
    const pdfUrl = `/invoices/${fileName}`;
    const job = await queueOp(() => queues.pdf.getJob(pdfJobId(invoice.id)));

    if (!job) {
      // The job record may have been cleaned up while the file still exists.
      if (fs.existsSync(filePath)) {
        return res.json({ status: 'completed', pdfUrl, generatedAt: fs.statSync(filePath).mtime.toISOString() });
      }
      return res.status(404).json({
        status: 'not_requested',
        error: `No PDF has been requested for invoice ${invoice.id}. POST /api/invoices/${invoice.id}/generate-pdf first`,
      });
    }

    const state = await queueOp(() => job.getState());
    const base = { jobId: job.id, attemptsMade: job.attemptsMade };

    if (state === 'completed') {
      if (!fs.existsSync(filePath)) {
        return res.json({
          ...base,
          status: 'missing',
          error: 'The PDF was generated but the file is no longer on disk; generate it again',
        });
      }
      const result = job.returnvalue || {};
      return res.json({
        ...base,
        status: 'completed',
        pdfUrl: result.pdfUrl || pdfUrl,
        generatedAt: result.generatedAt || fs.statSync(filePath).mtime.toISOString(),
      });
    }
    if (state === 'failed') {
      return res.json({ ...base, status: 'failed', error: job.failedReason || 'PDF generation failed' });
    }
    if (state === 'active' || (state === 'delayed' && job.attemptsMade > 0)) {
      // delayed + attempts made = waiting for its next retry
      return res.json({ ...base, status: 'in_progress' });
    }
    return res.json({ ...base, status: 'queued' });
  });

  // ------------------------------------------------------------------ jobs ---
  // Task 2 + bonus: general-purpose queue
  app.post('/api/jobs', async (req, res) => {
    const parsed = validateJobBody(req.body);
    if (parsed.error) return res.status(400).json({ success: false, error: parsed.error });

    const { type, data, priority: requested } = parsed.value;
    const priority = resolvePriority(type, requested);

    const job = await queueOp(() => queues.tasks.add(type, data, { priority }));
    log(`[API] Job ${job.id} queued: ${type} (priority ${priority})`);

    res.status(201).json({ jobId: job.id, status: 'queued', priority });
  });

  // Must be registered before /api/jobs/:id
  app.get('/api/jobs/status', async (req, res) => {
    const name = req.query.queue === undefined ? 'tasks' : req.query.queue;
    if (typeof name !== 'string' || !Object.hasOwn({ email: 1, tasks: 1, pdf: 1 }, name)) {
      return res.status(400).json({ error: 'queue must be one of: tasks, email, pdf' });
    }
    const counts = await queueOp(() =>
      queues[name].getJobCounts('waiting', 'prioritized', 'paused', 'active', 'completed', 'failed', 'delayed')
    );
    // BullMQ keeps prioritized (and paused) jobs out of "waiting", but to a
    // caller they are all simply waiting for a worker.
    res.json({
      waiting: counts.waiting + counts.prioritized + counts.paused,
      active: counts.active,
      completed: counts.completed,
      failed: counts.failed,
      delayed: counts.delayed,
    });
  });

  app.get('/api/jobs/:id', async (req, res) => {
    if (!JOB_ID_RE.test(req.params.id)) return res.status(400).json({ error: 'Invalid job id' });
    const job = await queueOp(() => queues.tasks.getJob(req.params.id));
    if (!job) return res.status(404).json({ error: `Job ${req.params.id} not found` });
    const state = await queueOp(() => job.getState());
    res.json({
      jobId: job.id,
      type: job.name,
      state,
      priority: job.opts.priority ?? null,
      attemptsMade: job.attemptsMade,
      maxAttempts: job.opts.attempts ?? 1,
      data: job.data,
      result: job.returnvalue ?? null,
      failedReason: job.failedReason ?? null,
    });
  });

  // -------------------------------------------------------------- fallthrough ---
  app.use((req, res) => res.status(404).json({ error: `Route not found: ${req.method} ${req.path}` }));

  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, next) => {
    if (res.headersSent) return next(err);
    if (err.type === 'entity.parse.failed') return res.status(400).json({ success: false, error: 'Invalid JSON body' });
    if (err.type === 'entity.too.large') return res.status(413).json({ success: false, error: 'Request body too large' });
    if (err instanceof QueueUnavailableError) {
      log(`[API] Queue error on ${req.method} ${req.originalUrl}: ${err.cause?.message || err.message}`);
      res.set('Retry-After', '5');
      return res.status(503).json({ success: false, error: 'Job queue is temporarily unavailable, please retry shortly' });
    }
    console.error(err);
    return res.status(500).json({ success: false, error: 'Internal server error' });
  });

  app.locals.queues = queues;
  return app;
}

module.exports = { createApp, validateJobBody, safeFileName, pdfJobId, QueueUnavailableError };
