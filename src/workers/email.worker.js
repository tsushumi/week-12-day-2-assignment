'use strict';

const { Worker, UnrecoverableError } = require('bullmq');
const { QUEUE_NAMES } = require('../jobs/queue');
const { sleep, describeFailure, attachErrorLogging } = require('./common');

const LABEL = 'Email Worker';

const formatMoney = (amount, currency) =>
  `${currency} ${new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(amount)}`;

// Builds the message for each kind of email job.
function buildEmail(job) {
  const d = job.data || {};
  if (job.name === 'invoice-email') {
    return {
      to: d.to,
      subject: `Invoice ${d.invoiceNumber}`,
      body: `Hello ${d.clientName || 'there'}, your invoice for ${formatMoney(d.amount, d.currency)} is due on ${d.dueDate}.`,
    };
  }
  if (job.name === 'pdf-ready') {
    return {
      to: d.to,
      subject: `Your invoice ${d.invoiceNumber} PDF is ready`,
      body: `Hello ${d.clientName || 'there'}, your PDF is ready. Download it here: ${d.downloadUrl}`,
    };
  }
  throw new UnrecoverableError(`Unknown email job type "${job.name}"`);
}

function createEmailProcessor({ log = console.log, sendDelayMs = 500 } = {}) {
  return async function processEmail(job) {
    const email = buildEmail(job);
    if (!email.to) throw new UnrecoverableError('Email job has no recipient');

    log(`[${LABEL}] Processing job ${job.id}: Sending "${email.subject}" to ${email.to}`);
    await sleep(sendDelayMs); // simulated provider call
    // In production this is where the real send happens, e.g. with Resend:
    //   await resend.emails.send({ from, to: email.to, subject: email.subject, html: email.body });
    return { to: email.to, subject: email.subject, simulated: true, sentAt: new Date().toISOString() };
  };
}

function createEmailWorker({ connection, prefix, log = console.log, concurrency = 5, sendDelayMs } = {}) {
  const worker = new Worker(QUEUE_NAMES.email, createEmailProcessor({ log, sendDelayMs }), {
    connection,
    concurrency,
    ...(prefix ? { prefix } : {}),
  });

  worker.on('completed', (job) => log(`[${LABEL}] Job ${job.id} completed successfully`));
  worker.on('failed', (job, err) => {
    if (!job) return log(`[${LABEL}] A job failed: ${err.message}`);
    const f = describeFailure(job, err);
    log(
      f.willRetry
        ? `[${LABEL}] Job ${job.id} failed (attempt ${f.attempt}/${f.max}, retrying in ${f.retryDelayMs}ms): ${err.message}`
        : `[${LABEL}] Job ${job.id} failed permanently: ${err.message}`
    );
  });
  attachErrorLogging(worker, LABEL, log);
  return worker;
}

module.exports = { createEmailWorker, createEmailProcessor, buildEmail };
