'use strict';

const path = require('node:path');
const { Worker, UnrecoverableError } = require('bullmq');
const { QUEUE_NAMES } = require('../jobs/queue');
const { generateInvoicePdf, validatePdfPayload } = require('../services/pdf');
const { describeFailure, attachErrorLogging } = require('./common');

const LABEL = 'PDF Worker';

// "./invoices/INV-2026-001.pdf" style path for logs.
function displayPath(filePath) {
  const relative = path.relative(process.cwd(), filePath);
  return relative.startsWith('..') ? filePath : `./${relative.split(path.sep).join('/')}`;
}

function createPdfProcessor({ queues, invoicesDir, publicBaseUrl, log = console.log }) {
  return async function processPdf(job) {
    const data = job.data;
    const problem = validatePdfPayload(data);
    if (problem) throw new UnrecoverableError(`Invalid PDF job payload: ${problem}`); // retrying cannot fix it

    log(`[${LABEL}] Generating PDF for invoice ${data.invoiceNumber}...`);
    const result = await generateInvoicePdf(data, invoicesDir);
    log(`[${LABEL}] PDF saved to ${displayPath(result.filePath)} (${(result.size / 1024).toFixed(1)} KB, ${Math.round(result.ms)}ms)`);

    const pdfUrl = `/invoices/${result.fileName}`;
    const downloadUrl = `${publicBaseUrl}${pdfUrl}`;

    // Tell the client it is ready. The id is derived from this job so a retry of
    // this job can never send the notification twice, while regenerating the PDF
    // later (a new job instance, new timestamp) does notify again.
    await queues.email.add(
      'pdf-ready',
      {
        invoiceId: data.invoiceId,
        to: data.clientEmail,
        clientName: data.clientName,
        invoiceNumber: data.invoiceNumber,
        downloadUrl,
      },
      { jobId: `pdf-ready-${data.invoiceId}-${job.timestamp}` }
    );
    log(`[${LABEL}] Notification email queued for ${data.clientEmail}`);

    return { pdfUrl, downloadUrl, fileName: result.fileName, size: result.size, generatedAt: new Date().toISOString() };
  };
}

function createPdfWorker({ connection, prefix, queues, invoicesDir, publicBaseUrl, log = console.log, concurrency = 2 }) {
  const worker = new Worker(QUEUE_NAMES.pdf, createPdfProcessor({ queues, invoicesDir, publicBaseUrl, log }), {
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

module.exports = { createPdfWorker, createPdfProcessor };
