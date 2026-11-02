'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {
  parseRedisUrl,
  buildJobOptions,
  isValidPriority,
  resolvePriority,
  DEFAULT_PRIORITY,
} = require('../src/jobs/queue');
const { resolveRedisUrl, getConfig } = require('../src/config');
const { computeTotals, findInvoice, invoices, buildPdfPayload } = require('../src/data/invoices');
const { computeBackoffDelay, describeFailure } = require('../src/workers/common');
const { generateInvoicePdf, validatePdfPayload } = require('../src/services/pdf');
const { validateJobBody } = require('../src/app');

// ------------------------------------------------------- parseRedisUrl ---
test('parseRedisUrl: Upstash rediss:// URL', () => {
  assert.deepEqual(parseRedisUrl('rediss://default:s3cret@eu1-fast-owl-1234.upstash.io:6379'), {
    host: 'eu1-fast-owl-1234.upstash.io',
    port: 6379,
    password: 's3cret',
    username: 'default',
    tls: {},
    maxRetriesPerRequest: null,
  });
});

test('parseRedisUrl: defaults (port 6379, user "default", no TLS, no password)', () => {
  const conn = parseRedisUrl('redis://127.0.0.1');
  assert.equal(conn.port, 6379);
  assert.equal(conn.username, 'default');
  assert.equal(conn.password, undefined);
  assert.equal(conn.tls, undefined);
  assert.equal(conn.maxRetriesPerRequest, null, 'BullMQ workers require null');
  assert.equal(parseRedisUrl('redis://localhost:6390').port, 6390);
});

test('parseRedisUrl: percent-encoded credentials are decoded (URL.password is not)', () => {
  const conn = parseRedisUrl('rediss://default:p%40ss%3Aw%2Frd@host.example:6379');
  assert.equal(conn.password, 'p@ss:w/rd');
  assert.equal(parseRedisUrl('redis://us%40er:pw@h.example').username, 'us@er');
});

test('parseRedisUrl: IPv6 host loses its brackets', () => {
  assert.equal(parseRedisUrl('redis://[::1]:6379').host, '::1');
});

test('parseRedisUrl: bad input throws without echoing the password', () => {
  assert.throws(() => parseRedisUrl('not a url'), /Invalid Redis URL/);
  assert.throws(() => parseRedisUrl('http://default:topsecret@host:6379'), (err) => {
    assert.match(err.message, /protocol/);
    assert.ok(!err.message.includes('topsecret'));
    return true;
  });
  assert.throws(() => parseRedisUrl('::topsecret::'), (err) => !err.message.includes('topsecret'));
});

// ------------------------------------------------------------- config ---
test('resolveRedisUrl: placeholder or empty Upstash URL falls back to local Redis', () => {
  const placeholder = 'rediss://default:your-password-here@your-endpoint.upstash.io:6379';
  assert.deepEqual(resolveRedisUrl({ UPSTASH_REDIS_URL: placeholder }), { url: 'redis://127.0.0.1:6379', source: 'local' });
  assert.equal(resolveRedisUrl({}).source, 'local');
  assert.equal(resolveRedisUrl({ UPSTASH_REDIS_URL: '   ' }).source, 'local');
  assert.equal(resolveRedisUrl({ UPSTASH_REDIS_URL: placeholder, REDIS_URL: 'redis://cache:6380' }).url, 'redis://cache:6380');
  const real = 'rediss://default:abc@eu1-real.upstash.io:6379';
  assert.deepEqual(resolveRedisUrl({ UPSTASH_REDIS_URL: real, REDIS_URL: 'redis://x' }), { url: real, source: 'upstash' });
});

test('getConfig: port, public URL and invoices dir', () => {
  const config = getConfig({ PORT: '4100', PUBLIC_BASE_URL: 'https://api.example.com/' });
  assert.equal(config.port, 4100);
  assert.equal(config.publicBaseUrl, 'https://api.example.com');
  assert.ok(path.isAbsolute(config.invoicesDir));
  assert.equal(getConfig({}).publicBaseUrl, 'http://localhost:3000');
});

// -------------------------------------------------- job options / priority ---
test('buildJobOptions matches the assignment defaults', () => {
  assert.deepEqual(buildJobOptions(), {
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000 },
    removeOnComplete: { count: 100 },
    removeOnFail: { count: 500 },
  });
  assert.equal(buildJobOptions({ backoffDelayMs: 50 }).backoff.delay, 50);
});

test('priority rules: fixed types always win, default is middle, validation is 1-10 integers', () => {
  assert.equal(resolvePriority('payment-notification', undefined), 1);
  assert.equal(resolvePriority('payment-notification', 9), 1, 'client cannot downgrade a payment notification');
  assert.equal(resolvePriority('generate-report', 1), 10, 'client cannot upgrade a report');
  assert.equal(resolvePriority('reliable', undefined), DEFAULT_PRIORITY);
  assert.equal(resolvePriority('reliable', 3), 3);
  assert.equal(resolvePriority('constructor', undefined), DEFAULT_PRIORITY, 'prototype keys are not special');
  for (const bad of [0, 11, 1.5, '2', null, NaN, -1]) assert.equal(isValidPriority(bad), false, String(bad));
  for (const good of [1, 5, 10]) assert.equal(isValidPriority(good), true);
});

test('validateJobBody', () => {
  assert.deepEqual(validateJobBody({ type: 'reliable', data: { a: 1 } }).value, { type: 'reliable', data: { a: 1 }, priority: undefined });
  assert.deepEqual(validateJobBody({ type: 'x' }).value.data, {}, 'data is optional');
  for (const bad of [undefined, null, [], 'str', {}, { type: '' }, { type: 5 }, { type: 'has space' }, { type: 'a'.repeat(65) },
    { type: 'x', data: [] }, { type: 'x', data: null }, { type: 'x', data: 'str' }, { type: 'x', priority: 0 }, { type: 'x', priority: 'high' }]) {
    assert.ok(validateJobBody(bad).error, `should reject ${JSON.stringify(bad)}`);
  }
});

// ------------------------------------------------------------ backoff ---
test('computeBackoffDelay: exponential doubles each attempt', () => {
  const backoff = { type: 'exponential', delay: 1000 };
  assert.deepEqual([1, 2, 3, 4].map((n) => computeBackoffDelay(backoff, n)), [1000, 2000, 4000, 8000]);
  assert.equal(computeBackoffDelay({ type: 'fixed', delay: 300 }, 3), 300);
  assert.equal(computeBackoffDelay(500, 2), 500);
  assert.equal(computeBackoffDelay(undefined, 1), 0);
});

test('describeFailure: retry vs permanent vs unrecoverable', () => {
  const opts = { attempts: 3, backoff: { type: 'exponential', delay: 1000 } };
  assert.deepEqual(describeFailure({ attemptsMade: 1, opts }, new Error('x')), { attempt: 1, max: 3, willRetry: true, retryDelayMs: 1000, unrecoverable: false });
  assert.equal(describeFailure({ attemptsMade: 2, opts }, new Error('x')).retryDelayMs, 2000);
  assert.equal(describeFailure({ attemptsMade: 3, opts }, new Error('x')).willRetry, false);
  const unrecoverable = Object.assign(new Error('bad'), { name: 'UnrecoverableError' });
  assert.equal(describeFailure({ attemptsMade: 1, opts }, unrecoverable).willRetry, false, 'no retry even with attempts left');
  assert.equal(describeFailure({ attemptsMade: 1, opts: {} }, new Error('x')).max, 1);
});

// ------------------------------------------------------------ invoices ---
test('totals: 16% VAT, computed in cents', () => {
  const invoice = findInvoice('1');
  assert.equal(invoice.subtotal, 130000);
  assert.equal(invoice.taxAmount, 20800);
  assert.equal(invoice.total, 150800);
  assert.equal(invoice.invoiceNumber, 'INV-2026-001');
  assert.equal(invoice.clientEmail, 'client@example.com');
  // the classic float trap: 0.1 * 3 and 16% of odd amounts
  const t = computeTotals([{ quantity: 3, unitPrice: 0.1 }, { quantity: 1, unitPrice: 19.99 }]);
  assert.equal(t.subtotal, 20.29);
  assert.equal(t.taxAmount, 3.25);
  assert.equal(t.total, 23.54);
  for (const inv of invoices) assert.equal(inv.total, Math.round((inv.subtotal + inv.taxAmount) * 100) / 100);
});

test('findInvoice only accepts plain positive integer ids', () => {
  assert.equal(findInvoice('5').id, 5);
  for (const bad of ['0', '99', 'abc', '1.5', '-1', '1e1', '', ' 1', '1 ', '0x1', '1; DROP']) assert.equal(findInvoice(bad), null, bad);
});

test('PDF payload contains every field the assignment lists', () => {
  const payload = buildPdfPayload(findInvoice('1'));
  for (const key of ['invoiceId', 'clientName', 'lineItems', 'subtotal', 'taxAmount', 'total', 'currency', 'dueDate']) {
    assert.ok(key in payload, key);
  }
  assert.equal(payload.currency, 'KES');
  assert.equal(payload.vatRate, 0.16);
  assert.equal(validatePdfPayload(payload), null);
  payload.lineItems[0].description = 'mutated';
  assert.notEqual(findInvoice('1').lineItems[0].description, 'mutated', 'payload is a copy');
});

// ----------------------------------------------------------------- PDF ---
test('validatePdfPayload rejects unusable payloads', () => {
  const good = buildPdfPayload(findInvoice('1'));
  assert.equal(validatePdfPayload(good), null);
  const cases = [
    [undefined, /missing/],
    [{ ...good, invoiceNumber: '' }, /invoiceNumber/],
    [{ ...good, clientName: undefined }, /clientName/],
    [{ ...good, lineItems: [] }, /lineItems/],
    [{ ...good, lineItems: [{ description: 'x', quantity: 0, unitPrice: 1 }] }, /quantity/],
    [{ ...good, lineItems: [{ description: 'x', quantity: 1, unitPrice: -1 }] }, /unitPrice/],
    [{ ...good, lineItems: [{ quantity: 1, unitPrice: 1 }] }, /description/],
    [{ ...good, total: 'a lot' }, /total/],
  ];
  for (const [payload, re] of cases) assert.match(validatePdfPayload(payload), re);
});

test('generateInvoicePdf writes a real PDF atomically', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'pdf-unit-'));
  try {
    const result = await generateInvoicePdf(buildPdfPayload(findInvoice('1')), dir);
    assert.equal(result.fileName, 'INV-2026-001.pdf');
    assert.ok(result.size > 1000, `size ${result.size}`);
    assert.ok(result.ms > 0);
    const bytes = fs.readFileSync(result.filePath);
    assert.equal(bytes.subarray(0, 5).toString(), '%PDF-');
    assert.match(bytes.subarray(-8).toString(), /%%EOF/);
    assert.deepEqual(fs.readdirSync(dir), ['INV-2026-001.pdf'], 'no temp files left behind');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('generateInvoicePdf paginates long invoices and sanitizes the file name', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'pdf-unit-'));
  try {
    const payload = buildPdfPayload(findInvoice('2'));
    payload.invoiceNumber = '../../evil/INV 7';
    payload.lineItems = Array.from({ length: 70 }, (_, i) => ({ description: `Line item number ${i + 1} with a reasonably long description`, quantity: 2, unitPrice: 100 + i }));
    const result = await generateInvoicePdf(payload, dir);
    assert.equal(path.dirname(result.filePath), dir, 'cannot escape the output directory');
    assert.match(result.fileName, /^[A-Za-z0-9._-]+\.pdf$/);
    const text = fs.readFileSync(result.filePath).toString('latin1');
    const pages = (text.match(/\/Type \/Page\b(?!s)/g) || []).length;
    assert.ok(pages >= 2, `expected several pages, got ${pages}`);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('generateInvoicePdf cleans up its temp file when writing fails', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'pdf-unit-'));
  const blocker = path.join(dir, 'not-a-dir');
  fs.writeFileSync(blocker, 'x');
  try {
    await assert.rejects(() => generateInvoicePdf(buildPdfPayload(findInvoice('1')), blocker));
    assert.deepEqual(fs.readdirSync(dir), ['not-a-dir']);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
