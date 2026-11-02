'use strict';

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const PDFDocument = require('pdfkit');

const moneyFormat = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

function safeFileName(invoiceNumber) {
  return `${String(invoiceNumber).replace(/[^A-Za-z0-9._-]/g, '_')}.pdf`;
}

function formatDate(isoDate) {
  const date = new Date(`${isoDate}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return String(isoDate);
  return date.toLocaleDateString('en-GB', { timeZone: 'UTC', day: 'numeric', month: 'short', year: 'numeric' });
}

// Returns an error message if the job payload cannot be turned into a PDF.
// Retrying would not help for these, so the worker fails them permanently.
function validatePdfPayload(data) {
  if (!data || typeof data !== 'object') return 'payload is missing';
  if (typeof data.invoiceNumber !== 'string' || !data.invoiceNumber) return 'invoiceNumber is required';
  if (typeof data.clientName !== 'string' || !data.clientName) return 'clientName is required';
  if (!Array.isArray(data.lineItems) || data.lineItems.length === 0) return 'lineItems must be a non-empty array';
  for (const [index, item] of data.lineItems.entries()) {
    if (!item || typeof item.description !== 'string') return `lineItems[${index}].description is required`;
    if (!Number.isFinite(item.quantity) || item.quantity <= 0) return `lineItems[${index}].quantity must be a positive number`;
    if (!Number.isFinite(item.unitPrice) || item.unitPrice < 0) return `lineItems[${index}].unitPrice must be a non-negative number`;
  }
  for (const field of ['subtotal', 'taxAmount', 'total']) {
    if (!Number.isFinite(data[field])) return `${field} must be a number`;
  }
  return null;
}

// Column layout on an A4 page with 50pt margins (content width 495pt).
const COL = {
  description: { x: 50, width: 255 },
  quantity: { x: 310, width: 45 },
  unitPrice: { x: 360, width: 90 },
  amount: { x: 455, width: 90 },
};
const LEFT = 50;
const RIGHT = 545;
const BOTTOM_LIMIT = 740; // leave room for the footer

function drawTableHeader(doc, currency, y) {
  doc.font('Helvetica-Bold').fontSize(9).fillColor('#555555');
  doc.text('DESCRIPTION', COL.description.x, y, { width: COL.description.width });
  doc.text('QTY', COL.quantity.x, y, { width: COL.quantity.width, align: 'right' });
  doc.text(`UNIT PRICE (${currency})`, COL.unitPrice.x, y, { width: COL.unitPrice.width, align: 'right' });
  doc.text(`AMOUNT (${currency})`, COL.amount.x, y, { width: COL.amount.width, align: 'right' });
  doc.moveTo(LEFT, y + 16).lineTo(RIGHT, y + 16).strokeColor('#999999').lineWidth(0.75).stroke();
  return y + 24;
}

function drawInvoice(doc, data) {
  const currency = data.currency || 'KES';
  const money = (value) => moneyFormat.format(value);

  doc.font('Helvetica-Bold').fontSize(26).fillColor('#111111').text('INVOICE', LEFT, 50);
  doc.font('Helvetica').fontSize(10).fillColor('#444444');
  doc.text(`Invoice No: ${data.invoiceNumber}`, 350, 54, { width: 195, align: 'right' });
  if (data.issueDate) doc.text(`Issued: ${formatDate(data.issueDate)}`, 350, 68, { width: 195, align: 'right' });
  if (data.dueDate) doc.text(`Due: ${formatDate(data.dueDate)}`, 350, 82, { width: 195, align: 'right' });

  doc.font('Helvetica-Bold').fontSize(9).fillColor('#555555').text('BILL TO', LEFT, 120);
  doc.font('Helvetica-Bold').fontSize(12).fillColor('#111111').text(data.clientName, LEFT, 134);
  if (data.clientEmail) doc.font('Helvetica').fontSize(10).fillColor('#444444').text(data.clientEmail, LEFT, 151);

  let y = drawTableHeader(doc, currency, 190);
  doc.font('Helvetica').fontSize(10).fillColor('#111111');

  for (const item of data.lineItems) {
    const rowHeight = Math.max(doc.heightOfString(item.description, { width: COL.description.width }), 14);
    if (y + rowHeight > BOTTOM_LIMIT) {
      doc.addPage();
      y = drawTableHeader(doc, currency, 50);
      doc.font('Helvetica').fontSize(10).fillColor('#111111');
    }
    doc.text(item.description, COL.description.x, y, { width: COL.description.width });
    doc.text(String(item.quantity), COL.quantity.x, y, { width: COL.quantity.width, align: 'right' });
    doc.text(money(item.unitPrice), COL.unitPrice.x, y, { width: COL.unitPrice.width, align: 'right' });
    doc.text(money(Math.round(item.quantity * item.unitPrice * 100) / 100), COL.amount.x, y, {
      width: COL.amount.width,
      align: 'right',
    });
    y += rowHeight + 8;
  }

  // Totals block (kept together on one page)
  if (y + 100 > BOTTOM_LIMIT) {
    doc.addPage();
    y = 50;
  }
  y += 6;
  doc.moveTo(310, y).lineTo(RIGHT, y).strokeColor('#999999').lineWidth(0.75).stroke();
  y += 10;
  const vatPercent = Math.round((data.vatRate ?? 0.16) * 10000) / 100;
  const totals = [
    ['Subtotal', money(data.subtotal), false],
    [`VAT (${vatPercent}%)`, money(data.taxAmount), false],
    [`Total (${currency})`, money(data.total), true],
  ];
  for (const [label, value, bold] of totals) {
    doc.font(bold ? 'Helvetica-Bold' : 'Helvetica').fontSize(bold ? 12 : 10).fillColor('#111111');
    doc.text(label, 310, y, { width: 140 });
    doc.text(value, 455, y, { width: 90, align: 'right' });
    y += bold ? 20 : 16;
  }

  doc.font('Helvetica').fontSize(9).fillColor('#666666');
  const footer = data.dueDate
    ? `Payment is due by ${formatDate(data.dueDate)}. Thank you for your business.`
    : 'Thank you for your business.';
  doc.text(footer, LEFT, 780, { width: RIGHT - LEFT, align: 'center', lineBreak: false });
}

/**
 * Renders the invoice to <dir>/<invoiceNumber>.pdf.
 * The file is written under a temporary dot-name and renamed when complete, so
 * nobody can ever download (or the static server serve) a half-written PDF.
 */
async function generateInvoicePdf(data, dir) {
  const start = process.hrtime.bigint();
  await fsp.mkdir(dir, { recursive: true });

  const fileName = safeFileName(data.invoiceNumber);
  const finalPath = path.join(dir, fileName);
  const tmpPath = path.join(dir, `.${fileName}.${process.pid}.${Date.now()}.tmp`);

  const doc = new PDFDocument({
    size: 'A4',
    margin: 50,
    info: { Title: `Invoice ${data.invoiceNumber}`, Author: 'Invoice Service' },
  });
  const out = fs.createWriteStream(tmpPath);
  const finished = new Promise((resolve, reject) => {
    out.on('finish', resolve);
    out.on('error', reject);
    doc.on('error', reject);
  });
  doc.pipe(out);

  try {
    drawInvoice(doc, data);
    doc.end();
    await finished;
    await fsp.rename(tmpPath, finalPath);
  } catch (err) {
    out.destroy();
    await fsp.rm(tmpPath, { force: true });
    throw err;
  }

  const { size } = await fsp.stat(finalPath);
  return { fileName, filePath: finalPath, size, ms: Number(process.hrtime.bigint() - start) / 1e6 };
}

module.exports = { generateInvoicePdf, validatePdfPayload, safeFileName };
