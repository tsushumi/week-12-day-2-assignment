'use strict';

// In-memory "database" of invoices (amounts in KES). Totals are derived from
// the line items so they can never disagree with them.

const VAT_RATE = 0.16;
const CURRENCY = 'KES';

// Work in integer cents so 16% VAT never produces float artifacts.
const toCents = (amount) => Math.round(amount * 100);
const fromCents = (cents) => cents / 100;

function computeTotals(lineItems, vatRate = VAT_RATE) {
  const subtotalCents = lineItems.reduce((sum, item) => sum + toCents(item.quantity * item.unitPrice), 0);
  const taxCents = Math.round(subtotalCents * vatRate);
  return {
    subtotal: fromCents(subtotalCents),
    vatRate,
    taxAmount: fromCents(taxCents),
    total: fromCents(subtotalCents + taxCents),
  };
}

const seed = [
  {
    invoiceNumber: 'INV-2026-001',
    clientName: 'Savannah Coffee Traders Ltd',
    clientEmail: 'client@example.com',
    issueDate: '2026-11-01',
    dueDate: '2026-11-30',
    lineItems: [
      { description: 'Website redesign', quantity: 1, unitPrice: 85000 },
      { description: 'Managed hosting (per month)', quantity: 12, unitPrice: 2500 },
      { description: 'Logo and brand kit', quantity: 1, unitPrice: 15000 },
    ],
  },
  {
    invoiceNumber: 'INV-2026-002',
    clientName: 'Mara Safari Lodge',
    clientEmail: 'accounts@mara-lodge.example',
    issueDate: '2026-11-02',
    dueDate: '2026-12-02',
    lineItems: [
      { description: 'Online booking system setup', quantity: 1, unitPrice: 120000 },
      { description: 'M-Pesa payment integration', quantity: 1, unitPrice: 45000 },
      { description: 'Staff training session (per hour)', quantity: 6, unitPrice: 3500 },
    ],
  },
  {
    invoiceNumber: 'INV-2026-003',
    clientName: 'Nairobi Fresh Produce',
    clientEmail: 'billing@nairobifresh.example',
    issueDate: '2026-11-03',
    dueDate: '2026-11-17',
    lineItems: [
      { description: 'Inventory tracking dashboard', quantity: 1, unitPrice: 64000 },
      { description: 'SMS order alerts (bundle of 1,000)', quantity: 5, unitPrice: 1200 },
    ],
  },
  {
    invoiceNumber: 'INV-2026-004',
    clientName: 'Kisumu Boat Works',
    clientEmail: 'office@kisumuboats.example',
    issueDate: '2026-11-04',
    dueDate: '2026-12-04',
    lineItems: [
      { description: 'Product catalogue website', quantity: 1, unitPrice: 52500 },
      { description: 'Product photography (per item)', quantity: 24, unitPrice: 750 },
      { description: 'Domain and email setup', quantity: 1, unitPrice: 6500 },
      { description: 'Search engine optimisation audit and recommendations report', quantity: 1, unitPrice: 18000 },
    ],
  },
  {
    invoiceNumber: 'INV-2026-005',
    clientName: 'Mombasa Spice Co.',
    clientEmail: 'finance@mombasaspice.example',
    issueDate: '2026-11-05',
    dueDate: '2026-11-25',
    lineItems: [
      { description: 'Online shop maintenance retainer', quantity: 3, unitPrice: 9000 },
      { description: 'Courier integration', quantity: 1, unitPrice: 22000 },
    ],
  },
].map((invoice, index) => ({ id: index + 1, currency: CURRENCY, ...invoice, ...computeTotals(invoice.lineItems) }));

// Accepts the raw :id path segment; returns the invoice or null.
function findInvoice(rawId, invoices = seed) {
  if (!/^\d{1,9}$/.test(String(rawId))) return null;
  const id = Number(rawId);
  return invoices.find((invoice) => invoice.id === id) || null;
}

const isValidInvoiceId = (rawId) => /^\d{1,9}$/.test(String(rawId)) && Number(rawId) > 0;

// Job payloads: only what the worker needs, copied so queued data is a snapshot.
function buildEmailPayload(invoice) {
  return {
    invoiceId: invoice.id,
    to: invoice.clientEmail,
    clientName: invoice.clientName,
    invoiceNumber: invoice.invoiceNumber,
    amount: invoice.total,
    currency: invoice.currency,
    dueDate: invoice.dueDate,
  };
}

function buildPdfPayload(invoice) {
  return {
    invoiceId: invoice.id,
    invoiceNumber: invoice.invoiceNumber,
    clientName: invoice.clientName,
    clientEmail: invoice.clientEmail,
    lineItems: invoice.lineItems.map((item) => ({ ...item })),
    subtotal: invoice.subtotal,
    vatRate: invoice.vatRate,
    taxAmount: invoice.taxAmount,
    total: invoice.total,
    currency: invoice.currency,
    issueDate: invoice.issueDate,
    dueDate: invoice.dueDate,
  };
}

module.exports = {
  VAT_RATE,
  CURRENCY,
  invoices: seed,
  computeTotals,
  findInvoice,
  isValidInvoiceId,
  buildEmailPayload,
  buildPdfPayload,
};
