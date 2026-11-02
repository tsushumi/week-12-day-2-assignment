'use strict';

const path = require('node:path');
require('dotenv').config({ quiet: true });

const PROJECT_ROOT = path.resolve(__dirname, '..');
const PLACEHOLDER_RE = /your-password-here|your-endpoint/i;

// Upstash URL if it has been filled in, otherwise a local Redis for development.
function resolveRedisUrl(env = process.env) {
  const upstash = (env.UPSTASH_REDIS_URL || '').trim();
  if (upstash && !PLACEHOLDER_RE.test(upstash)) return { url: upstash, source: 'upstash' };
  return { url: (env.REDIS_URL || '').trim() || 'redis://127.0.0.1:6379', source: 'local' };
}

// Both the API and the worker are separate processes and must agree on this
// folder, so it is anchored to the project root rather than the current directory.
function getConfig(env = process.env) {
  const port = Number(env.PORT) || 3000;
  const { url, source } = resolveRedisUrl(env);
  return {
    port,
    redisUrl: url,
    redisSource: source,
    publicBaseUrl: (env.PUBLIC_BASE_URL || `http://localhost:${port}`).replace(/\/+$/, ''),
    invoicesDir: env.INVOICES_DIR ? path.resolve(PROJECT_ROOT, env.INVOICES_DIR) : path.join(PROJECT_ROOT, 'invoices'),
    projectRoot: PROJECT_ROOT,
  };
}

module.exports = { getConfig, resolveRedisUrl, PROJECT_ROOT };
