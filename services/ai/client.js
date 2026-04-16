// ═══════════════════════════════════════════════════════════
// Anthropic AI Client
// Centralizes Claude SDK initialization and common message
// patterns. Returns null if ANTHROPIC_API_KEY is not set —
// callers should guard with isConfigured() and degrade gracefully.
// ═══════════════════════════════════════════════════════════

const Anthropic = require('@anthropic-ai/sdk');

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
// Default model — keep in sync with Claude Code's recommended Opus 4.6 (1M context)
const DEFAULT_MODEL = 'claude-opus-4-6';
const DEFAULT_MAX_TOKENS = 1024;

let _client = null;
if (ANTHROPIC_API_KEY) {
  try {
    _client = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
    console.log('✅ Anthropic Claude AI initialized');
  } catch (e) {
    console.error('Anthropic init failed:', e.message);
  }
}

function getClient() {
  return _client;
}

function isConfigured() {
  return _client !== null;
}

/**
 * Send a single user message to Claude and return the text response.
 * Throws on errors (caller should handle or use tryComplete).
 */
async function complete({ prompt, system = null, model = DEFAULT_MODEL, maxTokens = DEFAULT_MAX_TOKENS, temperature = 1.0 }) {
  if (!_client) throw new Error('Anthropic not configured');
  const params = {
    model,
    max_tokens: maxTokens,
    temperature,
    messages: [{ role: 'user', content: prompt }],
  };
  if (system) params.system = system;
  const response = await _client.messages.create(params);
  return response.content?.[0]?.text || '';
}

/**
 * Best-effort completion — returns null on failure instead of throwing.
 * Use for non-critical AI features where the main flow should continue.
 */
async function tryComplete(opts) {
  if (!_client) return null;
  try {
    return await complete(opts);
  } catch (e) {
    console.error('AI complete failed:', e.message);
    return null;
  }
}

/**
 * Multi-turn conversation. messages = [{ role: 'user'|'assistant', content: string }]
 */
async function chat({ messages, system = null, model = DEFAULT_MODEL, maxTokens = DEFAULT_MAX_TOKENS, temperature = 1.0 }) {
  if (!_client) throw new Error('Anthropic not configured');
  const params = { model, max_tokens: maxTokens, temperature, messages };
  if (system) params.system = system;
  const response = await _client.messages.create(params);
  return response.content?.[0]?.text || '';
}

module.exports = {
  DEFAULT_MODEL,
  DEFAULT_MAX_TOKENS,
  getClient,
  isConfigured,
  complete,
  tryComplete,
  chat,
};
