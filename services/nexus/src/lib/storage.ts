import { HistoryThread, UsageEntry } from '@/types';
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs';
import { join } from 'path';

const DATA_DIR = join(process.cwd(), 'data');
const HISTORY_FILE = join(DATA_DIR, 'history.json');
const USAGE_FILE = join(DATA_DIR, 'usage.json');

function ensureDataDir() {
  if (!existsSync(DATA_DIR)) {
    mkdirSync(DATA_DIR, { recursive: true });
  }
}

// ========== HISTORY ==========

function loadHistory(): HistoryThread[] {
  ensureDataDir();
  if (!existsSync(HISTORY_FILE)) return [];
  try {
    return JSON.parse(readFileSync(HISTORY_FILE, 'utf-8'));
  } catch {
    return [];
  }
}

function saveHistory(threads: HistoryThread[]) {
  ensureDataDir();
  writeFileSync(HISTORY_FILE, JSON.stringify(threads, null, 2));
}

export function saveThread(thread: HistoryThread) {
  const threads = loadHistory();
  threads.push(thread);
  if (threads.length > 200) threads.splice(0, threads.length - 200);
  saveHistory(threads);
}

export function getThreads(): HistoryThread[] {
  return loadHistory().reverse();
}

export function getThread(id: string): HistoryThread | null {
  return loadHistory().find((t) => t.id === id) || null;
}

export function deleteThread(id: string): boolean {
  const threads = loadHistory();
  const idx = threads.findIndex((t) => t.id === id);
  if (idx === -1) return false;
  threads.splice(idx, 1);
  saveHistory(threads);
  return true;
}

// ========== RATE LIMIT CACHE ==========

interface RateLimitInfo {
  openrouterDailyRemaining: number;
  openrouterDailyLimit: number;
  openrouterPerMinuteRemaining: number;
  openrouterPerMinuteLimit: number;
  lastUpdated: number;
}

let rateLimitCache: RateLimitInfo = {
  openrouterDailyRemaining: -1,
  openrouterDailyLimit: -1,
  openrouterPerMinuteRemaining: -1,
  openrouterPerMinuteLimit: -1,
  lastUpdated: 0,
};

export function updateRateLimits(headers: Record<string, string | null>) {
  if (headers['x-ratelimit-remaining']) {
    rateLimitCache.openrouterPerMinuteRemaining = parseInt(headers['x-ratelimit-remaining'] || '-1', 10);
  }
  if (headers['x-ratelimit-limit']) {
    rateLimitCache.openrouterPerMinuteLimit = parseInt(headers['x-ratelimit-limit'] || '-1', 10);
  }
  if (headers['x-ratelimit-remaining-tokens']) {
    rateLimitCache.openrouterDailyRemaining = parseInt(headers['x-ratelimit-remaining-tokens'] || '-1', 10);
  }
  if (headers['x-ratelimit-limit-tokens']) {
    rateLimitCache.openrouterDailyLimit = parseInt(headers['x-ratelimit-limit-tokens'] || '-1', 10);
  }
  rateLimitCache.lastUpdated = Date.now();
}

export function getRateLimits(): RateLimitInfo {
  return { ...rateLimitCache };
}

// ========== USAGE ==========

interface UsageData {
  entries: UsageEntry[];
}

function loadUsage(): UsageData {
  ensureDataDir();
  if (!existsSync(USAGE_FILE)) return { entries: [] };
  try {
    return JSON.parse(readFileSync(USAGE_FILE, 'utf-8'));
  } catch {
    return { entries: [] };
  }
}

function saveUsageData(data: UsageData) {
  ensureDataDir();
  writeFileSync(USAGE_FILE, JSON.stringify(data, null, 2));
}

export function recordUsageEntries(entries: UsageEntry[]) {
  const data = loadUsage();
  data.entries.push(...entries);
  if (data.entries.length > 10000) data.entries = data.entries.slice(-10000);
  saveUsageData(data);
}

function buildModelBreakdown(entries: UsageEntry[]) {
  const breakdown: Record<string, { requests: number; input_tokens: number; output_tokens: number; avg_latency_ms: number; latencySum: number }> = {};
  let searxngRequests = 0;
  let crawl4aiRequests = 0;

  for (const e of entries) {
    if (e.endpoint === 'searxng' || e.model === 'searxng') {
      searxngRequests++;
      continue;
    }
    if (e.endpoint === 'crawl4ai' || e.model === 'crawl4ai') {
      crawl4aiRequests++;
      continue;
    }
    if (!breakdown[e.model]) {
      breakdown[e.model] = { requests: 0, input_tokens: 0, output_tokens: 0, avg_latency_ms: 0, latencySum: 0 };
    }
    const m = breakdown[e.model];
    m.requests++;
    m.input_tokens += e.promptTokens || 0;
    m.output_tokens += e.completionTokens || 0;
    m.latencySum += e.latencyMs || 0;
    m.avg_latency_ms = Math.round(m.latencySum / m.requests);
  }

  // Clean up latencySum from output
  const cleanBreakdown: Record<string, { requests: number; input_tokens: number; output_tokens: number; avg_latency_ms?: number }> = {};
  for (const [k, v] of Object.entries(breakdown)) {
    cleanBreakdown[k] = { requests: v.requests, input_tokens: v.input_tokens, output_tokens: v.output_tokens };
    if (v.avg_latency_ms > 0) cleanBreakdown[k].avg_latency_ms = v.avg_latency_ms;
  }
  if (crawl4aiRequests > 0) cleanBreakdown['crawl4ai'] = { requests: crawl4aiRequests, input_tokens: 0, output_tokens: 0 };

  return { by_model: cleanBreakdown, searxng_requests: searxngRequests, crawl4ai_requests: crawl4aiRequests };
}

function buildPeriodStats(entries: UsageEntry[]) {
  const totalInputTokens = entries.reduce((s, e) => s + (e.promptTokens || 0), 0);
  const totalOutputTokens = entries.reduce((s, e) => s + (e.completionTokens || 0), 0);
  const latencies = entries.filter((e) => e.latencyMs).map((e) => e.latencyMs!);
  const avgLatency = latencies.length > 0 ? Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length) : 0;
  const { by_model, searxng_requests, crawl4ai_requests } = buildModelBreakdown(entries);

  return {
    total_requests: entries.length,
    total_input_tokens: totalInputTokens,
    total_output_tokens: totalOutputTokens,
    by_model,
    searxng_requests,
    crawl4ai_requests,
    avg_latency_ms: avgLatency,
  };
}

export function getUsageStats() {
  const data = loadUsage();
  const now = Date.now();
  const todayStart = new Date().setHours(0, 0, 0, 0);
  const weekStart = now - 7 * 24 * 60 * 60 * 1000;
  const monthStart = now - 30 * 24 * 60 * 60 * 1000;

  const todayEntries = data.entries.filter((e) => e.timestamp >= todayStart);
  const weekEntries = data.entries.filter((e) => e.timestamp >= weekStart);
  const monthEntries = data.entries.filter((e) => e.timestamp >= monthStart);

  // Daily breakdown for last 7 days
  const dailyBreakdown: Record<string, { requests: number; input_tokens: number; output_tokens: number }> = {};
  for (let d = 0; d < 7; d++) {
    const dayStart = new Date(now - d * 24 * 60 * 60 * 1000);
    dayStart.setHours(0, 0, 0, 0);
    const dayEnd = new Date(dayStart);
    dayEnd.setHours(23, 59, 59, 999);
    const dateKey = dayStart.toISOString().split('T')[0];
    const dayEntries = data.entries.filter((e) => e.timestamp >= dayStart.getTime() && e.timestamp <= dayEnd.getTime());
    dailyBreakdown[dateKey] = {
      requests: dayEntries.length,
      input_tokens: dayEntries.reduce((s, e) => s + (e.promptTokens || 0), 0),
      output_tokens: dayEntries.reduce((s, e) => s + (e.completionTokens || 0), 0),
    };
  }

  // Legacy compat fields
  const modelBreakdown: Record<string, { calls: number; promptTokens: number; completionTokens: number; totalTokens: number }> = {};
  for (const e of data.entries) {
    if (!modelBreakdown[e.model]) {
      modelBreakdown[e.model] = { calls: 0, promptTokens: 0, completionTokens: 0, totalTokens: 0 };
    }
    const m = modelBreakdown[e.model];
    m.calls++;
    m.promptTokens += e.promptTokens || 0;
    m.completionTokens += e.completionTokens || 0;
    m.totalTokens += e.totalTokens || 0;
  }

  return {
    today: buildPeriodStats(todayEntries),
    last_7_days: buildPeriodStats(weekEntries),
    last_30_days: buildPeriodStats(monthEntries),
    daily_breakdown: dailyBreakdown,
    rate_limit_status: {
      openrouter_daily_remaining: rateLimitCache.openrouterDailyRemaining,
      openrouter_daily_limit: rateLimitCache.openrouterDailyLimit,
      openrouter_per_minute_remaining: rateLimitCache.openrouterPerMinuteRemaining,
      openrouter_per_minute_limit: rateLimitCache.openrouterPerMinuteLimit,
    },
    // Legacy compat
    allTime: {
      apiCalls: data.entries.length,
      totalTokens: data.entries.reduce((s, e) => s + (e.totalTokens || 0), 0),
    },
    thisWeek: {
      apiCalls: weekEntries.length,
      totalTokens: weekEntries.reduce((s, e) => s + (e.totalTokens || 0), 0),
    },
    modelBreakdown,
    recentEntries: data.entries.slice(-50).reverse(),
  };
}
