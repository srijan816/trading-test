'use client';

import { useState, useEffect } from 'react';

interface PeriodStats {
  total_requests: number;
  total_input_tokens: number;
  total_output_tokens: number;
  by_model: Record<string, { requests: number; input_tokens: number; output_tokens: number; avg_latency_ms?: number }>;
  searxng_requests: number;
  crawl4ai_requests: number;
  avg_latency_ms: number;
}

interface UsageStats {
  today: PeriodStats;
  last_7_days: PeriodStats;
  last_30_days: PeriodStats;
  daily_breakdown: Record<string, { requests: number; input_tokens: number; output_tokens: number }>;
  rate_limit_status: {
    openrouter_daily_remaining: number;
    openrouter_daily_limit: number;
    openrouter_per_minute_remaining: number;
    openrouter_per_minute_limit: number;
  };
  recentEntries: {
    model: string;
    purpose: string;
    totalTokens: number;
    promptTokens: number;
    completionTokens: number;
    timestamp: number;
    latencyMs?: number;
    status?: string;
    endpoint?: string;
    sessionId?: string;
  }[];
  // Legacy compat
  allTime: { apiCalls: number; totalTokens: number };
  thisWeek: { apiCalls: number; totalTokens: number };
  modelBreakdown: Record<string, { calls: number; promptTokens: number; completionTokens: number; totalTokens: number }>;
}

function RateLimitBar({ label, remaining, limit }: { label: string; remaining: number; limit: number }) {
  if (limit <= 0) return null;
  const pct = Math.max(0, Math.min(100, (remaining / limit) * 100));
  const color = pct > 50 ? 'bg-green-500' : pct > 20 ? 'bg-yellow-500' : 'bg-red-500';
  const textColor = pct > 50 ? 'text-green-400' : pct > 20 ? 'text-yellow-400' : 'text-red-400';

  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs">
        <span className="text-dark-400">{label}</span>
        <span className={textColor}>{remaining.toLocaleString()} / {limit.toLocaleString()}</span>
      </div>
      <div className="h-2 bg-dark-800 rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full transition-all`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function DailyChart({ breakdown }: { breakdown: Record<string, { requests: number; input_tokens: number; output_tokens: number }> }) {
  const days = Object.entries(breakdown).sort(([a], [b]) => a.localeCompare(b));
  const maxReqs = Math.max(...days.map(([, d]) => d.requests), 1);

  return (
    <div className="flex items-end gap-1.5 h-32">
      {days.map(([date, data]) => {
        const height = Math.max(4, (data.requests / maxReqs) * 100);
        const dayLabel = new Date(date + 'T00:00:00').toLocaleDateString('en', { weekday: 'short' });
        return (
          <div key={date} className="flex-1 flex flex-col items-center gap-1">
            <div className="w-full flex flex-col items-center justify-end" style={{ height: '100px' }}>
              <div
                className="w-full bg-blue-500/70 rounded-t hover:bg-blue-400/70 transition-colors cursor-default relative group"
                style={{ height: `${height}%`, minHeight: '4px' }}
              >
                <div className="absolute -top-8 left-1/2 -translate-x-1/2 bg-dark-800 text-white text-[10px] px-1.5 py-0.5 rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap pointer-events-none z-10">
                  {data.requests} calls / {(data.input_tokens + data.output_tokens).toLocaleString()} tok
                </div>
              </div>
            </div>
            <span className="text-[10px] text-dark-500">{dayLabel}</span>
            <span className="text-[10px] text-dark-600">{data.requests}</span>
          </div>
        );
      })}
    </div>
  );
}

export default function SettingsPage() {
  const [stats, setStats] = useState<UsageStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/usage').then((r) => r.json()).then((d) => { setStats(d); setLoading(false); }).catch(() => setLoading(false));
  }, []);

  const fmtNum = (n: number) => n.toLocaleString();
  const fmtDate = (ts: number) => new Date(ts).toLocaleString();

  return (
    <main className="min-h-screen px-4 py-6">
      <div className="max-w-4xl mx-auto">
        <div className="flex items-center justify-between mb-8">
          <div className="flex items-center gap-3">
            <a href="/" className="text-dark-400 hover:text-white transition-colors">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" /></svg>
            </a>
            <h1 className="text-2xl font-bold text-white">Usage & Settings</h1>
          </div>
          <a href="/history" className="text-sm text-dark-400 hover:text-white transition-colors">History</a>
        </div>

        {loading && <p className="text-dark-400">Loading usage data...</p>}

        {stats && (
          <>
            {/* Summary cards */}
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
              <div className="bg-dark-900 border border-dark-700 rounded-xl p-5">
                <p className="text-xs text-dark-400 uppercase tracking-wider mb-1">Today</p>
                <p className="text-2xl font-bold text-white">{fmtNum(stats.today.total_requests)}</p>
                <p className="text-sm text-dark-400">API calls</p>
                <div className="mt-2 flex gap-3 text-xs text-dark-500">
                  <span>{fmtNum(stats.today.total_input_tokens)} in</span>
                  <span>{fmtNum(stats.today.total_output_tokens)} out</span>
                </div>
                {stats.today.avg_latency_ms > 0 && (
                  <p className="text-xs text-dark-500 mt-1">Avg latency: {fmtNum(stats.today.avg_latency_ms)}ms</p>
                )}
              </div>
              <div className="bg-dark-900 border border-dark-700 rounded-xl p-5">
                <p className="text-xs text-dark-400 uppercase tracking-wider mb-1">Last 7 Days</p>
                <p className="text-2xl font-bold text-white">{fmtNum(stats.last_7_days.total_requests)}</p>
                <p className="text-sm text-dark-400">API calls</p>
                <div className="mt-2 flex gap-3 text-xs text-dark-500">
                  <span>{fmtNum(stats.last_7_days.total_input_tokens)} in</span>
                  <span>{fmtNum(stats.last_7_days.total_output_tokens)} out</span>
                </div>
              </div>
              <div className="bg-dark-900 border border-dark-700 rounded-xl p-5">
                <p className="text-xs text-dark-400 uppercase tracking-wider mb-1">Last 30 Days</p>
                <p className="text-2xl font-bold text-white">{fmtNum(stats.last_30_days.total_requests)}</p>
                <p className="text-sm text-dark-400">API calls</p>
                <div className="mt-2 flex gap-3 text-xs text-dark-500">
                  <span>{fmtNum(stats.last_30_days.total_input_tokens)} in</span>
                  <span>{fmtNum(stats.last_30_days.total_output_tokens)} out</span>
                </div>
              </div>
            </div>

            {/* Daily bar chart */}
            {stats.daily_breakdown && Object.keys(stats.daily_breakdown).length > 0 && (
              <div className="bg-dark-900 border border-dark-700 rounded-xl p-5 mb-6">
                <h2 className="text-sm font-medium text-dark-300 uppercase tracking-wider mb-4">Daily API Calls (Last 7 Days)</h2>
                <DailyChart breakdown={stats.daily_breakdown} />
              </div>
            )}

            {/* Rate Limits */}
            <div className="bg-dark-900 border border-dark-700 rounded-xl p-5 mb-6">
              <h2 className="text-sm font-medium text-dark-300 uppercase tracking-wider mb-4">Rate Limit Status</h2>
              <div className="space-y-3">
                <RateLimitBar
                  label="OpenRouter Requests/min"
                  remaining={stats.rate_limit_status.openrouter_per_minute_remaining}
                  limit={stats.rate_limit_status.openrouter_per_minute_limit}
                />
                <RateLimitBar
                  label="OpenRouter Tokens/day"
                  remaining={stats.rate_limit_status.openrouter_daily_remaining}
                  limit={stats.rate_limit_status.openrouter_daily_limit}
                />
              </div>
              {stats.rate_limit_status.openrouter_daily_remaining === -1 && stats.rate_limit_status.openrouter_per_minute_remaining === -1 && (
                <p className="text-xs text-dark-500 mt-3">Rate limit data will appear after the first research query.</p>
              )}
            </div>

            {/* Model breakdown */}
            <div className="bg-dark-900 border border-dark-700 rounded-xl p-5 mb-6">
              <h2 className="text-sm font-medium text-dark-300 uppercase tracking-wider mb-4">Usage by Model (Last 7 Days)</h2>
              {Object.keys(stats.last_7_days.by_model).length === 0 ? (
                <p className="text-dark-500 text-sm">No usage recorded yet</p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-xs text-dark-400 uppercase">
                        <th className="pb-3 pr-4">Model</th>
                        <th className="pb-3 pr-4 text-right">Calls</th>
                        <th className="pb-3 pr-4 text-right">Input Tokens</th>
                        <th className="pb-3 pr-4 text-right">Output Tokens</th>
                        <th className="pb-3 text-right">Avg Latency</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(stats.last_7_days.by_model).sort((a, b) => b[1].requests - a[1].requests).map(([model, data]) => (
                        <tr key={model} className="border-t border-dark-700/50">
                          <td className="py-2.5 pr-4 text-white font-mono text-xs">{model}</td>
                          <td className="py-2.5 pr-4 text-right text-dark-300">{fmtNum(data.requests)}</td>
                          <td className="py-2.5 pr-4 text-right text-dark-300">{fmtNum(data.input_tokens)}</td>
                          <td className="py-2.5 pr-4 text-right text-dark-300">{fmtNum(data.output_tokens)}</td>
                          <td className="py-2.5 text-right text-dark-400 text-xs">
                            {data.avg_latency_ms ? `${fmtNum(data.avg_latency_ms)}ms` : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {(stats.last_7_days.searxng_requests > 0 || stats.last_7_days.crawl4ai_requests > 0) && (
                <p className="text-xs text-dark-500 mt-2">
                  + {fmtNum(stats.last_7_days.searxng_requests)} SearXNG searches, {fmtNum(stats.last_7_days.crawl4ai_requests)} Crawl4AI extraction batches
                </p>
              )}
            </div>

            {/* Recent API calls */}
            <div className="bg-dark-900 border border-dark-700 rounded-xl p-5 mb-6">
              <h2 className="text-sm font-medium text-dark-300 uppercase tracking-wider mb-4">Recent API Calls</h2>
              {stats.recentEntries.length === 0 ? (
                <p className="text-dark-500 text-sm">No API calls yet</p>
              ) : (
                <div className="space-y-1.5 max-h-96 overflow-y-auto">
                  {stats.recentEntries.map((e, i) => (
                    <div key={i} className="flex items-center justify-between py-1.5 border-b border-dark-700/30 last:border-0">
                      <div className="flex items-center gap-3 min-w-0">
                        <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${e.status === 'error' ? 'bg-red-500' : 'bg-green-500'}`} />
                        <span className="text-xs font-mono text-dark-400 truncate max-w-[140px]">{e.model}</span>
                        <span className="text-xs text-dark-500 truncate max-w-[180px]">{e.purpose}</span>
                      </div>
                      <div className="flex items-center gap-3 text-xs text-dark-400 flex-shrink-0">
                        {e.latencyMs && <span className="text-dark-500">{fmtNum(e.latencyMs)}ms</span>}
                        <span>{fmtNum(e.totalTokens)} tok</span>
                        <span className="text-dark-600">{fmtDate(e.timestamp)}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Services */}
            <div className="bg-dark-900 border border-dark-700 rounded-xl p-5">
              <h2 className="text-sm font-medium text-dark-300 uppercase tracking-wider mb-3">Services</h2>
              <div className="space-y-2 text-sm">
                <div className="flex items-center justify-between">
                  <span className="text-dark-300">Chatterbox TTS</span>
                  <span className="text-xs text-dark-500">Start: <code className="bg-dark-800 px-1.5 py-0.5 rounded">cd tts-service && ./start.sh</code></span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-dark-300">SearXNG</span>
                  <span className="text-xs text-dark-500">http://localhost:8081</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-dark-300">Crawl4AI</span>
                  <span className="text-xs text-dark-500">http://localhost:11235</span>
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </main>
  );
}
