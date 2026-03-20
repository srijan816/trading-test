'use client';

import { useState, useEffect } from 'react';
import AnswerRenderer from '@/components/AnswerRenderer';
import SourcesPanel from '@/components/SourcesPanel';
import CopyButton from '@/components/CopyButton';
import AudioPlayer from '@/components/AudioPlayer';

interface ThreadSummary {
  id: string; query: string; mode: string; model: string; outputLength: string;
  stats: { totalSearches: number; totalSourcesRead: number; passes: number; apiCalls: number; duration: number };
  timestamp: number; sourceCount: number;
}

interface FullThread extends ThreadSummary {
  report: string;
  sources: { title: string; url: string; content: string }[];
  usage: any[];
}

export default function HistoryPage() {
  const [threads, setThreads] = useState<ThreadSummary[]>([]);
  const [selected, setSelected] = useState<FullThread | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/history').then((r) => r.json()).then((data) => { setThreads(data); setLoading(false); }).catch(() => setLoading(false));
  }, []);

  const openThread = async (id: string) => {
    const res = await fetch(`/api/history?id=${id}`);
    const data = await res.json();
    setSelected(data);
  };

  const deleteThread = async (id: string) => {
    await fetch('/api/history', { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id }) });
    setThreads((prev) => prev.filter((t) => t.id !== id));
    if (selected?.id === id) setSelected(null);
  };

  const fmtDate = (ts: number) => new Date(ts).toLocaleString();
  const fmtDuration = (ms: number) => { const s = Math.round(ms / 1000); return s > 60 ? `${Math.floor(s / 60)}m ${s % 60}s` : `${s}s`; };

  return (
    <main className="min-h-screen px-4 py-6">
      <div className="max-w-6xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-3">
            <a href="/" className="text-dark-400 hover:text-white transition-colors">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" /></svg>
            </a>
            <h1 className="text-2xl font-bold text-white">Research History</h1>
          </div>
          <a href="/settings" className="text-sm text-dark-400 hover:text-white transition-colors">Usage & Settings</a>
        </div>

        {loading && <p className="text-dark-400">Loading...</p>}

        {!loading && threads.length === 0 && (
          <div className="text-center py-16">
            <p className="text-dark-400 text-lg">No research history yet</p>
            <a href="/" className="text-blue-400 hover:text-blue-300 text-sm mt-2 inline-block">Start your first research</a>
          </div>
        )}

        {!selected && threads.length > 0 && (
          <div className="space-y-2">
            {threads.map((t) => (
              <div key={t.id} className="bg-dark-900 border border-dark-700 rounded-xl p-4 hover:bg-dark-800 transition-colors">
                <div className="flex items-start justify-between gap-4">
                  <button onClick={() => openThread(t.id)} className="text-left flex-1 min-w-0">
                    <h3 className="text-sm font-medium text-white truncate">{t.query}</h3>
                    <div className="flex items-center gap-3 mt-1.5 text-xs text-dark-400 flex-wrap">
                      <span>{fmtDate(t.timestamp)}</span>
                      <span className="w-1 h-1 rounded-full bg-dark-600" />
                      <span>{t.sourceCount} sources</span>
                      <span className="w-1 h-1 rounded-full bg-dark-600" />
                      <span>{t.stats.apiCalls} API calls</span>
                      <span className="w-1 h-1 rounded-full bg-dark-600" />
                      <span>{fmtDuration(t.stats.duration)}</span>
                      <span className="px-1.5 py-0.5 rounded bg-dark-700 text-dark-400">{t.outputLength}</span>
                    </div>
                  </button>
                  <button onClick={() => deleteThread(t.id)} className="text-dark-500 hover:text-red-400 transition-colors p-1" title="Delete">
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}

        {selected && (
          <div>
            <button onClick={() => setSelected(null)} className="flex items-center gap-2 text-dark-400 hover:text-white transition-colors mb-4 text-sm">
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" /></svg>
              Back to list
            </button>
            <h2 className="text-xl font-semibold text-white mb-2">{selected.query}</h2>
            <div className="flex items-center gap-3 mb-4 text-xs text-dark-400 flex-wrap">
              <span>{fmtDate(selected.timestamp)}</span>
              <span className="w-1 h-1 rounded-full bg-dark-600" />
              <span>{selected.stats.totalSearches} searches</span>
              <span className="w-1 h-1 rounded-full bg-dark-600" />
              <span>{selected.sourceCount || selected.sources?.length || 0} sources</span>
              <span className="w-1 h-1 rounded-full bg-dark-600" />
              <span>{selected.stats.apiCalls} API calls</span>
              <span className="w-1 h-1 rounded-full bg-dark-600" />
              <span>{fmtDuration(selected.stats.duration)}</span>
            </div>
            <div className="flex items-center gap-2 mb-4">
              <CopyButton text={selected.report} />
              <AudioPlayer text={selected.report} />
            </div>
            <div className="flex gap-6">
              <div className="flex-1 min-w-0">
                <div className="bg-dark-900 border border-dark-700 rounded-xl p-6 max-h-[calc(100vh-300px)] overflow-y-auto">
                  <AnswerRenderer content={selected.report} />
                </div>
              </div>
              {selected.sources?.length > 0 && (
                <div className="w-80 flex-shrink-0 hidden lg:block">
                  <SourcesPanel sources={selected.sources} report={selected.report} />
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </main>
  );
}
