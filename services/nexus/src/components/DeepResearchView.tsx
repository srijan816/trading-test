'use client';

import { useState, useEffect, useRef, useCallback } from 'react';
import { StreamEvent, Source, OutputLength } from '@/types';
import ResearchProgress from './ResearchProgress';
import SourcesPanel from './SourcesPanel';
import AnswerRenderer from './AnswerRenderer';
import CopyButton from './CopyButton';
import AudioPlayer from './AudioPlayer';

interface DeepResearchViewProps {
  query: string;
  model: string;
  outputLength: OutputLength;
  mode?: string;
  onCancel: () => void;
  onComplete?: () => void;
  onFollowUp?: (q: string) => void;
}

export default function DeepResearchView({ query, model, outputLength, mode, onCancel, onComplete, onFollowUp }: DeepResearchViewProps) {
  const [events, setEvents] = useState<StreamEvent[]>([]);
  const [answer, setAnswer] = useState('');
  const [sources, setSources] = useState<any[]>([]);
  const [isComplete, setIsComplete] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [stats, setStats] = useState<{
    totalSearches: number; totalSourcesRead: number; passes: number; apiCalls: number;
    sourcesSearched?: number; sourcesCited?: number;
  } | null>(null);
  const [followUps, setFollowUps] = useState<string[]>([]);
  const [elapsedTime, setElapsedTime] = useState(0);
  const answerRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  const timerRef = useRef<NodeJS.Timeout | null>(null);

  const handleCancel = useCallback(() => {
    abortRef.current?.abort();
    if (timerRef.current) clearInterval(timerRef.current);
    onCancel();
  }, [onCancel]);

  useEffect(() => {
    const abortController = new AbortController();
    abortRef.current = abortController;
    const startTime = Date.now();
    timerRef.current = setInterval(() => setElapsedTime(Math.floor((Date.now() - startTime) / 1000)), 1000);

    const run = async () => {
      try {
        const res = await fetch('/api/research', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query, model, stream: true, outputLength, mode: mode || 'deep' }),
          signal: abortController.signal,
        });
        if (!res.ok) { setError(`Research failed (${res.status})`); return; }
        const reader = res.body?.getReader();
        if (!reader) { setError('No stream'); return; }

        const decoder = new TextDecoder();
        let buffer = '';
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';
          for (const line of lines) {
            if (!line.trim()) continue;
            try {
              const event: StreamEvent = JSON.parse(line);
              if (event.type === 'token') { setAnswer((prev) => prev + event.data); }
              else if (event.type === 'done') {
                const r = event.data;
                setSources(r.sources || []);
                setStats({
                  totalSearches: r.totalSearches, totalSourcesRead: r.totalSourcesRead,
                  passes: r.passes, apiCalls: r.apiCalls || 0,
                  sourcesSearched: r.sourcesSearched, sourcesCited: r.sourcesCited,
                });
                setIsComplete(true);
                onComplete?.();
                // Follow-ups are now separate from the report
                if (Array.isArray(r.followUps)) {
                  setFollowUps(r.followUps);
                } else {
                  // Fallback: try to parse from report
                  try {
                    const m = r.report?.match(/<<<FOLLOW_UPS>>>(.*?)<<<END_FOLLOW_UPS>>>/s);
                    if (m) setFollowUps(JSON.parse(m[1]));
                  } catch {}
                }
              } else if (event.type === 'error') { setError(event.data); }
              else { setEvents((prev) => [...prev, event]); }
            } catch {}
          }
        }
      } catch (err: any) {
        if (err.name !== 'AbortError') setError(err.message || 'Connection error');
      } finally {
        if (timerRef.current) clearInterval(timerRef.current);
      }
    };
    run();
    return () => { abortController.abort(); if (timerRef.current) clearInterval(timerRef.current); };
  }, [query, model, outputLength, mode]);

  useEffect(() => {
    if (answerRef.current && !isComplete) answerRef.current.scrollTop = answerRef.current.scrollHeight;
  }, [answer, isComplete]);

  const fmt = (s: number) => { const m = Math.floor(s / 60); return m > 0 ? `${m}m ${s % 60}s` : `${s}s`; };

  return (
    <div className="w-full max-w-6xl mx-auto mt-4">
      {/* Timer + cancel */}
      {!isComplete && !error && (
        <div className="flex items-center justify-between max-w-3xl mx-auto mb-3">
          <div className="flex items-center gap-2 text-sm text-dark-400">
            <span className="inline-block w-2 h-2 rounded-full bg-blue-500 animate-pulse-dot" />
            Researching... {fmt(elapsedTime)}
          </div>
          <button onClick={handleCancel} className="px-3 py-1.5 rounded-lg bg-red-900/30 border border-red-800/50 text-red-400 text-sm hover:bg-red-900/50 transition-colors">Cancel</button>
        </div>
      )}

      {error && (
        <div className="w-full max-w-3xl mx-auto mb-4">
          <div className="bg-red-900/20 border border-red-800 rounded-xl p-4">
            <p className="text-red-400 text-sm font-medium">Error</p>
            <p className="text-red-300 text-sm mt-1">{error}</p>
          </div>
        </div>
      )}

      {/* Collapsible progress */}
      {events.length > 0 && <ResearchProgress events={events} isComplete={isComplete} />}

      {/* Report + sources */}
      {answer && (
        <div className="flex gap-6 max-w-6xl mx-auto">
          <div className="flex-1 min-w-0">
            {/* Stats + action bar */}
            {stats && (
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-3 text-xs text-dark-400 flex-wrap">
                  <span>{stats.totalSearches} searches</span>
                  <span className="w-1 h-1 rounded-full bg-dark-600" />
                  {stats.sourcesCited != null ? (
                    <span>{stats.sourcesCited} cited / {stats.sourcesSearched || stats.totalSourcesRead} searched</span>
                  ) : (
                    <span>{stats.totalSourcesRead} sources</span>
                  )}
                  <span className="w-1 h-1 rounded-full bg-dark-600" />
                  <span>{stats.passes} pass{stats.passes !== 1 ? 'es' : ''}</span>
                  <span className="w-1 h-1 rounded-full bg-dark-600" />
                  <span>{stats.apiCalls} API calls</span>
                  <span className="w-1 h-1 rounded-full bg-dark-600" />
                  <span>{fmt(elapsedTime)}</span>
                </div>
                {isComplete && (
                  <div className="flex items-center gap-2">
                    <CopyButton text={answer} />
                    <AudioPlayer text={answer} title={query} />
                  </div>
                )}
              </div>
            )}

            <div ref={answerRef} className="bg-dark-900 border border-dark-700 rounded-xl p-6 max-h-[calc(100vh-300px)] overflow-y-auto">
              <AnswerRenderer content={answer} />
              {!isComplete && <span className="inline-block w-2 h-5 bg-blue-500 animate-pulse ml-0.5" />}
            </div>

            {followUps.length > 0 && isComplete && (
              <div className="mt-4 space-y-2">
                <p className="text-xs text-dark-400 uppercase tracking-wider">Related questions</p>
                {followUps.map((q, i) => (
                  <button key={i} onClick={() => {
                    if (onFollowUp) { onFollowUp(q); } else { window.location.href = `/?q=${encodeURIComponent(q)}&mode=deep`; }
                  }}
                    className="block w-full text-left px-4 py-2.5 rounded-lg bg-dark-900 border border-dark-700 text-sm text-dark-200 hover:bg-dark-800 hover:border-dark-500 transition-colors">{q}</button>
                ))}
              </div>
            )}
          </div>
          <div className="w-80 flex-shrink-0 hidden lg:block">
            <SourcesPanel sources={sources} report={answer} />
          </div>
        </div>
      )}
    </div>
  );
}
