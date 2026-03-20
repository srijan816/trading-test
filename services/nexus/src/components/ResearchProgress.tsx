'use client';

import { useState } from 'react';
import { StreamEvent } from '@/types';

interface ResearchProgressProps {
  events: StreamEvent[];
  isComplete: boolean;
}

function renderEvent(event: StreamEvent, i: number, isLatest: boolean, isComplete: boolean) {
  const key = `${event.type}-${i}`;

  if (event.type === 'status') {
    const { step, label, detail } = event.data;
    const active = isLatest && !isComplete;
    return (
      <div key={key} className="flex items-start gap-3 py-1">
        <div className="mt-0.5 flex-shrink-0">
          {active ? (
            <div className="w-4 h-4 rounded-full bg-blue-500/20 flex items-center justify-center">
              <div className="w-2 h-2 rounded-full bg-blue-500 animate-pulse-dot" />
            </div>
          ) : (
            <div className="w-4 h-4 rounded-full bg-green-500/20 flex items-center justify-center">
              <svg className="w-2.5 h-2.5 text-green-500" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
              </svg>
            </div>
          )}
        </div>
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            {step && <span className="text-[10px] px-1.5 py-0.5 rounded bg-dark-700 text-dark-400 font-mono">Step {step}</span>}
            <span className="text-sm font-medium text-white">{label}</span>
          </div>
          <p className="text-xs text-dark-400 mt-0.5">{detail}</p>
        </div>
      </div>
    );
  }

  if (event.type === 'sub_topic') {
    const { topic, status, index, total, sourceCount } = event.data;
    if (status === 'searching') {
      return (
        <div key={key} className="flex items-center gap-3 py-0.5 pl-7">
          <div className="w-3 h-3 rounded-full bg-blue-500 animate-pulse-dot flex-shrink-0" />
          <span className="text-sm text-dark-200"><span className="text-dark-500">[{index}/{total}]</span> Searching: {topic}</span>
        </div>
      );
    }
    if (status === 'complete') {
      return (
        <div key={key} className="flex items-center gap-3 py-0.5 pl-7">
          <svg className="w-3 h-3 text-green-500 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
          </svg>
          <span className="text-sm text-dark-300"><span className="text-dark-500">[{index}/{total}]</span> {topic}{sourceCount != null && <span className="text-green-500/70 ml-1">({sourceCount} sources)</span>}</span>
        </div>
      );
    }
    return null;
  }

  if (event.type === 'search_progress') {
    const { subTopic, query, engine, resultCount } = event.data;
    if (query === 'all') {
      return (
        <div key={key} className="flex items-center gap-3 py-0.5 pl-12">
          <span className="text-xs text-dark-500">{engine}: {resultCount} results combined</span>
        </div>
      );
    }
    return (
      <div key={key} className="flex items-center gap-3 py-0.5 pl-12">
        <span className="text-xs text-dark-500 font-mono truncate max-w-[400px]">{engine}: &ldquo;{query}&rdquo;</span>
      </div>
    );
  }

  if (event.type === 'filter_progress') {
    const { totalRaw, afterRules, afterRelevance, topRelevanceScore, bottomRelevanceScore } = event.data;
    return (
      <div key={key} className="flex items-start gap-3 py-1 pl-7">
        <svg className="w-3 h-3 text-blue-400 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
          <path fillRule="evenodd" d="M3 3a1 1 0 011-1h12a1 1 0 011 1v3a1 1 0 01-.293.707L12 11.414V15a1 1 0 01-.293.707l-2 2A1 1 0 018 17v-5.586L3.293 6.707A1 1 0 013 6V3z" clipRule="evenodd" />
        </svg>
        <div className="text-xs text-dark-400">
          <span className="text-dark-300 font-medium">Filtered:</span> {totalRaw} raw → {afterRules} (rules) → {afterRelevance} (relevance)
          {topRelevanceScore > 0 && <span className="ml-1 text-dark-500">scores: {topRelevanceScore}–{bottomRelevanceScore}</span>}
        </div>
      </div>
    );
  }

  if (event.type === 'extraction_progress') {
    const { attempted, succeeded, avgContentLength } = event.data;
    return (
      <div key={key} className="flex items-center gap-3 py-0.5 pl-7">
        <span className="text-xs text-dark-400">
          Content extracted: {succeeded}/{attempted} pages (avg {avgContentLength} chars)
        </span>
      </div>
    );
  }

  if (event.type === 'detail') {
    return <div key={key} className="flex items-center gap-3 py-0.5 pl-12"><span className="text-xs text-dark-500">{event.data.message}</span></div>;
  }

  if (event.type === 'sources') {
    const { count, searxng, crawl4ai } = event.data;
    return <div key={key} className="flex items-center gap-3 py-0.5 pl-12"><span className="text-xs text-dark-500">{count} sources (SearXNG: {searxng || 0}, Crawl4AI enriched: {crawl4ai || 0})</span></div>;
  }

  if (event.type === 'gap_analysis') {
    const { pass, gapsFound, gapsResolved, gapsUnresolvable, gapsPending, gaps } = event.data;
    return (
      <div key={key} className="flex items-start gap-3 py-1 pl-7">
        <span className="text-yellow-500 flex-shrink-0 mt-0.5">!</span>
        <div>
          <span className="text-sm text-yellow-400 font-medium">
            Pass {pass || ''}: {gapsPending || gaps?.length || 0} gap(s) to fill
            {gapsUnresolvable > 0 && <span className="text-dark-500 font-normal"> ({gapsUnresolvable} unresolvable)</span>}
          </span>
          {gaps && (
            <ul className="mt-1 space-y-0.5">
              {gaps.map((gap: string, gi: number) => (
                <li key={gi} className="text-xs text-yellow-400/70">- {gap}</li>
              ))}
            </ul>
          )}
        </div>
      </div>
    );
  }

  return null;
}

export default function ResearchProgress({ events, isComplete }: ResearchProgressProps) {
  const [expanded, setExpanded] = useState(false);

  if (events.length === 0) return null;

  const latestStatusIdx = events.findLastIndex((e) => e.type === 'status');
  const latestStatus = latestStatusIdx >= 0 ? events[latestStatusIdx] : null;

  // Find filter/extraction progress for summary
  const filterEvent = events.find((e) => e.type === 'filter_progress');
  const extractionEvent = events.find((e) => e.type === 'extraction_progress');

  return (
    <div className="w-full max-w-3xl mx-auto mb-4">
      <div className="bg-dark-900 border border-dark-700 rounded-xl overflow-hidden">
        <button
          onClick={() => setExpanded(!expanded)}
          className="w-full flex items-center justify-between px-4 py-3 hover:bg-dark-800/50 transition-colors"
        >
          <div className="flex items-center gap-2 min-w-0">
            <h3 className="text-xs font-medium text-dark-400 uppercase tracking-wider">Research Progress</h3>
            {!isComplete && <span className="inline-block w-2 h-2 rounded-full bg-blue-500 animate-pulse-dot" />}
          </div>
          <div className="flex items-center gap-3">
            {!expanded && latestStatus && (
              <span className="text-xs text-dark-400 truncate max-w-[300px]">
                {latestStatus.data.label}: {latestStatus.data.detail}
              </span>
            )}
            {!expanded && filterEvent && (
              <span className="text-[10px] text-dark-500">
                {filterEvent.data.afterRelevance} sources
              </span>
            )}
            <svg className={`w-4 h-4 text-dark-500 transition-transform ${expanded ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </div>
        </button>

        {expanded && (
          <div className="px-4 pb-4 space-y-1 border-t border-dark-700/50 pt-3">
            {events.map((event, i) =>
              renderEvent(event, i, i === events.length - 1, isComplete)
            )}
          </div>
        )}
      </div>
    </div>
  );
}
