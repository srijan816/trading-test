'use client';

import { useState } from 'react';

interface SourceWithMeta {
  title: string;
  url: string;
  content: string;
  cited?: boolean;
  relevanceScore?: number;
  domain?: string;
}

interface SourcesPanelProps {
  sources: SourceWithMeta[];
  report?: string;
}

function getDomain(url: string): string {
  try {
    return new URL(url).hostname.replace('www.', '');
  } catch {
    return url;
  }
}

function categorize(domain: string): string {
  if (domain.includes('reddit.com')) return 'Reddit Discussions';
  if (domain.includes('github.com')) return 'GitHub';
  if (domain.includes('stackoverflow.com') || domain.includes('stackexchange.com')) return 'Stack Overflow';
  if (domain.includes('arxiv.org')) return 'Academic Papers';
  if (domain.includes('wikipedia.org')) return 'Wikipedia';
  if (domain.includes('news.ycombinator.com')) return 'Hacker News';
  if (domain.includes('medium.com') || domain.includes('dev.to') || domain.includes('substack.com')) return 'Blogs & Articles';
  if (domain.includes('.gov')) return 'Government Sources';
  if (domain.includes('.edu')) return 'Academic Sources';
  return 'Web Sources';
}

function getFaviconUrl(domain: string): string {
  return `https://www.google.com/s2/favicons?domain=${domain}&sz=16`;
}

export default function SourcesPanel({ sources, report }: SourcesPanelProps) {
  const [showAll, setShowAll] = useState(false);

  if (sources.length === 0) return null;

  // Determine which sources are cited based on report content or cited flag
  let citedSources: SourceWithMeta[] = [];
  let allSources = sources;

  if (report) {
    const citedNumbers = new Set<number>();
    const citationRegex = /\[(\d+)\]/g;
    let match;
    while ((match = citationRegex.exec(report)) !== null) {
      citedNumbers.add(parseInt(match[1], 10));
    }
    citedSources = sources.filter((_, i) => citedNumbers.has(i + 1));
    // Also respect the cited flag
    sources.forEach((s, i) => {
      if (s.cited && !citedNumbers.has(i + 1)) {
        citedSources.push(s);
      }
    });
  } else {
    citedSources = sources.filter(s => s.cited);
    if (citedSources.length === 0) citedSources = sources;
  }

  // Group cited sources by category
  const groups: Record<string, { source: SourceWithMeta; index: number }[]> = {};
  citedSources.forEach((source) => {
    const domain = source.domain || getDomain(source.url);
    const category = categorize(domain);
    if (!groups[category]) groups[category] = [];
    const origIndex = sources.indexOf(source);
    groups[category].push({ source, index: origIndex >= 0 ? origIndex : 0 });
  });

  return (
    <div className="bg-dark-900 border border-dark-700 rounded-xl p-4">
      <h3 className="text-sm font-medium text-dark-300 mb-1 uppercase tracking-wider">
        Sources
      </h3>
      <p className="text-xs text-dark-500 mb-3">
        Cited {citedSources.length} of {sources.length} sources searched
      </p>

      <div className="space-y-4 max-h-[calc(100vh-250px)] overflow-y-auto">
        {Object.entries(groups).map(([category, items]) => (
          <div key={category}>
            <p className="text-[10px] uppercase tracking-wider text-dark-500 mb-1.5 font-medium">{category}</p>
            <div className="space-y-1">
              {items.map(({ source, index }) => {
                const domain = source.domain || getDomain(source.url);
                return (
                  <a
                    key={index}
                    href={source.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-start gap-2.5 p-2 rounded-lg hover:bg-dark-800 transition-colors group"
                  >
                    <span className="flex-shrink-0 w-5 h-5 rounded bg-dark-700 text-dark-400 text-[10px] flex items-center justify-center font-mono mt-0.5">
                      {index + 1}
                    </span>
                    <img src={getFaviconUrl(domain)} alt="" className="w-4 h-4 mt-0.5 flex-shrink-0" loading="lazy" />
                    <div className="min-w-0 flex-1">
                      <p className="text-xs text-white font-medium truncate group-hover:text-blue-400 transition-colors" title={source.title}>
                        {source.title.length > 80 ? source.title.substring(0, 80) + '...' : source.title}
                      </p>
                      <p className="text-[10px] text-dark-500 truncate">{domain}</p>
                    </div>
                  </a>
                );
              })}
            </div>
          </div>
        ))}
      </div>

      {/* Collapsible all sources section */}
      {sources.length > citedSources.length && (
        <div className="mt-3 pt-3 border-t border-dark-700/50">
          <button
            onClick={() => setShowAll(!showAll)}
            className="flex items-center gap-1.5 text-[10px] text-dark-500 hover:text-dark-300 transition-colors uppercase tracking-wider"
          >
            <svg className={`w-3 h-3 transition-transform ${showAll ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
            All sources searched ({sources.length})
          </button>
          {showAll && (
            <div className="mt-2 space-y-0.5 max-h-60 overflow-y-auto">
              {sources.map((source, i) => {
                const domain = source.domain || getDomain(source.url);
                return (
                  <a
                    key={i}
                    href={source.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center gap-2 py-1 text-[11px] text-dark-500 hover:text-dark-300 transition-colors"
                  >
                    <img src={getFaviconUrl(domain)} alt="" className="w-3 h-3 flex-shrink-0" loading="lazy" />
                    <span className="truncate">{source.title}</span>
                    {source.cited && <span className="text-green-500/60 flex-shrink-0">cited</span>}
                  </a>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
