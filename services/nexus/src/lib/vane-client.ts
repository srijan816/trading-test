import { Source } from '@/types';
import { ScoredSource } from './source-filter';

const SEARXNG_URL = process.env.SEARXNG_URL || 'http://searxng:8080';

function deduplicateByUrl(sources: ScoredSource[]): ScoredSource[] {
  const seen = new Set<string>();
  return sources.filter((s) => {
    if (seen.has(s.url)) return false;
    seen.add(s.url);
    return true;
  });
}

export async function searchSearXNG(
  query: string,
  signal?: AbortSignal
): Promise<ScoredSource[]> {
  const results: ScoredSource[] = [];

  for (const page of [1, 2]) {
    if (signal?.aborted) break;
    try {
      const params = new URLSearchParams({
        q: query,
        format: 'json',
        categories: 'general',
        language: 'en',
        pageno: String(page),
        safesearch: '0',
      });

      const res = await fetch(`${SEARXNG_URL}/search?${params}`, { signal });
      if (!res.ok) continue;

      const data = await res.json();
      if (data.results) {
        results.push(
          ...data.results.map((r: any) => ({
            title: r.title || 'Untitled',
            url: r.url || '',
            content: r.content || '',
            engine: r.engines?.join(', ') || 'unknown',
          }))
        );
      }
    } catch {
      // Page 2 failure is non-critical
      if (page === 1) throw new Error(`SearXNG search failed for query: ${query}`);
    }
  }

  return deduplicateByUrl(results);
}

export async function searchSearXNGMultiQuery(
  queries: string[],
  signal?: AbortSignal
): Promise<ScoredSource[]> {
  const allResults = await Promise.allSettled(
    queries.map((q) => searchSearXNG(q, signal))
  );

  const combined: ScoredSource[] = [];
  for (const r of allResults) {
    if (r.status === 'fulfilled') {
      combined.push(...r.value);
    }
  }

  return deduplicateByUrl(combined);
}
