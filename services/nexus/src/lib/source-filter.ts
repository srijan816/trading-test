import filterConfig from '@/config/source-filters.json';
import { getEmbeddings, cosineSimilarity } from './embeddings';

export interface ScoredSource {
  title: string;
  url: string;
  content: string;
  relevanceScore?: number;
  extractedContent?: string;
  cited?: boolean;
  engine?: string;
  domain?: string;
}

function getDomain(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function isNonLatin(text: string): boolean {
  if (!text) return false;
  const nonLatin = text.replace(/[\x00-\x7F]/g, '').length;
  const total = text.length;
  return total > 0 && nonLatin / total > filterConfig.blockNonLatinThreshold;
}

function normalizeSnippet(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
}

function snippetSimilarity(a: string, b: string): number {
  const wordsA = new Set(normalizeSnippet(a).split(' '));
  const wordsB = new Set(normalizeSnippet(b).split(' '));
  if (wordsA.size === 0 || wordsB.size === 0) return 0;
  let overlap = 0;
  for (const w of wordsA) {
    if (wordsB.has(w)) overlap++;
  }
  return overlap / Math.max(wordsA.size, wordsB.size);
}

export function filterSourcesByRules(sources: ScoredSource[], originalQuery: string): { kept: ScoredSource[]; removed: number } {
  const blockedSet = new Set(filterConfig.blockedDomains.map((d: string) => d.toLowerCase()));
  const seenUrls = new Set<string>();
  const kept: ScoredSource[] = [];
  let removed = 0;

  for (const source of sources) {
    const domain = getDomain(source.url).toLowerCase();
    source.domain = domain;

    // Blocked domain
    if (blockedSet.has(domain) || filterConfig.blockedDomains.some((d: string) => domain.endsWith('.' + d.toLowerCase()))) {
      removed++;
      continue;
    }

    // Duplicate URL
    if (seenUrls.has(source.url)) {
      removed++;
      continue;
    }
    seenUrls.add(source.url);

    // Empty or too-short snippet
    if (!source.content || source.content.trim().length < filterConfig.minSnippetLength) {
      removed++;
      continue;
    }

    // Non-Latin content
    if (isNonLatin(source.title + ' ' + source.content)) {
      removed++;
      continue;
    }

    // Near-duplicate content check against already-kept sources
    let isDuplicate = false;
    for (const existing of kept) {
      if (snippetSimilarity(source.content, existing.content) > 0.9) {
        isDuplicate = true;
        break;
      }
    }
    if (isDuplicate) {
      removed++;
      continue;
    }

    kept.push(source);
  }

  return { kept, removed };
}

export async function scoreSourcesByRelevance(
  sources: ScoredSource[],
  query: string,
  minScore: number = filterConfig.minRelevanceScore,
  maxSources: number = filterConfig.maxSourcesForSynthesis
): Promise<{ scored: ScoredSource[]; dropped: number }> {
  if (sources.length === 0) return { scored: [], dropped: 0 };

  try {
    const texts = [query, ...sources.map((s) => `${s.title} ${s.content}`.substring(0, 500))];
    const embeddings = await getEmbeddings(texts);

    if (embeddings.length === 0 || embeddings.length < texts.length) {
      // Embedding failed — return all sources unscored
      return { scored: sources.slice(0, maxSources), dropped: Math.max(0, sources.length - maxSources) };
    }

    const queryVec = embeddings[0];
    for (let i = 0; i < sources.length; i++) {
      sources[i].relevanceScore = cosineSimilarity(queryVec, embeddings[i + 1]);
    }

    const filtered = sources.filter((s) => (s.relevanceScore || 0) >= minScore);
    const dropped = sources.length - filtered.length;

    filtered.sort((a, b) => (b.relevanceScore || 0) - (a.relevanceScore || 0));
    return { scored: filtered.slice(0, maxSources), dropped: dropped + Math.max(0, filtered.length - maxSources) };
  } catch (err) {
    console.error('Embedding scoring failed:', err);
    return { scored: sources.slice(0, maxSources), dropped: Math.max(0, sources.length - maxSources) };
  }
}
