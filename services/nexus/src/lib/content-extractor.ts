import { JSDOM } from 'jsdom';
import { Readability } from '@mozilla/readability';
import { crawlUrls } from '@/crawl4ai-client';
import { ScoredSource } from './source-filter';

const USER_AGENTS = [
  'Mozilla/5.0 (compatible; NexusBot/1.0; +https://github.com/nexus-research)',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
];

export interface ExtractionStats {
  attempted: number;
  succeeded: number;
  avgContentLength: number;
  failedUrls: string[];
  crawl4aiAttempts: number;
  crawl4aiSucceeded: number;
  readabilityAttempts: number;
  readabilitySucceeded: number;
}

async function tryExtract(url: string, userAgent: string, timeoutMs: number): Promise<string | null> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: { 'User-Agent': userAgent },
    });
    clearTimeout(timeout);
    if (!response.ok) return null;
    const contentType = response.headers.get('content-type') || '';
    if (!contentType.includes('text/html') && !contentType.includes('text/plain')) return null;
    const html = await response.text();
    const dom = new JSDOM(html, { url });
    const article = new Readability(dom.window.document).parse();
    if (!article?.textContent) return null;
    const text = article.textContent.replace(/\s+/g, ' ').trim();
    if (text.length < 100) return null;
    return text.substring(0, 3000);
  } catch {
    clearTimeout(timeout);
    return null;
  }
}

async function extractContentWithReadability(url: string, timeoutMs: number = 5000): Promise<string | null> {
  let result = await tryExtract(url, USER_AGENTS[0], timeoutMs);
  if (result) return result;

  await new Promise((r) => setTimeout(r, 2000));
  result = await tryExtract(url, USER_AGENTS[1], timeoutMs);
  if (result) return result;

  return tryExtract(url, USER_AGENTS[2], timeoutMs);
}

export async function extractContent(url: string, timeoutMs: number = 5000, query?: string): Promise<string | null> {
  const crawled = await crawlUrls([url], { query, timeout: timeoutMs });
  const primary = crawled[0];
  const markdown = (primary?.fit_markdown || primary?.markdown || '').trim();
  if (primary?.success && markdown.length > 100) {
    return markdown.substring(0, 5000);
  }
  return extractContentWithReadability(url, timeoutMs);
}

export async function extractContentForTopSources(
  sources: ScoredSource[],
  targetCount: number,
  signal?: AbortSignal,
  options?: { query?: string; timeoutMs?: number },
): Promise<ExtractionStats> {
  const timeoutMs = options?.timeoutMs || 20000;
  const candidates = sources.filter((source) =>
    source?.url
    && (!source.extractedContent || source.extractedContent.length <= (source.content?.length || 0)),
  );
  const maxAttempts = Math.min(candidates.length, Math.ceil(targetCount * 1.5));
  let succeeded = 0;
  let totalLength = 0;
  const failedUrls: string[] = [];
  let crawl4aiSucceeded = 0;
  let readabilitySucceeded = 0;

  if (targetCount <= 0 || maxAttempts <= 0) {
    return {
      attempted: 0,
      succeeded: 0,
      avgContentLength: 0,
      failedUrls: [],
      crawl4aiAttempts: 0,
      crawl4aiSucceeded: 0,
      readabilityAttempts: 0,
      readabilitySucceeded: 0,
    };
  }

  const attemptedSources = candidates.slice(0, maxAttempts);
  const crawlUrlsToFetch = attemptedSources.slice(0, targetCount).map((source) => source.url);
  const crawlByUrl = new Map<string, string>();

  if (!signal?.aborted && crawlUrlsToFetch.length > 0) {
    const crawlResults = await crawlUrls(crawlUrlsToFetch, {
      query: options?.query,
      timeout: timeoutMs,
    });
    for (const result of crawlResults) {
      const markdown = (result.fit_markdown || result.markdown || '').trim();
      if (result.success && markdown.length > 100) {
        crawlByUrl.set(result.url, markdown.substring(0, 5000));
      }
    }
  }

  for (const source of attemptedSources) {
    if (signal?.aborted) break;

    const crawled = crawlByUrl.get(source.url);
    if (crawled && crawled.length > (source.content?.length || 0)) {
      source.extractedContent = crawled;
      source.content = crawled;
      source.engine = source.engine ? `${source.engine}, crawl4ai` : 'crawl4ai';
      succeeded++;
      crawl4aiSucceeded++;
      totalLength += crawled.length;
      continue;
    }

    if (succeeded >= targetCount) continue;

    const fallback = await extractContentWithReadability(source.url, Math.min(timeoutMs, 7000));
    if (fallback && fallback.length > (source.content?.length || 0)) {
      source.extractedContent = fallback;
      source.content = fallback;
      succeeded++;
      readabilitySucceeded++;
      totalLength += fallback.length;
    } else {
      failedUrls.push(source.url);
    }
  }

  return {
    attempted: attemptedSources.length,
    succeeded,
    avgContentLength: succeeded > 0 ? Math.round(totalLength / succeeded) : 0,
    failedUrls,
    crawl4aiAttempts: crawlUrlsToFetch.length,
    crawl4aiSucceeded,
    readabilityAttempts: Math.max(0, attemptedSources.length - crawl4aiSucceeded),
    readabilitySucceeded,
  };
}
