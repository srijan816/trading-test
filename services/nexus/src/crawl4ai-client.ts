const CRAWL4AI_URL = process.env.CRAWL4AI_URL || 'http://localhost:11235';

export interface CrawlResult {
  url: string;
  markdown: string;
  fit_markdown: string;
  success: boolean;
  error_message?: string;
  links: { internal: string[]; external: string[] };
  media: { images: any[]; videos: any[] };
}

function normalizeResult(result: any, fallbackUrl?: string): CrawlResult | null {
  if (!result) return null;
  return {
    url: String(result.url || fallbackUrl || ''),
    markdown: String(result.markdown || result.raw_markdown || result.markdown?.raw_markdown || ''),
    fit_markdown: String(result.fit_markdown || result.markdown?.fit_markdown || ''),
    success: Boolean(result.success),
    error_message: result.error_message ? String(result.error_message) : undefined,
    links: {
      internal: Array.isArray(result.links?.internal) ? result.links.internal.map(String) : [],
      external: Array.isArray(result.links?.external) ? result.links.external.map(String) : [],
    },
    media: {
      images: Array.isArray(result.media?.images) ? result.media.images : [],
      videos: Array.isArray(result.media?.videos) ? result.media.videos : [],
    },
  };
}

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function pollTask(taskId: string, maxAttempts: number, intervalMs: number, timeoutMs: number): Promise<CrawlResult[]> {
  for (let attempts = 0; attempts < maxAttempts; attempts++) {
    await sleep(intervalMs);
    const response = await fetch(`${CRAWL4AI_URL}/task/${taskId}`, {
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!response.ok) {
      if (response.status >= 500) continue;
      return [];
    }
    const taskData = await response.json() as any;
    if (Array.isArray(taskData?.results)) {
      return taskData.results.map((result: any) => normalizeResult(result)).filter(Boolean) as CrawlResult[];
    }
    if (taskData?.status === 'failed') return [];
  }
  return [];
}

export async function crawlUrl(
  url: string,
  options?: {
    query?: string;
    timeout?: number;
    cacheMode?: string;
  },
): Promise<CrawlResult | null> {
  try {
    const timeout = options?.timeout || 30000;
    const body: any = {
      urls: [url],
      priority: 10,
      crawler_params: {
        headless: true,
        page_timeout: timeout,
        cache_mode: options?.cacheMode || 'enabled',
      },
    };

    if (options?.query) {
      body.crawler_params.markdown_generator = {
        content_filter: {
          type: 'bm25',
          user_query: options.query,
          bm25_threshold: 1.0,
        },
      };
    } else {
      body.crawler_params.markdown_generator = {
        content_filter: {
          type: 'pruning',
          threshold: 0.48,
          threshold_type: 'fixed',
        },
      };
    }

    const response = await fetch(`${CRAWL4AI_URL}/crawl`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeout),
    });

    if (!response.ok) {
      console.error(`Crawl4AI error for ${url}: ${response.status}`);
      return null;
    }

    const data = await response.json() as any;
    if (data?.task_id && !data?.results) {
      const polled = await pollTask(String(data.task_id), 30, 1000, timeout);
      return polled[0] || null;
    }

    return normalizeResult(data?.results?.[0], url);
  } catch (err) {
    console.error(`Crawl4AI fetch failed for ${url}:`, err);
    return null;
  }
}

export async function crawlUrls(
  urls: string[],
  options?: { query?: string; timeout?: number },
): Promise<CrawlResult[]> {
  if (urls.length === 0) return [];

  try {
    const timeout = options?.timeout || 30000;
    const body: any = {
      urls,
      priority: 8,
      crawler_params: {
        headless: true,
        page_timeout: timeout,
        cache_mode: 'enabled',
      },
    };

    if (options?.query) {
      body.crawler_params.markdown_generator = {
        content_filter: {
          type: 'bm25',
          user_query: options.query,
          bm25_threshold: 1.0,
        },
      };
    } else {
      body.crawler_params.markdown_generator = {
        content_filter: {
          type: 'pruning',
          threshold: 0.48,
          threshold_type: 'fixed',
        },
      };
    }

    const response = await fetch(`${CRAWL4AI_URL}/crawl`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeout * 2),
    });

    if (!response.ok) {
      console.error(`Crawl4AI batch crawl failed: ${response.status}`);
      return [];
    }

    const data = await response.json() as any;
    if (data?.task_id && !data?.results) {
      return pollTask(String(data.task_id), 60, 1000, timeout);
    }

    return Array.isArray(data?.results)
      ? data.results.map((result: any, index: number) => normalizeResult(result, urls[index])).filter(Boolean) as CrawlResult[]
      : [];
  } catch (err) {
    console.error('Crawl4AI batch crawl failed:', err);
    return [];
  }
}
