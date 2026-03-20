import { NextRequest } from 'next/server';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const SEARXNG_URL = process.env.SEARXNG_URL || 'http://searxng:8080';
  const CRAWL4AI_URL = process.env.CRAWL4AI_URL || 'http://localhost:11235';
  const OPENROUTER_KEY = process.env.OPENROUTER_API_KEY || '';

  let searxngOk = false;
  let openrouterOk = false;
  let crawl4aiOk = false;

  try {
    const res = await fetch(`${SEARXNG_URL}/search?q=test&format=json`, { signal: AbortSignal.timeout(10000) });
    if (res.ok) {
      const body = await res.text();
      searxngOk = body.trim().startsWith('{');
    }
  } catch {}

  try {
    const res = await fetch('https://openrouter.ai/api/v1/models', {
      headers: { 'Authorization': `Bearer ${OPENROUTER_KEY}` },
      signal: AbortSignal.timeout(10000),
    });
    openrouterOk = res.ok;
  } catch {}

  try {
    const res = await fetch(`${CRAWL4AI_URL}/health`, {
      signal: AbortSignal.timeout(10000),
    });
    crawl4aiOk = res.ok;
  } catch {}

  return new Response(JSON.stringify({
    status: searxngOk && openrouterOk && crawl4aiOk ? 'ok' : 'degraded',
    searxng: searxngOk,
    openrouter: openrouterOk,
    crawl4ai: crawl4aiOk,
    timestamp: new Date().toISOString(),
  }), { headers: { 'Content-Type': 'application/json' } });
}
