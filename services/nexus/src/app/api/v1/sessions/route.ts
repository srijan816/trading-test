import { NextRequest } from 'next/server';
import { getThread } from '@/lib/storage';

function authenticateRequest(request: NextRequest): boolean {
  const apiKeys = (process.env.NEXUS_API_KEYS || '').split(',').filter(Boolean);
  if (apiKeys.length === 0) return true;
  const authHeader = request.headers.get('authorization') || '';
  const token = authHeader.replace('Bearer ', '').trim();
  return apiKeys.includes(token);
}

export async function GET(request: NextRequest) {
  if (!authenticateRequest(request)) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), {
      status: 401, headers: { 'Content-Type': 'application/json' },
    });
  }

  const { searchParams } = new URL(request.url);
  const id = searchParams.get('id');

  if (!id) {
    return new Response(JSON.stringify({ error: 'id parameter required' }), {
      status: 400, headers: { 'Content-Type': 'application/json' },
    });
  }

  const thread = getThread(id);
  if (!thread) {
    return new Response(JSON.stringify({ error: 'Session not found' }), {
      status: 404, headers: { 'Content-Type': 'application/json' },
    });
  }

  return new Response(JSON.stringify({
    session_id: thread.id,
    query: thread.query,
    mode: thread.mode,
    report: thread.report,
    sources: thread.sources.map((s: any) => ({
      url: s.url, title: s.title, snippet: s.content?.substring(0, 200),
      relevance: s.relevanceScore || null, cited: s.cited || false,
    })),
    metadata: {
      ...thread.stats,
      model: thread.model,
      output_length: thread.outputLength,
      sources_searched: thread.sourcesSearched,
      sources_cited: thread.sourcesCited,
    },
    timestamp: thread.timestamp,
  }), { headers: { 'Content-Type': 'application/json' } });
}
