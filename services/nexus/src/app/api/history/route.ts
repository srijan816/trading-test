import { NextRequest } from 'next/server';
import { getThreads, getThread, deleteThread } from '@/lib/storage';

export async function GET(request: NextRequest) {
  const id = request.nextUrl.searchParams.get('id');

  if (id) {
    const thread = getThread(id);
    if (!thread) {
      return new Response(JSON.stringify({ error: 'Thread not found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
    }
    return new Response(JSON.stringify(thread), { headers: { 'Content-Type': 'application/json' } });
  }

  const threads = getThreads();
  // Return list without full report content for efficiency
  const summary = threads.map((t) => ({
    id: t.id,
    query: t.query,
    mode: t.mode,
    model: t.model,
    outputLength: t.outputLength,
    stats: t.stats,
    timestamp: t.timestamp,
    sourceCount: t.sources?.length || 0,
  }));

  return new Response(JSON.stringify(summary), { headers: { 'Content-Type': 'application/json' } });
}

export async function DELETE(request: NextRequest) {
  const { id } = await request.json();
  if (!id) {
    return new Response(JSON.stringify({ error: 'id required' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
  }
  const ok = deleteThread(id);
  return new Response(JSON.stringify({ ok }), { headers: { 'Content-Type': 'application/json' } });
}
