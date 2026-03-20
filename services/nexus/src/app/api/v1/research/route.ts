import { NextRequest } from 'next/server';
import { runDeepResearch } from '@/lib/research-agent';
import { saveThread, recordUsageEntries, getThread } from '@/lib/storage';
import { OutputLength, ResearchMode } from '@/types';
import { ScoredSource } from '@/lib/source-filter';

function authenticateRequest(request: NextRequest): boolean {
  const apiKeys = (process.env.NEXUS_API_KEYS || '').split(',').filter(Boolean);
  if (apiKeys.length === 0) return true; // No keys configured = open access

  const authHeader = request.headers.get('authorization') || '';
  const token = authHeader.replace('Bearer ', '').trim();
  return apiKeys.includes(token);
}

export const maxDuration = 300;

export async function POST(request: NextRequest) {
  if (!authenticateRequest(request)) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), {
      status: 401, headers: { 'Content-Type': 'application/json' },
    });
  }

  const body = await request.json();
  const { query, mode, model, stream, session_id, max_sources } = body;

  if (!query) {
    return new Response(JSON.stringify({ error: 'query is required' }), {
      status: 400, headers: { 'Content-Type': 'application/json' },
    });
  }

  const tier = mode || 'standard';
  const selectedModel = model || process.env.DEFAULT_MODEL || 'stepfun/step-3.5-flash:free';
  const outputLength: OutputLength = body.output_length || 'medium';
  const abortController = new AbortController();

  // Load previous session context if provided
  let previousSessionContext: { sources: ScoredSource[]; report: string } | undefined;
  if (session_id) {
    const prev = getThread(session_id);
    if (prev) {
      previousSessionContext = {
        sources: prev.sources as ScoredSource[],
        report: prev.report,
      };
    }
  }

  if (stream) {
    const encoder = new TextEncoder();
    const startTime = Date.now();
    const responseStream = new ReadableStream({
      async start(controller) {
        try {
          await runDeepResearch(query, selectedModel, outputLength, (event) => {
            try {
              controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
            } catch {}

            if (event.type === 'done') {
              const result = event.data;
              try {
                saveThread({
                  id: result.id, query, mode: tier as ResearchMode, model: selectedModel, outputLength,
                  report: result.report, sources: result.sources,
                  sourcesSearched: result.sourcesSearched, sourcesCited: result.sourcesCited,
                  stats: { totalSearches: result.totalSearches, totalSourcesRead: result.totalSourcesRead, passes: result.passes, apiCalls: result.apiCalls, duration: Date.now() - startTime },
                  usage: result.usage, timestamp: result.timestamp,
                  sessionId: session_id || result.id,
                });
                recordUsageEntries(result.usage);
              } catch {}
            }
          }, abortController.signal, tier, previousSessionContext);
        } catch (error: any) {
          try { controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'error', data: error.message })}\n\n`)); } catch {}
        } finally {
          try { controller.close(); } catch {}
        }
      },
      cancel() { abortController.abort(); },
    });

    return new Response(responseStream, {
      headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' },
    });
  }

  // Non-streaming
  try {
    let finalResult: any = null;
    const startTime = Date.now();
    await runDeepResearch(query, selectedModel, outputLength, (event) => {
      if (event.type === 'done') finalResult = event.data;
    }, abortController.signal, tier, previousSessionContext);

    if (finalResult) {
      const sessionIdToUse = session_id || finalResult.id;
      saveThread({
        id: finalResult.id, query, mode: tier as ResearchMode, model: selectedModel, outputLength,
        report: finalResult.report, sources: finalResult.sources,
        sourcesSearched: finalResult.sourcesSearched, sourcesCited: finalResult.sourcesCited,
        stats: { totalSearches: finalResult.totalSearches, totalSourcesRead: finalResult.totalSourcesRead, passes: finalResult.passes, apiCalls: finalResult.apiCalls, duration: Date.now() - startTime },
        usage: finalResult.usage, timestamp: finalResult.timestamp,
        sessionId: sessionIdToUse,
      });
      recordUsageEntries(finalResult.usage);

      return new Response(JSON.stringify({
        session_id: sessionIdToUse,
        query,
        mode: tier,
        report: finalResult.report,
        sources: finalResult.sources.map((s: any) => ({
          url: s.url, title: s.title, snippet: s.content?.substring(0, 200),
          relevance: s.relevanceScore || null, cited: s.cited || false,
        })),
        follow_ups: finalResult.followUps || [],
        metadata: {
          total_searches: finalResult.totalSearches,
          total_sources_found: finalResult.sourcesSearched,
          sources_after_filtering: finalResult.sources.length,
          sources_cited: finalResult.sourcesCited,
          passes: finalResult.passes,
          duration_seconds: Math.round((Date.now() - startTime) / 1000),
          models_used: { decomposition: 'step-3.5-flash', synthesis: selectedModel },
          gaps_resolved: finalResult.gapsResolved,
          gaps_unresolvable: finalResult.gapsUnresolvable,
        },
      }), { headers: { 'Content-Type': 'application/json' } });
    }

    return new Response(JSON.stringify({ error: 'No result' }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  } catch (error: any) {
    return new Response(JSON.stringify({ error: error.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
}
