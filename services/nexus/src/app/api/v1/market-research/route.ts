import { NextRequest } from 'next/server';
import { runMarketResearch } from '@/market-research';
import { MarketResearchRequest } from '@/types';

function authenticateRequest(request: NextRequest): boolean {
  const apiKeys = (process.env.NEXUS_API_KEYS || process.env.NEXUS_API_KEY || '').split(',').filter(Boolean);
  if (apiKeys.length === 0) return true;
  const authHeader = request.headers.get('authorization') || '';
  const token = authHeader.replace('Bearer ', '').trim();
  return apiKeys.includes(token);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

export const maxDuration = 120;

export async function POST(request: NextRequest) {
  if (!authenticateRequest(request)) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), {
      status: 401,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  let body: MarketResearchRequest;
  try {
    body = await request.json();
  } catch {
    return new Response(JSON.stringify({ error: 'Invalid JSON body' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  if (!body?.question?.trim()) {
    return new Response(JSON.stringify({ error: 'question is required' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const currentYesPrice = body.market_data?.current_price_yes ?? body.market_data?.current_yes_price;
  if (!body.market_data || !isFiniteNumber(currentYesPrice)) {
    return new Response(JSON.stringify({ error: 'market_data.current_price_yes or market_data.current_yes_price is required' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  if (body.ensemble_data) {
    if (!isFiniteNumber(body.ensemble_data.mu) || !isFiniteNumber(body.ensemble_data.sigma)) {
      return new Response(JSON.stringify({ error: 'ensemble_data.mu and ensemble_data.sigma must be numbers' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  try {
    const result = await runMarketResearch(body);
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error: any) {
    return new Response(JSON.stringify({
      error: error?.message || 'Market research failed',
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}
