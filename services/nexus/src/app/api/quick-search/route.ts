import { NextRequest } from 'next/server';

const VANE_URL = process.env.VANE_URL || 'http://localhost:3000';

export async function POST(request: NextRequest) {
  const body = await request.json();
  const { query, stream } = body;

  if (!query) {
    return new Response(JSON.stringify({ error: 'query is required' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const providersRes = await fetch(`${VANE_URL}/api/providers`);
  const providersData = await providersRes.json();
  const provider = providersData.providers?.[0];

  if (!provider) {
    return new Response(JSON.stringify({ error: 'No provider configured in Vane' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const vaneRes = await fetch(`${VANE_URL}/api/search`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      chatModel: {
        providerId: provider.id,
        key: provider.chatModels?.[0]?.key,
      },
      embeddingModel: {
        providerId: provider.id,
        key: provider.embeddingModels?.[0]?.key || provider.chatModels?.[0]?.key,
      },
      optimizationMode: 'balanced',
      sources: ['web'],
      query,
      stream: stream || false,
    }),
  });

  const data = await vaneRes.json();
  return new Response(JSON.stringify(data), {
    headers: { 'Content-Type': 'application/json' },
  });
}
