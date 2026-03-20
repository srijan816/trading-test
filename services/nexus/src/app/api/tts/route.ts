import { NextRequest } from 'next/server';

const TTS_URL = process.env.TTS_URL || 'http://host.docker.internal:3002';

export async function POST(request: NextRequest) {
  const { text, voice } = await request.json();

  if (!text) {
    return new Response(JSON.stringify({ error: 'text is required' }), {
      status: 400, headers: { 'Content-Type': 'application/json' },
    });
  }

  try {
    const res = await fetch(`${TTS_URL}/synthesize`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: text.substring(0, 10000), voice: voice || 'male' }),
    });

    if (!res.ok) {
      const err = await res.text().catch(() => 'TTS service error');
      return new Response(JSON.stringify({ error: err }), {
        status: 502, headers: { 'Content-Type': 'application/json' },
      });
    }

    const audioData = await res.arrayBuffer();
    const contentType = res.headers.get('Content-Type') || 'audio/wav';
    return new Response(audioData, {
      headers: { 'Content-Type': contentType, 'Content-Length': String(audioData.byteLength) },
    });
  } catch {
    return new Response(
      JSON.stringify({ error: 'TTS service unavailable. Make sure it\'s running on port 3002.' }),
      { status: 503, headers: { 'Content-Type': 'application/json' } },
    );
  }
}
