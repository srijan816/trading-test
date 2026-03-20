import { NextRequest } from 'next/server';

const TTS_URL = process.env.TTS_URL || 'http://host.docker.internal:3002';

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const playlist = searchParams.get('playlist') || 'default';
  try {
    const res = await fetch(`${TTS_URL}/playlist?playlist=${encodeURIComponent(playlist)}`);
    if (!res.ok) return Response.json({ items: [] });
    return Response.json(await res.json());
  } catch {
    return Response.json({ items: [] });
  }
}

export async function POST(request: NextRequest) {
  const body = await request.json();
  // Ensure playlist_id is forwarded (defaults to "default" on TTS side)
  try {
    const res = await fetch(`${TTS_URL}/playlist/add`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.text().catch(() => 'Failed to add to playlist');
      return Response.json({ error: err }, { status: 502 });
    }
    return Response.json(await res.json());
  } catch {
    return Response.json({ error: 'TTS service unavailable' }, { status: 503 });
  }
}

export async function DELETE(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const id = searchParams.get('id');
  const playlist = searchParams.get('playlist') || 'default';
  if (!id) return Response.json({ error: 'id required' }, { status: 400 });
  try {
    const res = await fetch(
      `${TTS_URL}/playlist/${id}?playlist=${encodeURIComponent(playlist)}`,
      { method: 'DELETE' },
    );
    if (!res.ok) return Response.json({ error: 'Not found' }, { status: 404 });
    return Response.json(await res.json());
  } catch {
    return Response.json({ error: 'TTS service unavailable' }, { status: 503 });
  }
}
