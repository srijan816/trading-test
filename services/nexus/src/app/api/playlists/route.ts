import { NextRequest } from 'next/server';

const TTS_URL = process.env.TTS_URL || 'http://host.docker.internal:3002';

// GET /api/playlists — list all playlists
export async function GET() {
  try {
    const res = await fetch(`${TTS_URL}/playlists`);
    if (!res.ok) return Response.json({ playlists: [] });
    return Response.json(await res.json());
  } catch {
    return Response.json({ playlists: [] });
  }
}

// POST /api/playlists — create a new playlist
// Body: { name: string, playlist_id?: string }
export async function POST(request: NextRequest) {
  const body = await request.json();
  try {
    const res = await fetch(`${TTS_URL}/playlists`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.text().catch(() => 'Failed to create playlist');
      return Response.json({ error: err }, { status: res.status >= 400 ? res.status : 502 });
    }
    return Response.json(await res.json());
  } catch {
    return Response.json({ error: 'TTS service unavailable' }, { status: 503 });
  }
}

// DELETE /api/playlists?id=<playlist_id> — delete a playlist
export async function DELETE(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const id = searchParams.get('id');
  if (!id) return Response.json({ error: 'id required' }, { status: 400 });
  try {
    const res = await fetch(`${TTS_URL}/playlists/${encodeURIComponent(id)}`, {
      method: 'DELETE',
    });
    if (!res.ok) {
      const err = await res.text().catch(() => 'Failed to delete playlist');
      return Response.json({ error: err }, { status: res.status >= 400 ? res.status : 502 });
    }
    return Response.json(await res.json());
  } catch {
    return Response.json({ error: 'TTS service unavailable' }, { status: 503 });
  }
}
