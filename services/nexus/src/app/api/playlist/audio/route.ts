import { NextRequest } from 'next/server';

const TTS_URL = process.env.TTS_URL || 'http://host.docker.internal:3002';

export async function GET(request: NextRequest) {
  const file = request.nextUrl.searchParams.get('file');
  if (!file || !/^[\w-]+\.(wav|mp3)$/.test(file)) {
    return new Response('Invalid filename', { status: 400 });
  }

  try {
    const res = await fetch(`${TTS_URL}/playlist/audio/${file}`);
    if (!res.ok) return new Response('Not found', { status: 404 });
    const data = await res.arrayBuffer();
    const contentType = file.endsWith('.wav') ? 'audio/wav' : 'audio/mpeg';
    return new Response(data, {
      headers: { 'Content-Type': contentType, 'Content-Length': String(data.byteLength) },
    });
  } catch {
    return new Response('TTS service unavailable', { status: 503 });
  }
}
