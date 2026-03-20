'use client';

import { useState, useRef } from 'react';

interface AudioPlayerProps {
  text: string;
  title?: string;
}

export default function AudioPlayer({ text, title }: AudioPlayerProps) {
  const [loading, setLoading] = useState(false);
  const [audioUrl, setAudioUrl] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [playing, setPlaying] = useState(false);
  const [addingToPlaylist, setAddingToPlaylist] = useState(false);
  const [addedToPlaylist, setAddedToPlaylist] = useState(false);
  const audioRef = useRef<HTMLAudioElement>(null);

  const cleanText = (t: string) =>
    t.replace(/<<<FOLLOW_UPS>>>.*?<<<END_FOLLOW_UPS>>>/s, '')
      .replace(/\[(\d+)\]/g, '')
      .replace(/#{1,6}\s/g, '')
      .replace(/\*{1,2}([^*]+)\*{1,2}/g, '$1')
      .replace(/```[\s\S]*?```/g, '')
      .replace(/`[^`]+`/g, '')
      .trim();

  const handleGenerate = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/api/tts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: cleanText(text) }),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'TTS failed' }));
        setError(err.error || 'TTS failed');
        return;
      }

      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      setAudioUrl(url);

      setTimeout(() => {
        audioRef.current?.play();
        setPlaying(true);
      }, 100);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const togglePlay = () => {
    if (!audioRef.current) return;
    if (playing) {
      audioRef.current.pause();
      setPlaying(false);
    } else {
      audioRef.current.play();
      setPlaying(true);
    }
  };

  const handleAddToPlaylist = async () => {
    setAddingToPlaylist(true);
    try {
      const res = await fetch('/api/playlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: title || 'Research Audio',
          text: cleanText(text),
        }),
      });
      if (res.ok) {
        setAddedToPlaylist(true);
        setTimeout(() => setAddedToPlaylist(false), 3000);
      } else {
        const err = await res.json().catch(() => ({ error: 'Failed' }));
        setError(err.error);
      }
    } catch {
      setError('Could not add to playlist');
    } finally {
      setAddingToPlaylist(false);
    }
  };

  return (
    <div className="flex items-center gap-2">
      {!audioUrl ? (
        <button
          onClick={handleGenerate}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-dark-800 border border-dark-600 text-sm text-dark-300 hover:bg-dark-700 hover:text-white transition-colors disabled:opacity-50"
          title="Convert to audio"
        >
          {loading ? (
            <>
              <svg className="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              Generating...
            </>
          ) : (
            <>
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15.536 8.464a5 5 0 010 7.072m2.828-9.9a9 9 0 010 12.728M5.586 15H4a1 1 0 01-1-1v-4a1 1 0 011-1h1.586l4.707-4.707C10.923 3.663 12 4.109 12 5v14c0 .891-1.077 1.337-1.707.707L5.586 15z" />
              </svg>
              Listen
            </>
          )}
        </button>
      ) : (
        <div className="flex items-center gap-2">
          <button
            onClick={togglePlay}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-dark-800 border border-dark-600 text-sm text-dark-300 hover:bg-dark-700 hover:text-white transition-colors"
          >
            {playing ? (
              <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M6 4h4v16H6V4zm8 0h4v16h-4V4z" /></svg>
            ) : (
              <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M8 5v14l11-7z" /></svg>
            )}
            {playing ? 'Pause' : 'Play'}
          </button>
          <a
            href={audioUrl}
            download="nexus-research.wav"
            className="p-1.5 rounded-lg bg-dark-800 border border-dark-600 text-dark-300 hover:bg-dark-700 hover:text-white transition-colors"
            title="Download"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
            </svg>
          </a>
        </div>
      )}
      {/* Add to playlist button */}
      <button
        onClick={handleAddToPlaylist}
        disabled={addingToPlaylist || addedToPlaylist}
        className="flex items-center gap-1 px-2.5 py-1.5 rounded-lg bg-dark-800 border border-dark-600 text-sm text-dark-300 hover:bg-dark-700 hover:text-white transition-colors disabled:opacity-50"
        title="Add to playlist"
      >
        {addedToPlaylist ? (
          <svg className="w-4 h-4 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
          </svg>
        ) : addingToPlaylist ? (
          <svg className="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
        ) : (
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
        )}
      </button>
      {error && <span className="text-xs text-red-400">{error}</span>}
      {audioUrl && (
        <audio ref={audioRef} src={audioUrl} onEnded={() => setPlaying(false)} className="hidden" />
      )}
    </div>
  );
}
