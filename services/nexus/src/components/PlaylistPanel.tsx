'use client';

import { useState, useEffect, useRef } from 'react';

interface PlaylistItem {
  id: string;
  title: string;
  filename: string;
  duration_estimate: number;
  created_at: number;
}

const TTS_URL = '/api/playlist';

export default function PlaylistPanel() {
  const [items, setItems] = useState<PlaylistItem[]>([]);
  const [currentIdx, setCurrentIdx] = useState(-1);
  const [playing, setPlaying] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const audioRef = useRef<HTMLAudioElement>(null);

  const fetchPlaylist = async () => {
    try {
      const res = await fetch(TTS_URL);
      if (res.ok) {
        const data = await res.json();
        setItems(data.items || []);
      }
    } catch {}
  };

  useEffect(() => { fetchPlaylist(); }, []);

  const playItem = (idx: number) => {
    if (idx < 0 || idx >= items.length) {
      setPlaying(false);
      setCurrentIdx(-1);
      return;
    }
    setCurrentIdx(idx);
    const ttsBase = process.env.NEXT_PUBLIC_TTS_URL || '';
    // Audio served through TTS service directly
    if (audioRef.current) {
      // Use the playlist audio endpoint through the proxy
      audioRef.current.src = `/api/playlist/audio?file=${items[idx].filename}`;
      audioRef.current.play().then(() => setPlaying(true)).catch(() => {});
    }
  };

  const togglePlay = () => {
    if (!audioRef.current) return;
    if (playing) {
      audioRef.current.pause();
      setPlaying(false);
    } else if (currentIdx >= 0) {
      audioRef.current.play();
      setPlaying(true);
    } else if (items.length > 0) {
      playItem(0);
    }
  };

  const handleNext = () => {
    if (currentIdx < items.length - 1) playItem(currentIdx + 1);
  };

  const handlePrev = () => {
    if (currentIdx > 0) playItem(currentIdx - 1);
  };

  const handleEnded = () => {
    // Auto-advance to next
    if (currentIdx < items.length - 1) {
      playItem(currentIdx + 1);
    } else {
      setPlaying(false);
      setCurrentIdx(-1);
    }
  };

  const handleRemove = async (id: string) => {
    try {
      await fetch(`${TTS_URL}?id=${id}`, { method: 'DELETE' });
      fetchPlaylist();
    } catch {}
  };

  const fmtTime = (s: number) => {
    const m = Math.floor(s / 60);
    const sec = s % 60;
    return m > 0 ? `${m}m ${sec}s` : `${sec}s`;
  };

  if (items.length === 0) return null;

  return (
    <div className="fixed bottom-0 left-0 right-0 z-50">
      {/* Expanded playlist */}
      {expanded && (
        <div className="bg-dark-900 border-t border-dark-700 max-h-64 overflow-y-auto">
          <div className="max-w-4xl mx-auto px-4 py-2">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-xs text-dark-400 uppercase tracking-wider">Playlist ({items.length})</h3>
              <button onClick={() => setExpanded(false)} className="text-dark-400 hover:text-white">
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </button>
            </div>
            <div className="space-y-1">
              {items.map((item, i) => (
                <div
                  key={item.id}
                  className={`flex items-center justify-between py-1.5 px-2 rounded text-sm cursor-pointer transition-colors ${
                    i === currentIdx ? 'bg-blue-900/30 text-white' : 'text-dark-300 hover:bg-dark-800'
                  }`}
                  onClick={() => playItem(i)}
                >
                  <div className="flex items-center gap-2 min-w-0">
                    {i === currentIdx && playing ? (
                      <svg className="w-3 h-3 text-blue-400 flex-shrink-0" fill="currentColor" viewBox="0 0 24 24"><path d="M6 4h4v16H6V4zm8 0h4v16h-4V4z" /></svg>
                    ) : (
                      <span className="text-xs text-dark-500 w-3 text-center flex-shrink-0">{i + 1}</span>
                    )}
                    <span className="truncate">{item.title}</span>
                  </div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <span className="text-xs text-dark-500">~{fmtTime(item.duration_estimate)}</span>
                    <button
                      onClick={(e) => { e.stopPropagation(); handleRemove(item.id); }}
                      className="text-dark-500 hover:text-red-400 transition-colors"
                    >
                      <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Mini player bar */}
      <div className="bg-dark-950 border-t border-dark-700 px-4 py-2">
        <div className="max-w-4xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-3 min-w-0">
            {/* Prev */}
            <button onClick={handlePrev} disabled={currentIdx <= 0} className="text-dark-400 hover:text-white disabled:opacity-30 transition-colors">
              <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M6 6h2v12H6zm3.5 6l8.5 6V6z" /></svg>
            </button>
            {/* Play/Pause */}
            <button onClick={togglePlay} className="w-8 h-8 flex items-center justify-center rounded-full bg-white text-black hover:bg-gray-200 transition-colors">
              {playing ? (
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M6 4h4v16H6V4zm8 0h4v16h-4V4z" /></svg>
              ) : (
                <svg className="w-4 h-4 ml-0.5" fill="currentColor" viewBox="0 0 24 24"><path d="M8 5v14l11-7z" /></svg>
              )}
            </button>
            {/* Next */}
            <button onClick={handleNext} disabled={currentIdx >= items.length - 1} className="text-dark-400 hover:text-white disabled:opacity-30 transition-colors">
              <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M16 18h2V6h-2zM4 18l8.5-6L4 6z" /></svg>
            </button>
            {/* Current track */}
            <span className="text-sm text-dark-300 truncate">
              {currentIdx >= 0 ? items[currentIdx].title : 'No track selected'}
            </span>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs text-dark-500">{items.length} tracks</span>
            <button onClick={() => setExpanded(!expanded)} className="text-dark-400 hover:text-white transition-colors">
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={expanded ? "M19 9l-7 7-7-7" : "M5 15l7-7 7 7"} />
              </svg>
            </button>
          </div>
        </div>
      </div>

      <audio ref={audioRef} onEnded={handleEnded} className="hidden" />
    </div>
  );
}
