'use client';

import { useState, useRef, useEffect } from 'react';

interface SearchBarProps {
  onSearch: (query: string) => void;
  isLoading: boolean;
  centered?: boolean;
}

export default function SearchBar({ onSearch, isLoading, centered = true }: SearchBarProps) {
  const [query, setQuery] = useState('');
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const handleSubmit = () => {
    const trimmed = query.trim();
    if (trimmed && !isLoading) {
      onSearch(trimmed);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  return (
    <div className={`w-full max-w-3xl ${centered ? 'mx-auto' : ''}`}>
      <div className="relative">
        <textarea
          ref={inputRef}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask anything — I'll research it deeply..."
          rows={1}
          disabled={isLoading}
          className="w-full resize-none rounded-2xl bg-dark-900 border border-dark-700 px-5 py-4 pr-14 text-white placeholder-dark-500 focus:outline-none focus:border-dark-500 focus:ring-1 focus:ring-dark-500 transition-all text-base disabled:opacity-50"
          style={{ minHeight: '56px', maxHeight: '200px' }}
          onInput={(e) => {
            const target = e.target as HTMLTextAreaElement;
            target.style.height = 'auto';
            target.style.height = Math.min(target.scrollHeight, 200) + 'px';
          }}
        />
        <button
          onClick={handleSubmit}
          disabled={!query.trim() || isLoading}
          className="absolute right-3 bottom-3 p-2 rounded-lg bg-blue-600 hover:bg-blue-500 disabled:bg-dark-700 disabled:text-dark-500 text-white transition-colors"
        >
          {isLoading ? (
            <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
            </svg>
          ) : (
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 5l7 7m0 0l-7 7m7-7H3" />
            </svg>
          )}
        </button>
      </div>
    </div>
  );
}
