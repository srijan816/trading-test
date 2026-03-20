'use client';

import { useState, useCallback } from 'react';
import { ResearchMode, OutputLength } from '@/types';
import SearchBar from '@/components/SearchBar';
import ModeToggle from '@/components/ModeToggle';
import ModelSelector from '@/components/ModelSelector';
import OutputLengthSelector from '@/components/OutputLengthSelector';
import DeepResearchView from '@/components/DeepResearchView';

interface ResearchTab {
  id: number;
  query: string;
  mode: ResearchMode;
  model: string;
  outputLength: OutputLength;
  isComplete: boolean;
  label: string;
}

export default function HomePage() {
  const [mode, setMode] = useState<ResearchMode>('deep');
  const [model, setModel] = useState('stepfun/step-3.5-flash:free');
  const [outputLength, setOutputLength] = useState<OutputLength>('medium');
  const [tabs, setTabs] = useState<ResearchTab[]>([]);
  const [activeTabId, setActiveTabId] = useState<number | null>(null);
  const [nextId, setNextId] = useState(1);

  const handleSearch = (query: string) => {
    const id = nextId;
    const newTab: ResearchTab = {
      id,
      query,
      mode,
      model,
      outputLength,
      isComplete: false,
      label: query.slice(0, 40),
    };
    setTabs((prev) => [...prev, newTab]);
    setActiveTabId(id);
    setNextId((n) => n + 1);
  };

  const markTabComplete = useCallback((id: number) => {
    setTabs((prev) => prev.map((t) => (t.id === id ? { ...t, isComplete: true } : t)));
  }, []);

  const closeTab = useCallback((id: number) => {
    setTabs((prev) => {
      const remaining = prev.filter((t) => t.id !== id);
      setActiveTabId((current) => {
        if (current !== id) return current;
        return remaining.length > 0 ? remaining[remaining.length - 1].id : null;
      });
      return remaining;
    });
  }, []);

  const activeTab = tabs.find((t) => t.id === activeTabId) ?? null;

  const homeView = (
    <main className="min-h-screen flex flex-col items-center justify-center px-4">
      <div className="text-center mb-8">
        <h1 className="text-4xl font-bold text-white mb-2">Nexus</h1>
        <p className="text-dark-400 text-lg">Deep research, powered by AI</p>
      </div>
      <SearchBar onSearch={handleSearch} isLoading={false} centered />
      <div className="flex items-center gap-3 mt-4 flex-wrap justify-center">
        <ModeToggle mode={mode} onChange={setMode} />
        <OutputLengthSelector value={outputLength} onChange={setOutputLength} />
        <ModelSelector selectedModel={model} onChange={setModel} />
      </div>
      <div className="mt-10 grid grid-cols-1 sm:grid-cols-3 gap-3 max-w-2xl">
        {['Compare OpenAI vs Anthropic approaches to AI safety', 'Latest advances in quantum error correction', 'How does the Linux kernel handle memory management?'].map((s) => (
          <button key={s} onClick={() => handleSearch(s)} className="px-4 py-3 rounded-xl bg-dark-900 border border-dark-700 text-sm text-dark-300 hover:bg-dark-800 hover:border-dark-500 transition-colors text-left">{s}</button>
        ))}
      </div>
      <div className="mt-8 flex items-center gap-4 text-sm text-dark-500">
        <a href="/history" className="hover:text-dark-300 transition-colors">History</a>
        <span>|</span>
        <a href="/settings" className="hover:text-dark-300 transition-colors">Usage & Settings</a>
      </div>
    </main>
  );

  return (
    <div className="min-h-screen flex flex-col">
      {/* Tab strip */}
      {tabs.length > 0 && (
        <div className="flex-shrink-0 bg-dark-950 border-b border-dark-700 overflow-x-auto">
          <div className="flex items-center gap-1 px-2 py-1.5 min-w-max">
            {tabs.map((tab) => {
              const isActive = tab.id === activeTabId;
              return (
                <div
                  key={tab.id}
                  className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-sm cursor-pointer transition-colors max-w-[200px] flex-shrink-0 ${
                    isActive
                      ? 'bg-dark-800 border-dark-500 text-white'
                      : 'bg-dark-900 border-dark-700 text-dark-400 hover:bg-dark-850 hover:text-dark-200'
                  }`}
                  onClick={() => setActiveTabId(tab.id)}
                >
                  {!tab.isComplete && (
                    <span className="inline-block w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse flex-shrink-0" />
                  )}
                  <span className="truncate">{tab.label}</span>
                  <button
                    onClick={(e) => { e.stopPropagation(); closeTab(tab.id); }}
                    className="flex-shrink-0 text-dark-500 hover:text-white transition-colors ml-0.5"
                    aria-label="Close tab"
                  >
                    ×
                  </button>
                </div>
              );
            })}
            {/* New search / home button */}
            <button
              onClick={() => setActiveTabId(null)}
              className="flex-shrink-0 px-2 py-1.5 rounded-lg bg-dark-900 border border-dark-700 text-dark-400 hover:bg-dark-800 hover:text-white transition-colors text-sm"
              title="New search"
            >
              +
            </button>
          </div>
        </div>
      )}

      {/* Content */}
      {tabs.length === 0 || activeTabId === null ? (
        homeView
      ) : activeTab ? (
        <main className="flex-1 px-4 py-6">
          <div className="flex items-center justify-between max-w-6xl mx-auto mb-4">
            <button onClick={() => setActiveTabId(null)} className="flex items-center gap-2 text-dark-400 hover:text-white transition-colors">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" /></svg>
              <span className="text-lg font-semibold text-white">Nexus</span>
            </button>
            <div className="flex items-center gap-2 flex-wrap">
              <ModeToggle mode={mode} onChange={setMode} />
              <OutputLengthSelector value={outputLength} onChange={setOutputLength} />
              <ModelSelector selectedModel={model} onChange={setModel} />
              <a href="/history" className="px-3 py-1.5 rounded-lg bg-dark-800 border border-dark-600 text-sm text-dark-300 hover:bg-dark-700 transition-colors">History</a>
            </div>
          </div>
          <div className="max-w-3xl mx-auto mb-4">
            <p className="text-sm text-dark-300 line-clamp-2">{activeTab.query}</p>
          </div>
          <DeepResearchView
            key={activeTab.id}
            query={activeTab.query}
            model={activeTab.model}
            outputLength={activeTab.outputLength}
            mode={activeTab.mode}
            onCancel={() => closeTab(activeTab.id)}
            onComplete={() => markTabComplete(activeTab.id)}
            onFollowUp={(q) => handleSearch(q)}
          />
        </main>
      ) : null}
    </div>
  );
}
