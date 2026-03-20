'use client';

import { useState } from 'react';
import { MODELS } from '@/lib/models';

interface ModelSelectorProps {
  selectedModel: string;
  onChange: (modelId: string) => void;
}

export default function ModelSelector({ selectedModel, onChange }: ModelSelectorProps) {
  const [open, setOpen] = useState(false);
  const current = MODELS.find((m) => m.id === selectedModel) || MODELS[0];

  return (
    <div className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-dark-900 border border-dark-700 text-sm text-dark-200 hover:border-dark-500 transition-colors"
      >
        <span className="w-2 h-2 rounded-full bg-green-500" />
        {current.displayName}
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <>
          <div className="fixed inset-0 z-10" onClick={() => setOpen(false)} />
          <div className="absolute top-full mt-2 right-0 z-20 w-80 bg-dark-900 border border-dark-700 rounded-xl shadow-2xl overflow-hidden">
            {MODELS.map((model) => (
              <button
                key={model.id}
                onClick={() => {
                  onChange(model.id);
                  setOpen(false);
                }}
                className={`w-full text-left px-4 py-3 hover:bg-dark-800 transition-colors ${
                  model.id === selectedModel ? 'bg-dark-800' : ''
                }`}
              >
                <div className="flex items-center gap-2">
                  <span className="font-medium text-white text-sm">{model.displayName}</span>
                  {model.id === selectedModel && (
                    <svg className="w-4 h-4 text-blue-400" fill="currentColor" viewBox="0 0 20 20">
                      <path
                        fillRule="evenodd"
                        d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                        clipRule="evenodd"
                      />
                    </svg>
                  )}
                </div>
                <p className="text-xs text-dark-400 mt-0.5">{model.description}</p>
                <p className="text-xs text-dark-500 mt-0.5">Best for: {model.bestFor}</p>
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
