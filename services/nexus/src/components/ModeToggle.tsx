'use client';

import { ResearchMode } from '@/types';

interface ModeToggleProps {
  mode: ResearchMode;
  onChange: (mode: ResearchMode) => void;
}

const modes: { value: ResearchMode; label: string; time: string }[] = [
  { value: 'quick', label: 'Quick', time: '5-15s' },
  { value: 'standard', label: 'Standard', time: '30-60s' },
  { value: 'deep', label: 'Deep', time: '2-5m' },
];

export default function ModeToggle({ mode, onChange }: ModeToggleProps) {
  return (
    <div className="flex items-center bg-dark-900 rounded-full p-1 border border-dark-700">
      {modes.map((m) => (
        <button
          key={m.value}
          onClick={() => onChange(m.value)}
          className={`px-3 py-1.5 rounded-full text-sm font-medium transition-all ${
            mode === m.value
              ? m.value === 'deep' ? 'bg-blue-600 text-white shadow-sm'
              : m.value === 'standard' ? 'bg-dark-600 text-white shadow-sm'
              : 'bg-dark-700 text-white shadow-sm'
              : 'text-dark-400 hover:text-dark-200'
          }`}
          title={m.time}
        >
          {m.label}
        </button>
      ))}
    </div>
  );
}
