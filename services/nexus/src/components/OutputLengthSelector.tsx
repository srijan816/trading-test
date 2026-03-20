'use client';

import { OutputLength } from '@/types';

interface OutputLengthSelectorProps {
  value: OutputLength;
  onChange: (v: OutputLength) => void;
}

const OPTIONS: { value: OutputLength; label: string }[] = [
  { value: 'short', label: 'Short' },
  { value: 'medium', label: 'Medium' },
  { value: 'long', label: 'Long' },
];

export default function OutputLengthSelector({ value, onChange }: OutputLengthSelectorProps) {
  return (
    <div className="flex items-center bg-dark-900 rounded-lg border border-dark-700 p-0.5">
      {OPTIONS.map((opt) => (
        <button
          key={opt.value}
          onClick={() => onChange(opt.value)}
          className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${
            value === opt.value
              ? 'bg-dark-700 text-white'
              : 'text-dark-400 hover:text-dark-200'
          }`}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}
