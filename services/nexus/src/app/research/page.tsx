'use client';

import { useSearchParams } from 'next/navigation';
import { Suspense } from 'react';
import { OutputLength } from '@/types';
import DeepResearchView from '@/components/DeepResearchView';

function ResearchContent() {
  const searchParams = useSearchParams();
  const query = searchParams.get('q');
  const model = searchParams.get('model') || 'stepfun/step-3.5-flash:free';
  const outputLength = (searchParams.get('outputLength') as 'short' | 'medium' | 'long') || 'medium';
  const mode = (searchParams.get('mode') as 'quick' | 'standard' | 'deep') || 'deep';

  if (!query) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-white mb-2">No query provided</h1>
          <p className="text-dark-400">
            Go to{' '}
            <a href="/" className="text-blue-400 hover:text-blue-300">
              the home page
            </a>{' '}
            to start a research session.
          </p>
        </div>
      </div>
    );
  }

  return (
    <main className="min-h-screen px-4 py-6">
      <div className="max-w-6xl mx-auto">
        <div className="flex items-center gap-2 mb-6">
          <a
            href="/"
            className="flex items-center gap-2 text-dark-400 hover:text-white transition-colors"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
            </svg>
            <span className="text-lg font-semibold text-white">Nexus</span>
          </a>
        </div>

        <h2 className="text-xl font-semibold text-white mb-6 max-w-3xl mx-auto">{query}</h2>

        <DeepResearchView query={query} model={model} outputLength={outputLength} mode={mode} onCancel={() => window.location.href = '/'} />
      </div>
    </main>
  );
}

export default function ResearchPage() {
  return (
    <Suspense
      fallback={
        <div className="min-h-screen flex items-center justify-center">
          <div className="text-dark-400">Loading...</div>
        </div>
      }
    >
      <ResearchContent />
    </Suspense>
  );
}
