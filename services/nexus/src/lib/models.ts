import { ModelConfig } from '@/types';

export const MODELS: ModelConfig[] = [
  {
    id: 'stepfun/step-3.5-flash:free',
    displayName: 'Step 3.5 Flash',
    description: 'Fast MoE model, used as the main research and synthesis model',
    contextWindow: 128000,
    bestFor: 'Default research, synthesis, and quick lookups',
  },
  {
    id: 'openrouter/hunter-alpha',
    displayName: 'Hunter Alpha',
    description: 'Legacy frontier model option',
    contextWindow: 1000000,
    bestFor: 'Legacy/manual selection only',
  },
  {
    id: 'openrouter/healer-alpha',
    displayName: 'Healer Alpha',
    description: 'Legacy general reasoning model option',
    contextWindow: 128000,
    bestFor: 'Legacy/manual selection only',
  },
];
