import { appendFileSync, existsSync, mkdirSync } from 'fs';
import { join } from 'path';

export interface MarketResearchPhaseCost {
  phase: string;
  model: string;
  prompt_tokens: number;
  completion_tokens: number;
  estimated_cost_usd: number;
}

function resolveServiceRoot(): string {
  const cwd = process.cwd();
  if (existsSync(join(cwd, 'package.json')) && existsSync(join(cwd, 'src'))) {
    return cwd;
  }
  return join(cwd, 'services', 'nexus');
}

const DATA_DIR = join(resolveServiceRoot(), 'data');
const COST_LOG_FILE = join(DATA_DIR, 'market-research-costs.jsonl');
const PRICING_CACHE_TTL_MS = 6 * 60 * 60 * 1000;

let pricingCache: {
  expiresAt: number;
  prices: Record<string, { prompt: number; completion: number }>;
} = {
  expiresAt: 0,
  prices: {},
};

function ensureDataDir() {
  if (!existsSync(DATA_DIR)) {
    mkdirSync(DATA_DIR, { recursive: true });
  }
  if (!existsSync(COST_LOG_FILE)) {
    appendFileSync(COST_LOG_FILE, '');
  }
}

async function loadOpenRouterPricing(): Promise<Record<string, { prompt: number; completion: number }>> {
  if (pricingCache.expiresAt > Date.now() && Object.keys(pricingCache.prices).length > 0) {
    return pricingCache.prices;
  }

  try {
    const res = await fetch('https://openrouter.ai/api/v1/models', {
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) {
      return pricingCache.prices;
    }
    const payload = await res.json();
    const prices: Record<string, { prompt: number; completion: number }> = {};
    for (const model of payload.data || []) {
      const pricing = model?.pricing || {};
      const prompt = Number(pricing.prompt || 0);
      const completion = Number(pricing.completion || 0);
      if (model?.id) {
        prices[String(model.id)] = {
          prompt: Number.isFinite(prompt) ? prompt : 0,
          completion: Number.isFinite(completion) ? completion : 0,
        };
      }
    }
    pricingCache = {
      expiresAt: Date.now() + PRICING_CACHE_TTL_MS,
      prices,
    };
    return prices;
  } catch {
    return pricingCache.prices;
  }
}

export async function estimateModelCostUsd(model: string, promptTokens: number, completionTokens: number): Promise<number> {
  const prices = await loadOpenRouterPricing();
  const pricing = prices[model];
  if (!pricing) return 0;
  return (promptTokens * pricing.prompt) + (completionTokens * pricing.completion);
}

export function appendMarketResearchCostLog(entry: Record<string, unknown>) {
  ensureDataDir();
  appendFileSync(COST_LOG_FILE, JSON.stringify(entry) + '\n');
}
