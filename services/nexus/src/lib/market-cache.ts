import { createHash } from 'crypto';

export const CACHE_TTL_MS = {
  weather: 2 * 60 * 60 * 1000,
  crypto: 1 * 60 * 60 * 1000,
  event: 6 * 60 * 60 * 1000,
  politics: 6 * 60 * 60 * 1000,
  entertainment: 6 * 60 * 60 * 1000,
  default: 4 * 60 * 60 * 1000,
} as const;

type CacheableResult = Record<string, any>;

interface CacheEntry {
  result: CacheableResult;
  timestamp: number;
  currentYesPrice?: number;
}

const marketCache = new Map<string, CacheEntry>();

export function buildMarketCacheKey(question: string, marketData: Record<string, unknown>, ensembleData: Record<string, unknown>): string {
  return createHash('sha256')
    .update(question)
    .update(JSON.stringify(marketData || {}))
    .update(JSON.stringify(ensembleData || {}))
    .digest('hex');
}

export function getCacheTtl(marketType?: string): number {
  if (!marketType) return CACHE_TTL_MS.default;
  return CACHE_TTL_MS[marketType as keyof typeof CACHE_TTL_MS] || CACHE_TTL_MS.default;
}

export function getCachedMarketResearch(key: string, marketType?: string, currentYesPrice?: number): CacheableResult | null {
  const entry = marketCache.get(key);
  if (!entry) return null;
  const ttl = getCacheTtl(marketType);
  if ((Date.now() - entry.timestamp) > ttl) {
    marketCache.delete(key);
    return null;
  }
  if (
    typeof currentYesPrice === 'number'
    && typeof entry.currentYesPrice === 'number'
    && Math.abs(currentYesPrice - entry.currentYesPrice) > 0.05
  ) {
    marketCache.delete(key);
    return null;
  }
  return {
    ...entry.result,
    from_cache: true,
    duration_ms: 0,
  };
}

export function setCachedMarketResearch(key: string, marketType: string | undefined, currentYesPrice: number | undefined, result: CacheableResult): void {
  const ttl = getCacheTtl(marketType);
  marketCache.set(key, {
    result: { ...result, from_cache: false, cache_ttl_ms: ttl },
    timestamp: Date.now(),
    currentYesPrice,
  });
}
