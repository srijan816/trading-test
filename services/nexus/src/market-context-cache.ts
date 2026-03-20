import { existsSync, mkdirSync, readFileSync } from 'fs';
import { join } from 'path';

interface WeatherBackgroundEntry {
  city_key: string;
  cached_at: string;
  background_context: string;
}

const CACHE_DIR = join(process.cwd(), 'data', 'market-cache');
const weatherBackgroundCache = new Map<string, WeatherBackgroundEntry>();

function ensureCacheDir() {
  if (!existsSync(CACHE_DIR)) {
    mkdirSync(CACHE_DIR, { recursive: true });
  }
}

function weatherCachePath(cityKey: string): string {
  return join(CACHE_DIR, `weather-${sanitizeKey(cityKey)}.json`);
}

function sanitizeKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9_-]+/g, '-');
}

export function getWeatherBackground(cityKey?: string | null): string | null {
  if (!cityKey) return null;
  const mem = weatherBackgroundCache.get(cityKey);
  if (mem) return mem.background_context;

  ensureCacheDir();
  const path = weatherCachePath(cityKey);
  if (!existsSync(path)) return null;

  try {
    const parsed = JSON.parse(readFileSync(path, 'utf-8')) as WeatherBackgroundEntry;
    weatherBackgroundCache.set(cityKey, parsed);
    return parsed.background_context;
  } catch {
    return null;
  }
}
