import { getUsageStats, getRateLimits } from '@/lib/storage';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  // Optional: Bearer token auth
  const apiKeys = (process.env.NEXUS_API_KEYS || '').split(',').filter(Boolean);
  if (apiKeys.length > 0) {
    const auth = request.headers.get('authorization');
    const token = auth?.replace('Bearer ', '');
    if (!token || !apiKeys.includes(token)) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }
  }

  const stats = getUsageStats();
  const rateLimits = getRateLimits();

  return Response.json({
    today: stats.today,
    last_7_days: stats.last_7_days,
    last_30_days: stats.last_30_days,
    daily_breakdown: stats.daily_breakdown,
    rate_limits: {
      openrouter: {
        requests_remaining: rateLimits.openrouterPerMinuteRemaining,
        requests_limit: rateLimits.openrouterPerMinuteLimit,
        tokens_remaining: rateLimits.openrouterDailyRemaining,
        tokens_limit: rateLimits.openrouterDailyLimit,
        last_updated: rateLimits.lastUpdated > 0 ? new Date(rateLimits.lastUpdated).toISOString() : null,
      },
    },
    recent_entries: stats.recentEntries.slice(0, 20),
  });
}
