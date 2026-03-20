import { getUsageStats } from '@/lib/storage';

export const dynamic = 'force-dynamic';

export async function GET() {
  const stats = getUsageStats();
  return new Response(JSON.stringify(stats), { headers: { 'Content-Type': 'application/json' } });
}
