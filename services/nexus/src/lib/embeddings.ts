import { updateRateLimits } from './storage';

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY || '';
const EMBEDDING_MODEL = 'nvidia/llama-nemotron-embed-vl-1b-v2:free';

export async function getEmbeddings(texts: string[]): Promise<number[][]> {
  if (texts.length === 0) return [];

  const batches: string[][] = [];
  for (let i = 0; i < texts.length; i += 50) {
    batches.push(texts.slice(i, i + 50));
  }

  const allEmbeddings: number[][] = [];

  for (const batch of batches) {
    const response = await fetch('https://openrouter.ai/api/v1/embeddings', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: EMBEDDING_MODEL,
        input: batch,
      }),
    });

    // Capture rate limit headers from OpenRouter
    updateRateLimits({
      'x-ratelimit-remaining': response.headers.get('x-ratelimit-remaining'),
      'x-ratelimit-limit': response.headers.get('x-ratelimit-limit'),
      'x-ratelimit-remaining-tokens': response.headers.get('x-ratelimit-remaining-tokens'),
      'x-ratelimit-limit-tokens': response.headers.get('x-ratelimit-limit-tokens'),
    });

    if (!response.ok) {
      console.error(`Embedding API error: ${response.status}`);
      return [];
    }

    const data = await response.json();
    if (data.data) {
      const sorted = data.data.sort((a: any, b: any) => a.index - b.index);
      allEmbeddings.push(...sorted.map((d: any) => d.embedding));
    }
  }

  return allEmbeddings;
}

export function cosineSimilarity(a: number[], b: number[]): number {
  if (a.length !== b.length || a.length === 0) return 0;
  let dot = 0, normA = 0, normB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  const denom = Math.sqrt(normA) * Math.sqrt(normB);
  return denom === 0 ? 0 : dot / denom;
}
