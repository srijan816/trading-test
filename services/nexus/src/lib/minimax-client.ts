const MINIMAX_BASE_URL = process.env.MINIMAX_BASE_URL || 'https://api.minimax.io/v1';
const OPENROUTER_BASE_URL = 'https://openrouter.ai/api/v1/chat/completions';

export interface MiniMaxCallResult {
  content: string;
  usage: { input: number; output: number };
  path: 'direct-minimax' | 'openrouter-fetch';
  modelUsed: string;
}

interface CallOptions {
  model?: string;
  temperature: number;
  max_tokens: number;
}

function normalizeModelName(model?: string): string {
  const raw = String(model || '').trim();
  if (!raw) return 'MiniMax-M2.7';
  if (raw.includes('/')) {
    const suffix = raw.split('/').pop() || raw;
    if (/^minimax-m/i.test(suffix)) {
      return suffix.replace(/^minimax-m/i, 'MiniMax-M');
    }
    return suffix;
  }
  if (/^minimax-m/i.test(raw)) {
    return raw.replace(/^minimax-m/i, 'MiniMax-M');
  }
  return raw;
}

function usageFromJson(payload: any): { input: number; output: number } {
  return {
    input: Number(payload?.usage?.prompt_tokens ?? payload?.usage?.input_tokens ?? 0) || 0,
    output: Number(payload?.usage?.completion_tokens ?? payload?.usage?.output_tokens ?? 0) || 0,
  };
}

async function parseResponse(response: Response): Promise<any> {
  const text = await response.text();
  if (!text.trim()) {
    throw new Error(`Empty response body (status ${response.status})`);
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`Failed to parse JSON response (status ${response.status}): ${text.slice(0, 500)}`);
  }
}

export async function callDirectMiniMaxSynthesis(
  systemPrompt: string,
  userPrompt: string,
  options: CallOptions,
): Promise<MiniMaxCallResult | null> {
  const directModel = normalizeModelName(options.model);

  if (!process.env.MINIMAX_API_KEY) {
    return null;
  }

  try {
    const response = await fetch(`${MINIMAX_BASE_URL}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${process.env.MINIMAX_API_KEY}`,
      },
      body: JSON.stringify({
        model: directModel,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userPrompt },
        ],
        temperature: options.temperature,
        max_tokens: options.max_tokens,
        stream: false,
        response_format: { type: 'json_object' },
      }),
    });

    const payload = await parseResponse(response);
    if (!response.ok) {
      console.error(`[market-research] direct MiniMax error ${response.status}: ${JSON.stringify(payload).slice(0, 500)}`);
      return null;
    }

    const content = String(payload?.choices?.[0]?.message?.content || '').trim();
    if (!content) {
      console.error('[market-research] direct MiniMax returned empty content');
      return null;
    }

    return {
      content,
      usage: usageFromJson(payload),
      path: 'direct-minimax',
      modelUsed: String(payload?.model || directModel),
    };
  } catch (error: any) {
    console.error(`[market-research] direct MiniMax request failed: ${error instanceof Error ? error.message : String(error)}`);
    return null;
  }
}

export async function callOpenRouterMiniMaxSynthesis(
  systemPrompt: string,
  userPrompt: string,
  options: CallOptions,
): Promise<MiniMaxCallResult | null> {
  const directModel = normalizeModelName(options.model);
  if (!process.env.OPENROUTER_API_KEY) {
    return null;
  }

  try {
    const response = await fetch(OPENROUTER_BASE_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`,
        'HTTP-Referer': 'http://localhost:3001',
        'X-Title': 'Nexus Deep Research',
      },
      body: JSON.stringify({
        model: options.model,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userPrompt },
        ],
        temperature: options.temperature,
        max_tokens: options.max_tokens,
        stream: false,
        include_reasoning: false,
        reasoning: { exclude: true },
        response_format: { type: 'json_object' },
      }),
    });

    const payload = await parseResponse(response);
    if (!response.ok) {
      console.error(`[market-research] OpenRouter raw MiniMax error ${response.status}: ${JSON.stringify(payload).slice(0, 500)}`);
      return null;
    }

    const content = String(payload?.choices?.[0]?.message?.content || '').trim();
    if (!content) {
      console.error('[market-research] OpenRouter raw MiniMax returned empty content');
      return null;
    }

    return {
      content,
      usage: usageFromJson(payload),
      path: 'openrouter-fetch',
      modelUsed: String(payload?.model || options.model || directModel),
    };
  } catch (error: any) {
    console.error(`[market-research] OpenRouter raw MiniMax request failed: ${error instanceof Error ? error.message : String(error)}`);
    return null;
  }
}

export async function callMiniMaxSynthesis(
  systemPrompt: string,
  userPrompt: string,
  options: CallOptions,
): Promise<MiniMaxCallResult | null> {
  return (await callDirectMiniMaxSynthesis(systemPrompt, userPrompt, options))
    || (await callOpenRouterMiniMaxSynthesis(systemPrompt, userPrompt, options));
}
