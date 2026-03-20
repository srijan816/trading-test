import OpenAI from 'openai';

const OPENROUTER_BASE_URL = 'https://openrouter.ai/api/v1';

export function getOpenRouterClient(): OpenAI {
  return new OpenAI({
    baseURL: OPENROUTER_BASE_URL,
    apiKey: process.env.OPENROUTER_API_KEY,
    defaultHeaders: {
      'HTTP-Referer': 'http://localhost:3001',
      'X-Title': 'Nexus Deep Research',
    },
  });
}
