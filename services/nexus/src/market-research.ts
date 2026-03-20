import { crawlUrls } from '@/crawl4ai-client';
import { MODEL_ROUTES } from '@/config/model-routes';
import { MARKET_PROMPTS } from '@/lib/prompts';
import { buildMarketCacheKey, getCachedMarketResearch, setCachedMarketResearch } from '@/lib/market-cache';
import { callDirectMiniMaxSynthesis, callOpenRouterMiniMaxSynthesis } from '@/lib/minimax-client';
import { getOpenRouterClient } from '@/lib/openrouter';
import { searchSearXNG } from '@/lib/vane-client';
import { getWeatherBackground } from '@/market-context-cache';
import { appendMarketResearchCostLog } from '@/market-research-costs';

export type MarketType =
  | 'weather'
  | 'crypto'
  | 'sports'
  | 'politics'
  | 'entertainment'
  | 'other'
  | 'event'
  | 'auto';
export type SearchDepth = 'quick' | 'standard' | 'deep';

export interface MarketResearchRequest {
  question: string;
  market_type?: MarketType;
  market_data?: {
    current_yes_price?: number;
    current_no_price?: number;
    current_price_yes?: number;
    current_price_no?: number;
    volume_usd?: number;
    volume?: number;
    resolution_date?: string;
    resolution_time?: string;
    market_id?: string;
    platform?: string;
  };
  ensemble_data?: {
    mu: number;
    sigma: number;
    sources?: string[] | Record<string, number>;
    threshold?: number;
    threshold_unit?: string;
    unit?: string;
    probability?: number;
    ensemble_probability?: number;
  };
  calibration_data?: {
    recent_crps?: number;
    calibration_ratio?: number;
    grad_sigma?: number;
    suggested_sigma_adjustment?: number;
    sigma_suggestion?: string;
    city_track_record?: string;
  };
  model?: string;
  search_depth?: SearchDepth;
}

export interface MarketResearchResponse {
  probability: number;
  confidence: 'low' | 'medium' | 'high';
  reasoning: string;
  reasoning_trace?: string | null;
  edge_assessment: {
    model_probability: number;
    market_probability: number;
    raw_edge_bps: number;
    adjusted_edge_bps: number;
    recommendation: 'STRONG_BUY_YES' | 'BUY_YES' | 'HOLD' | 'BUY_NO' | 'STRONG_BUY_NO';
    risk_factors: string[];
  };
  sources: Array<{ url: string; title: string; snippet: string }>;
  search_queries_used: string[];
  model_used: string;
  tokens: { input: number; output: number };
  tokens_used: { input: number; output: number };
  duration_ms: number;
  market_type: Exclude<MarketType, 'auto'>;
  ensemble_override_triggered: boolean;
  from_cache: boolean;
  ensemble_probability?: number;
  llm_probability?: number;
}

interface PhaseUsage {
  model: string;
  input: number;
  output: number;
  estimatedCostUsd: number;
}

interface SearchDocument {
  url: string;
  title: string;
  snippet: string;
  content: string;
}

interface NormalizedMarketData {
  currentYesPrice: number;
  currentNoPrice: number;
  volumeUsd?: number;
  resolutionDate?: string;
  marketId?: string;
  platform?: string;
}

interface NormalizedEnsembleData {
  mu: number;
  sigma: number;
  sources: string[];
  threshold?: number;
  thresholdUnit?: string;
  probability?: number;
}

const SEARCH_LIMITS: Record<SearchDepth, { queries: number; perQuerySources: number; selectedSources: number }> = {
  quick: { queries: 1, perQuerySources: 3, selectedSources: 3 },
  standard: { queries: 2, perQuerySources: 4, selectedSources: 4 },
  deep: { queries: 2, perQuerySources: 5, selectedSources: 5 },
};

export class MarketResearchError extends Error {
  errorCode: string;
  statusCode: number;
  durationMs: number;

  constructor(errorCode: string, message: string, statusCode: number, durationMs: number) {
    super(message);
    this.errorCode = errorCode;
    this.statusCode = statusCode;
    this.durationMs = durationMs;
  }
}

function clampProbability(value: number): number {
  if (!Number.isFinite(value)) return 0.5;
  return Math.min(0.999, Math.max(0.001, value));
}

function asNumber(value: unknown): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function normalizeConfidence(value: unknown): 'low' | 'medium' | 'high' {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'low' || normalized === 'medium' || normalized === 'high') {
    return normalized;
  }
  return 'medium';
}

function inferMarketType(
  question: string,
  requested?: MarketType,
  ensembleData?: MarketResearchRequest['ensemble_data'],
): Exclude<MarketType, 'auto'> {
  if (requested && requested !== 'auto') {
    return requested as Exclude<MarketType, 'auto'>;
  }
  if (ensembleData) return 'weather';
  const lowered = question.toLowerCase();
  if (/(temperature|forecast|weather|rain|snow|precipitation)/.test(lowered)) return 'weather';
  if (/(bitcoin|btc|ethereum|eth|crypto|solana|doge)/.test(lowered)) return 'crypto';
  if (/(nba|nfl|mlb|nhl|match|tournament|cup|game)/.test(lowered)) return 'sports';
  if (/(election|president|congress|senate|policy|politic)/.test(lowered)) return 'politics';
  if (/(movie|box office|album|oscar|grammy|celebrity)/.test(lowered)) return 'entertainment';
  return 'other';
}

function normalizeMarketData(input?: MarketResearchRequest['market_data']): NormalizedMarketData {
  const yes = asNumber(input?.current_yes_price ?? input?.current_price_yes) ?? 0.5;
  const no = asNumber(input?.current_no_price ?? input?.current_price_no) ?? (1 - yes);
  return {
    currentYesPrice: clampProbability(yes),
    currentNoPrice: clampProbability(no),
    volumeUsd: asNumber(input?.volume_usd ?? input?.volume),
    resolutionDate: String(input?.resolution_date || input?.resolution_time || '').trim() || undefined,
    marketId: String(input?.market_id || '').trim() || undefined,
    platform: String(input?.platform || '').trim() || undefined,
  };
}

function normalizeEnsembleData(input?: MarketResearchRequest['ensemble_data']): NormalizedEnsembleData | undefined {
  if (!input) return undefined;
  const mu = asNumber(input.mu);
  const sigma = asNumber(input.sigma);
  if (mu === undefined || sigma === undefined) return undefined;

  const sources = Array.isArray(input.sources)
    ? input.sources.map(String)
    : Object.keys(input.sources || {}).map(String);
  const probability = asNumber(input.probability ?? input.ensemble_probability);

  return {
    mu,
    sigma,
    sources,
    threshold: asNumber(input.threshold),
    thresholdUnit: String(input.threshold_unit || input.unit || '').trim() || undefined,
    probability: probability !== undefined ? clampProbability(probability) : undefined,
  };
}

function erfApprox(x: number): number {
  const sign = x < 0 ? -1 : 1;
  const absX = Math.abs(x);
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;
  const t = 1 / (1 + p * absX);
  const y = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-absX * absX);
  return sign * y;
}

function normalCdf(x: number): number {
  return 0.5 * (1 + erfApprox(x / Math.sqrt(2)));
}

function computeWeatherCdfProbability(ensemble: NormalizedEnsembleData): number | undefined {
  if (typeof ensemble.threshold !== 'number') {
    return undefined;
  }
  const sigma = Math.max(Math.abs(ensemble.sigma), 1e-6);
  const z = (ensemble.threshold - ensemble.mu) / sigma;
  return clampProbability(1 - normalCdf(z));
}

function computeEnsembleReferenceProbability(ensemble?: NormalizedEnsembleData): number | undefined {
  if (!ensemble) return undefined;
  if (typeof ensemble.probability === 'number') {
    return clampProbability(ensemble.probability);
  }
  return computeWeatherCdfProbability(ensemble);
}

function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 1);
}

function bm25Rank(question: string, documents: SearchDocument[]): SearchDocument[] {
  if (documents.length <= 1) return documents;
  const queryTokens = tokenize(question);
  if (!queryTokens.length) return documents;

  const docTokens = documents.map((document) => tokenize(`${document.title} ${document.snippet} ${document.content}`));
  const avgDocLength = docTokens.reduce((sum, tokens) => sum + tokens.length, 0) / documents.length || 1;
  const docFreq = new Map<string, number>();

  for (const tokens of docTokens) {
    const seen = new Set(tokens);
    for (const token of seen) {
      docFreq.set(token, (docFreq.get(token) || 0) + 1);
    }
  }

  const k1 = 1.5;
  const b = 0.75;

  return documents
    .map((document, index) => {
      const tokens = docTokens[index];
      const counts = new Map<string, number>();
      for (const token of tokens) {
        counts.set(token, (counts.get(token) || 0) + 1);
      }

      let score = 0;
      for (const token of queryTokens) {
        const tf = counts.get(token) || 0;
        if (!tf) continue;
        const df = docFreq.get(token) || 0;
        const idf = Math.log(1 + ((documents.length - df + 0.5) / (df + 0.5)));
        const denom = tf + k1 * (1 - b + (b * tokens.length) / avgDocLength);
        score += idf * ((tf * (k1 + 1)) / denom);
      }

      return { document, score };
    })
    .sort((a, b) => b.score - a.score)
    .map((item) => item.document);
}

function dedupeDocuments(documents: SearchDocument[]): SearchDocument[] {
  const seen = new Set<string>();
  const deduped: SearchDocument[] = [];
  for (const document of documents) {
    const key = document.url || `${document.title}:${document.snippet}`;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    deduped.push(document);
  }
  return deduped;
}

function stripCodeFences(value: string): string {
  return value.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/\s*```$/, '').trim();
}

function stripThinkingBlocks(value: string): string {
  return value.replace(/<think>[\s\S]*?<\/think>/gi, '').trim();
}

function extractThinkingTrace(value: string): string | undefined {
  const matches = [...value.matchAll(/<think>([\s\S]*?)<\/think>/gi)];
  if (!matches.length) return undefined;
  const trace = matches
    .map((match) => String(match[1] || '').trim())
    .filter(Boolean)
    .join('\n\n');
  return trace || undefined;
}

function extractJsonPayload(rawValue: string): string {
  const trimmed = rawValue.trim();
  if (!trimmed) return trimmed;

  if (trimmed.startsWith('```')) {
    const fencedMatch = trimmed.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
    if (fencedMatch?.[1]) {
      return fencedMatch[1].trim();
    }
  }

  if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
    return trimmed;
  }

  const firstBrace = trimmed.indexOf('{');
  const lastBrace = trimmed.lastIndexOf('}');
  if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
    return trimmed.slice(firstBrace, lastBrace + 1).trim();
  }

  return trimmed;
}

function parseJsonObject(rawValue: string): any {
  const stripped = stripCodeFences(stripThinkingBlocks(rawValue.trim()));
  const primary = extractJsonPayload(stripped);
  try {
    return JSON.parse(primary);
  } catch {
    const fallback = extractJsonPayload(stripThinkingBlocks(rawValue));
    return JSON.parse(fallback);
  }
}

function estimateCostUsd(model: string, inputTokens: number, outputTokens: number): number {
  if (model === 'stepfun/step-3.5-flash:free') {
    return 0;
  }
  if (model === 'minimax/minimax-m2.7') {
    return ((inputTokens / 1000) * 0.00017) + ((outputTokens / 1000) * 0.0012);
  }
  return ((inputTokens / 1000) * 0.001) + ((outputTokens / 1000) * 0.003);
}

function usageFromResponse(response: any, model: string): PhaseUsage {
  const input = Number(response?.usage?.prompt_tokens || 0);
  const output = Number(response?.usage?.completion_tokens || 0);
  return {
    model,
    input,
    output,
    estimatedCostUsd: estimateCostUsd(model, input, output),
  };
}

function extractWeatherContext(question: string): { city: string; dateLabel: string } {
  const normalized = question.replace(/\s+/g, ' ').trim();
  const cityMatch =
    normalized.match(/will\s+(.+?)\s+high\b/i) ||
    normalized.match(/for\s+(.+?)\s+(?:on|by)\b/i) ||
    normalized.match(/in\s+(.+?)\s+(?:on|by)\b/i);
  const dateMatch =
    normalized.match(/\b(today|tomorrow)\b/i) ||
    normalized.match(/\bon\s+([A-Za-z]+\s+\d{1,2}(?:,\s*\d{4})?|\d{4}-\d{2}-\d{2})\b/i);

  return {
    city: (cityMatch?.[1] || 'local').replace(/[?.,]+$/g, '').trim(),
    dateLabel: (dateMatch?.[1] || '').replace(/[?.,]+$/g, '').trim(),
  };
}

function buildWeatherBackgroundKey(question: string): string | undefined {
  const { city } = extractWeatherContext(question);
  const normalized = city.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
  return normalized || undefined;
}

function deterministicQueries(question: string, marketType: Exclude<MarketType, 'auto'>): string[] {
  if (marketType === 'crypto') {
    return [
      `${question} latest price and catalysts`,
      `${question} market consensus`,
    ];
  }
  return [
    `${question} latest developments`,
    `${question} consensus and evidence`,
  ];
}

function deterministicWeatherQueries(question: string): string[] {
  const { city, dateLabel } = extractWeatherContext(question);
  const datePart = dateLabel ? ` ${dateLabel}` : '';
  return [
    `${city} weather forecast high temperature${datePart} site:weather.gov`,
    `${city} weather forecast high temperature${datePart} site:wunderground.com OR site:accuweather.com`,
  ];
}

async function generateSearchQueries(
  question: string,
  marketType: Exclude<MarketType, 'auto'>,
  searchDepth: SearchDepth,
): Promise<{ queries: string[]; usage?: PhaseUsage }> {
  if (marketType === 'weather') {
    return { queries: deterministicWeatherQueries(question).slice(0, 2) };
  }

  const client = getOpenRouterClient();
  const model = MODEL_ROUTES.decomposition;
  const maxQueries = SEARCH_LIMITS[searchDepth].queries;

  try {
    const response = await client.chat.completions.create({
      model,
      messages: [
        {
          role: 'system',
          content: 'Generate 1-2 focused web search queries for a prediction market. Return JSON only: {"queries":["..."]}. Keep the queries concise and high-signal.',
        },
        {
          role: 'user',
          content: `Market type: ${marketType}\nQuestion: ${question}\nReturn at most ${maxQueries} search queries.`,
        },
      ],
      response_format: { type: 'json_object' },
      temperature: 0.2,
      max_tokens: 180,
    });

    const parsed = parseJsonObject(String(response.choices[0]?.message?.content || '{}'));
    const queries = Array.isArray(parsed?.queries)
      ? parsed.queries.map((item: unknown) => String(item).trim()).filter(Boolean).slice(0, maxQueries)
      : [];

    return {
      queries: queries.length ? queries : deterministicQueries(question, marketType).slice(0, maxQueries),
      usage: usageFromResponse(response, model),
    };
  } catch {
    return { queries: deterministicQueries(question, marketType).slice(0, maxQueries) };
  }
}

async function runTargetedSearch(
  question: string,
  queries: string[],
  searchDepth: SearchDepth,
): Promise<{ documents: SearchDocument[]; usage?: PhaseUsage }> {
  const perQuerySources = SEARCH_LIMITS[searchDepth].perQuerySources;
  const selectedSources = SEARCH_LIMITS[searchDepth].selectedSources;
  const searchResults = await Promise.allSettled(queries.map((query) => searchSearXNG(query)));

  const rawDocuments: SearchDocument[] = [];
  for (const result of searchResults) {
    if (result.status !== 'fulfilled') continue;
    for (const source of result.value.slice(0, perQuerySources)) {
      rawDocuments.push({
        url: String(source.url || ''),
        title: String(source.title || 'Untitled source'),
        snippet: String(source.content || '').trim().slice(0, 280),
        content: String(source.content || '').trim(),
      });
    }
  }

  const deduped = dedupeDocuments(rawDocuments);
  if (!deduped.length) {
    return { documents: [] };
  }

  try {
    const crawled = await crawlUrls(
      deduped.map((document) => document.url).filter(Boolean),
      { query: question, timeout: 20000 },
    );
    const crawlByUrl = new Map<string, string>();
    for (const item of crawled) {
      const content = String(item.fit_markdown || item.markdown || '').trim();
      if (item.success && content.length) {
        crawlByUrl.set(item.url, content.slice(0, 5000));
      }
    }
    for (const document of deduped) {
      const crawledContent = crawlByUrl.get(document.url);
      if (crawledContent) {
        document.content = crawledContent;
        document.snippet = crawledContent.slice(0, 280);
      }
    }
  } catch {
    // Crawl4AI unavailable — proceed with SearXNG snippets.
  }

  const ranked = bm25Rank(question, deduped).slice(0, selectedSources);
  if (!ranked.length) {
    return { documents: [] };
  }

  const client = getOpenRouterClient();
  const model = MODEL_ROUTES.search_synthesis;
  const sourceBlock = ranked
    .map((document, index) => `[${index + 1}] ${document.title}\nURL: ${document.url}\n${document.content || document.snippet}`)
    .join('\n\n---\n\n');

  try {
    const response = await client.chat.completions.create({
      model,
      messages: [
        {
          role: 'system',
          content: 'Extract only the most decision-relevant facts from the supplied sources. Return 4-8 concise bullet-style facts as plain text.',
        },
        {
          role: 'user',
          content: `Question: ${question}\n\nSources:\n${sourceBlock}`,
        },
      ],
      temperature: 0.2,
      max_tokens: 600,
    });

    const facts = String(response.choices[0]?.message?.content || '').trim();
    if (facts) {
      ranked[0].content = `${facts}\n\n${ranked[0].content}`.slice(0, 5000);
    }

    return { documents: ranked, usage: usageFromResponse(response, model) };
  } catch {
    return { documents: ranked };
  }
}

function buildProbabilityPrompt(params: {
  question: string;
  marketType: Exclude<MarketType, 'auto'>;
  marketData: NormalizedMarketData;
  ensembleData?: NormalizedEnsembleData;
  ensembleProbability?: number;
  calibrationData?: MarketResearchRequest['calibration_data'];
  extractedFacts: string[];
  searchQueriesUsed: string[];
  sources: SearchDocument[];
  weatherBackground?: string | null;
}): string {
  const sourceSummary = params.sources.map((source) => ({
    title: source.title,
    url: source.url,
    snippet: source.snippet,
  }));

  return [
    MARKET_PROMPTS.probabilitySynthesis,
    'Return ONLY a raw JSON object. Do NOT wrap it in markdown code fences.',
    'Do NOT include any text before or after the JSON.',
    'If ensemble forecast data is provided, anchor heavily to it — your probability should not deviate more than 10 percentage points from the ensemble probability unless you have very strong evidence from the search results.',
    '',
    `Market question: ${params.question}`,
    `Market type: ${params.marketType}`,
    `Market data: ${JSON.stringify({
      current_yes_price: params.marketData.currentYesPrice,
      current_no_price: params.marketData.currentNoPrice,
      volume_usd: params.marketData.volumeUsd,
      resolution_date: params.marketData.resolutionDate,
      market_id: params.marketData.marketId,
      platform: params.marketData.platform,
    }, null, 2)}`,
    `Ensemble data: ${JSON.stringify(params.ensembleData ? {
      mu: params.ensembleData.mu,
      sigma: params.ensembleData.sigma,
      sources: params.ensembleData.sources,
      threshold: params.ensembleData.threshold,
      threshold_unit: params.ensembleData.thresholdUnit,
      probability: params.ensembleProbability,
    } : null, null, 2)}`,
    `Calibration data: ${JSON.stringify(params.calibrationData || null, null, 2)}`,
    `Weather background context: ${JSON.stringify(params.weatherBackground || null)}`,
    `Search queries used: ${JSON.stringify(params.searchQueriesUsed)}`,
    `Extracted facts: ${JSON.stringify(params.extractedFacts, null, 2)}`,
    `Source snippets: ${JSON.stringify(sourceSummary, null, 2)}`,
  ].join('\n');
}

async function synthesizeProbability(
  prompt: string,
  model: string,
  retry = false,
): Promise<{
  probability: number;
  confidence: 'low' | 'medium' | 'high';
  reasoning: string;
  reasoningTrace?: string;
  riskFactors: string[];
  usage: PhaseUsage;
  modelUsed: string;
  pathUsed: 'direct-minimax' | 'openrouter-fetch' | 'openrouter-sdk-fallback' | 'openrouter-sdk';
}> {
  const client = getOpenRouterClient();
  const systemPrompt = retry
    ? `${MARKET_PROMPTS.probabilitySynthesisRepair}\nReturn ONLY a raw JSON object. Do NOT wrap it in markdown code fences. Do NOT include any text before or after the JSON.`
    : `You are a prediction market analyst. Return ONLY a raw JSON object with keys probability, confidence, reasoning, risk_factors. Do NOT wrap it in markdown code fences. Do NOT include any text before or after the JSON.`;
  const temperature = retry ? 0 : 0.2;
  const maxTokens = retry ? 400 : 700;

  try {
    console.log(`[market-research] probability synthesis model=${model} retry=${retry}`);
    const isMiniMaxModel = model.toLowerCase().includes('minimax');
    let raw = '';
    let usage: PhaseUsage = {
      model,
      input: 0,
      output: 0,
      estimatedCostUsd: 0,
    };
    let reasoningTrace: string | undefined;
    let modelUsed = model;
    let pathUsed: 'direct-minimax' | 'openrouter-fetch' | 'openrouter-sdk-fallback' | 'openrouter-sdk' = 'openrouter-sdk';

    if (isMiniMaxModel) {
      const miniMaxAttempts = [
        () => callDirectMiniMaxSynthesis(systemPrompt, prompt, {
          model,
          temperature,
          max_tokens: maxTokens,
        }),
        () => callOpenRouterMiniMaxSynthesis(systemPrompt, prompt, {
          model,
          temperature,
          max_tokens: maxTokens,
        }),
      ];

      let parsedFromMiniMax: any = null;
      for (const attempt of miniMaxAttempts) {
        const miniMaxResult = await attempt();
        if (!miniMaxResult) {
          continue;
        }
        raw = miniMaxResult.content;
        reasoningTrace =
          (typeof (miniMaxResult as { reasoning?: unknown }).reasoning === 'string'
            ? String((miniMaxResult as { reasoning?: unknown }).reasoning || '').trim()
            : '')
          || extractThinkingTrace(raw);
        modelUsed = miniMaxResult.modelUsed;
        pathUsed = miniMaxResult.path;
        usage = {
          model: modelUsed,
          input: miniMaxResult.usage.input,
          output: miniMaxResult.usage.output,
          estimatedCostUsd: estimateCostUsd(modelUsed, miniMaxResult.usage.input, miniMaxResult.usage.output),
        };
        console.log(`[market-research] synthesis path used=${pathUsed} model=${modelUsed}`);
        if (process.env.NEXUS_DEBUG === 'true') {
          console.log(`[market-research] raw synthesis response (${modelUsed}): ${raw.slice(0, 500)}`);
        }
        try {
          parsedFromMiniMax = parseJsonObject(raw);
          break;
        } catch (parseError: any) {
          const message = parseError instanceof Error ? parseError.message : String(parseError);
          console.error(`[market-research] synthesis parse failed after ${pathUsed}: ${message}`);
        }
      }

      if (parsedFromMiniMax) {
        const probability = asNumber(parsedFromMiniMax?.probability);
        const reasoning = String(parsedFromMiniMax?.reasoning || '').trim();
        if (probability === undefined || !reasoning) {
          throw new Error('invalid_json');
        }
        return {
          probability: clampProbability(probability),
          confidence: normalizeConfidence(parsedFromMiniMax?.confidence),
          reasoning,
          reasoningTrace,
          riskFactors: Array.isArray(parsedFromMiniMax?.risk_factors)
            ? parsedFromMiniMax.risk_factors.map((item: unknown) => String(item).trim()).filter(Boolean)
            : [],
          usage,
          modelUsed,
          pathUsed,
        };
      } else {
        const fallbackModel = MODEL_ROUTES.default;
        console.warn(`[market-research] MiniMax direct/raw paths failed; falling back to SDK model=${fallbackModel}`);
        const response = await client.chat.completions.create({
          model: fallbackModel,
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: prompt },
          ],
          temperature,
          max_tokens: maxTokens,
        });
        raw = String(response.choices[0]?.message?.content || '').trim();
        reasoningTrace = extractThinkingTrace(raw);
        modelUsed = fallbackModel;
        pathUsed = 'openrouter-sdk-fallback';
        usage = usageFromResponse(response, fallbackModel);
        console.log(`[market-research] synthesis path used=${pathUsed} model=${modelUsed}`);
      }
    } else {
      const response = await client.chat.completions.create({
        model,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: prompt },
        ],
        temperature,
        max_tokens: maxTokens,
      });
      raw = String(response.choices[0]?.message?.content || '').trim();
      reasoningTrace = extractThinkingTrace(raw);
      modelUsed = model;
      pathUsed = 'openrouter-sdk';
      usage = usageFromResponse(response, model);
      console.log(`[market-research] synthesis path used=${pathUsed} model=${modelUsed}`);
    }

    if (process.env.NEXUS_DEBUG === 'true') {
      console.log(`[market-research] raw synthesis response (${modelUsed}): ${raw.slice(0, 500)}`);
    }

    const parsed = parseJsonObject(raw);
    const probability = asNumber(parsed?.probability);
    const reasoning = String(parsed?.reasoning || '').trim();
    if (probability === undefined || !reasoning) {
      throw new Error('invalid_json');
    }

    return {
      probability: clampProbability(probability),
      confidence: normalizeConfidence(parsed?.confidence),
      reasoning,
      reasoningTrace,
      riskFactors: Array.isArray(parsed?.risk_factors)
        ? parsed.risk_factors.map((item: unknown) => String(item).trim()).filter(Boolean)
        : [],
      usage,
      modelUsed,
      pathUsed,
    };
  } catch (error: any) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[market-research] synthesis failure model=${model} retry=${retry}: ${message}`);
    throw error;
  }
}

function buildEdgeAssessment(
  probability: number,
  marketYesPrice: number,
  riskFactors: string[] = [],
): MarketResearchResponse['edge_assessment'] {
  const rawEdgeBps = Math.round((probability - marketYesPrice) * 10000);
  const adjustedEdgeBps = rawEdgeBps - 200;
  let recommendation: MarketResearchResponse['edge_assessment']['recommendation'] = 'HOLD';

  if (adjustedEdgeBps > 500) recommendation = 'STRONG_BUY_YES';
  else if (adjustedEdgeBps > 200) recommendation = 'BUY_YES';
  else if (adjustedEdgeBps < -500) recommendation = 'STRONG_BUY_NO';
  else if (adjustedEdgeBps < -200) recommendation = 'BUY_NO';

  return {
    model_probability: probability,
    market_probability: marketYesPrice,
    raw_edge_bps: rawEdgeBps,
    adjusted_edge_bps: adjustedEdgeBps,
    recommendation,
    risk_factors: riskFactors,
  };
}

function applyEnsembleOverride(
  finalProbability: number,
  ensembleData?: NormalizedEnsembleData,
): { probability: number; ensembleOverrideTriggered: boolean; ensembleReferenceProbability?: number } {
  const ensembleReferenceProbability = computeEnsembleReferenceProbability(ensembleData);
  if (ensembleReferenceProbability === undefined) {
    return {
      probability: finalProbability,
      ensembleOverrideTriggered: false,
      ensembleReferenceProbability,
    };
  }

  const deviation = Math.abs(finalProbability - ensembleReferenceProbability);
  if (deviation > 0.2) {
    console.log(`Ensemble override: model=${finalProbability.toFixed(3)}, ensemble=${ensembleReferenceProbability.toFixed(3)}, deviation=${deviation.toFixed(3)}`);
    return {
      probability: ensembleReferenceProbability,
      ensembleOverrideTriggered: true,
      ensembleReferenceProbability,
    };
  }

  return {
    probability: finalProbability,
    ensembleOverrideTriggered: false,
    ensembleReferenceProbability,
  };
}

export async function runMarketResearch(request: MarketResearchRequest): Promise<MarketResearchResponse> {
  const startedAt = Date.now();
  const marketType = inferMarketType(request.question, request.market_type, request.ensemble_data);
  const marketData = normalizeMarketData(request.market_data);
  const ensembleData = normalizeEnsembleData(request.ensemble_data);
  const searchDepth = (request.search_depth || 'standard') as SearchDepth;
  const probabilityModel = request.model?.trim() || MODEL_ROUTES.probability_synthesis;
  const cacheKey = buildMarketCacheKey(request.question, request.market_data || {}, request.ensemble_data || {});

  const cached = getCachedMarketResearch(cacheKey, marketType, marketData.currentYesPrice);
  if (cached) {
    return cached as MarketResearchResponse;
  }

  const ensembleProbability = computeEnsembleReferenceProbability(ensembleData);
  const weatherBackground = marketType === 'weather'
    ? getWeatherBackground(buildWeatherBackgroundKey(request.question))
    : null;

  const shouldSkipSearch = marketType === 'weather'
    && !!ensembleData
    && Number.isFinite(ensembleData.mu)
    && Number.isFinite(ensembleData.sigma)
    && typeof ensembleData.probability === 'number';

  const usages: PhaseUsage[] = [];
  let searchQueriesUsed: string[] = [];
  let documents: SearchDocument[] = [];
  let extractedFacts: string[] = [];

  if (!shouldSkipSearch) {
    const planning = await generateSearchQueries(request.question, marketType, searchDepth);
    searchQueriesUsed = planning.queries;
    if (planning.usage) usages.push(planning.usage);

    try {
      const searchPhase = await runTargetedSearch(request.question, searchQueriesUsed, searchDepth);
      documents = searchPhase.documents;
      if (searchPhase.usage) usages.push(searchPhase.usage);
      extractedFacts = documents
        .map((document) => document.content.split('\n').slice(0, 3).join(' ').trim())
        .filter(Boolean)
        .slice(0, 5);
    } catch {
      documents = [];
      extractedFacts = [];
    }
  }

  const probabilityPrompt = buildProbabilityPrompt({
    question: request.question,
    marketType,
    marketData,
    ensembleData,
    ensembleProbability,
    calibrationData: request.calibration_data,
    extractedFacts,
    searchQueriesUsed,
    sources: documents,
    weatherBackground,
  });

  let synthesis:
    | {
      probability: number;
      confidence: 'low' | 'medium' | 'high';
      reasoning: string;
      reasoningTrace?: string;
      riskFactors: string[];
      usage: PhaseUsage;
      modelUsed: string;
      pathUsed: 'direct-minimax' | 'openrouter-fetch' | 'openrouter-sdk-fallback' | 'openrouter-sdk';
    }
    | undefined;
  let lastSynthesisError: unknown;

  try {
    synthesis = await synthesizeProbability(probabilityPrompt, probabilityModel, false);
  } catch (error) {
    lastSynthesisError = error;
    try {
      synthesis = await synthesizeProbability(probabilityPrompt, probabilityModel, true);
    } catch (retryError) {
      lastSynthesisError = retryError;
      if (ensembleProbability !== undefined) {
        synthesis = {
          probability: ensembleProbability,
          confidence: 'low',
          reasoning: 'Probability synthesis failed; using the ensemble baseline directly.',
          riskFactors: [],
          usage: { model: probabilityModel, input: 0, output: 0, estimatedCostUsd: 0 },
          modelUsed: probabilityModel,
          pathUsed: 'openrouter-sdk-fallback',
        };
      } else {
        const durationMs = Date.now() - startedAt;
        const message = retryError instanceof Error
          ? retryError.message
          : (lastSynthesisError instanceof Error
            ? lastSynthesisError.message
            : 'Probability synthesis model returned invalid JSON after 2 attempts');
        throw new MarketResearchError('synthesis_failed', message, 502, durationMs);
      }
    }
  }

  usages.push(synthesis.usage);

  const llmProbability = synthesis.probability;
  const overrideResult = applyEnsembleOverride(llmProbability, ensembleData);
  const finalProbability = overrideResult.probability;
  const ensembleOverrideTriggered = overrideResult.ensembleOverrideTriggered;

  const edgeAssessment = buildEdgeAssessment(finalProbability, marketData.currentYesPrice, synthesis.riskFactors);
  const totalInput = usages.reduce((sum, usage) => sum + usage.input, 0);
  const totalOutput = usages.reduce((sum, usage) => sum + usage.output, 0);
  const totalCost = usages.reduce((sum, usage) => sum + usage.estimatedCostUsd, 0);
  const durationMs = Date.now() - startedAt;

  const response: MarketResearchResponse = {
    probability: finalProbability,
    confidence: synthesis.confidence,
    reasoning: synthesis.reasoning,
    reasoning_trace: synthesis.reasoningTrace ?? null,
    edge_assessment: edgeAssessment,
    sources: documents.slice(0, SEARCH_LIMITS[searchDepth].selectedSources).map((document) => ({
      url: document.url,
      title: document.title,
      snippet: document.snippet.slice(0, 280),
    })),
    search_queries_used: searchQueriesUsed,
    model_used: synthesis.modelUsed,
    tokens: { input: totalInput, output: totalOutput },
    tokens_used: { input: totalInput, output: totalOutput },
    duration_ms: durationMs,
    market_type: marketType,
    ensemble_override_triggered: ensembleOverrideTriggered,
    from_cache: false,
    ensemble_probability: overrideResult.ensembleReferenceProbability,
    llm_probability: llmProbability,
  };

  setCachedMarketResearch(cacheKey, marketType, marketData.currentYesPrice, response);
  appendMarketResearchCostLog({
    timestamp: new Date().toISOString(),
    question: request.question,
    market_type: marketType,
    model_used: synthesis.modelUsed,
    tokens_input: totalInput,
    tokens_output: totalOutput,
    estimated_cost_usd: Number(totalCost.toFixed(8)),
    duration_ms: durationMs,
    from_cache: false,
  });

  return response;
}
