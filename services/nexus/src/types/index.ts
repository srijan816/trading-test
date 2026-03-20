export interface Source {
  title: string;
  url: string;
  content: string;
}

export interface SubTopicQuery {
  description: string;
  queries: string[];
}

export interface SubTopicResult {
  subTopic: string;
  answer: string;
  sources: Source[];
}

export interface UsageEntry {
  model: string;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  purpose: string;
  timestamp: number;
  latencyMs?: number;
  status?: 'success' | 'error';
  errorMessage?: string;
  endpoint?: string;
  sessionId?: string;
}

export interface ResearchResult {
  id: string;
  query: string;
  report: string;
  sources: Source[];
  sourcesSearched: number;
  sourcesCited: number;
  subTopicResults: SubTopicResult[];
  totalSearches: number;
  totalSourcesRead: number;
  passes: number;
  model: string;
  timestamp: number;
  apiCalls: number;
  usage: UsageEntry[];
  gapsResolved: number;
  gapsUnresolvable: number;
  unresolvableGaps: string[];
}

export interface StreamEvent {
  type: 'status' | 'sub_topic' | 'sources' | 'gap_analysis' | 'detail' | 'token' | 'done' | 'error' | 'search_progress' | 'filter_progress' | 'extraction_progress';
  data: any;
}

export type ResearchMode = 'quick' | 'standard' | 'deep';
export type OutputLength = 'short' | 'medium' | 'long';

export interface ModelConfig {
  id: string;
  displayName: string;
  description: string;
  contextWindow: number;
  bestFor: string;
}

export interface HistoryThread {
  id: string;
  query: string;
  mode: ResearchMode;
  model: string;
  outputLength: OutputLength;
  report: string;
  sources: Source[];
  sourcesSearched?: number;
  sourcesCited?: number;
  stats: {
    totalSearches: number;
    totalSourcesRead: number;
    passes: number;
    apiCalls: number;
    duration: number;
  };
  usage: UsageEntry[];
  timestamp: number;
  sessionId?: string;
}

export type MarketType = 'weather' | 'event' | 'crypto' | 'sports' | 'politics' | 'entertainment' | 'other' | 'auto';
export type SearchDepth = 'quick' | 'standard' | 'deep';

export interface MarketDataInput {
  current_yes_price?: number;
  current_price_yes?: number;
  current_no_price?: number;
  current_price_no?: number;
  volume_usd?: number;
  volume?: number;
  resolution_date?: string;
  resolution_time?: string;
  platform?: string;
  market_id?: string;
}

export interface EnsembleDataInput {
  mu: number;
  sigma: number;
  unit?: 'celsius' | 'fahrenheit';
  threshold_unit?: string;
  sources?: string[] | Record<string, number>;
  threshold?: number;
  probability?: number;
  ensemble_probability?: number;
}

export interface CalibrationDataInput {
  recent_crps?: number;
  calibration_ratio?: number;
  grad_sigma?: number;
  suggested_sigma_adjustment?: number;
  sigma_suggestion?: string;
  city_track_record?: string;
}

export interface MarketResearchRequest {
  question: string;
  market_type?: MarketType;
  market_data: MarketDataInput;
  ensemble_data?: EnsembleDataInput;
  calibration_data?: CalibrationDataInput;
  model?: string;
  search_depth?: SearchDepth;
}

export interface MarketResearchSource {
  title: string;
  url: string;
  snippet: string;
}

export interface MarketResearchEdgeAssessment {
  model_probability: number;
  market_probability: number;
  raw_edge_bps: number;
  adjusted_edge_bps: number;
  recommendation: 'STRONG_BUY_YES' | 'BUY_YES' | 'HOLD' | 'BUY_NO' | 'STRONG_BUY_NO';
  risk_factors: string[];
  kelly_fraction?: number;
}

export interface MarketResearchResponse {
  probability: number;
  confidence: 'high' | 'medium' | 'low';
  reasoning: string;
  reasoning_trace?: string | null;
  edge_assessment: MarketResearchEdgeAssessment;
  sources: MarketResearchSource[];
  search_queries_used: string[];
  model_used: string;
  tokens?: {
    input: number;
    output: number;
  };
  tokens_used: {
    input: number;
    output: number;
  };
  duration_ms: number;
  market_type: Exclude<MarketType, 'auto'>;
  from_cache: boolean;
  ensemble_probability?: number;
  llm_probability?: number;
  ensemble_override_triggered?: boolean;
}
