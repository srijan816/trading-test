export interface TierConfig {
  maxSubTopics: number;
  queriesPerSubTopic: number;
  crawl4aiSubTopics: number;
  maxGapFillPasses: number;
  contentExtractionCount: number;
  useEmbeddingFilter: boolean;
  synthesisModel: string;
  maxSources: number;
}

export const RESEARCH_TIERS: Record<string, TierConfig> = {
  quick: {
    maxSubTopics: 1,
    queriesPerSubTopic: 2,
    crawl4aiSubTopics: 0,
    maxGapFillPasses: 0,
    contentExtractionCount: 0,
    useEmbeddingFilter: false,
    synthesisModel: 'stepfun/step-3.5-flash:free',
    maxSources: 15,
  },
  standard: {
    maxSubTopics: 3,
    queriesPerSubTopic: 2,
    crawl4aiSubTopics: 2,
    maxGapFillPasses: 1,
    contentExtractionCount: 5,
    useEmbeddingFilter: true,
    synthesisModel: 'stepfun/step-3.5-flash:free',
    maxSources: 30,
  },
  deep: {
    maxSubTopics: 5,
    queriesPerSubTopic: 4,
    crawl4aiSubTopics: 5,
    maxGapFillPasses: 2,
    contentExtractionCount: 10,
    useEmbeddingFilter: true,
    synthesisModel: 'stepfun/step-3.5-flash:free',
    maxSources: 50,
  },
};
