import { v4 as uuidv4 } from 'uuid';
import { getOpenRouterClient } from './openrouter';
import { searchSearXNGMultiQuery } from './vane-client';
import { filterSourcesByRules, scoreSourcesByRelevance, ScoredSource } from './source-filter';
import { extractContentForTopSources } from './content-extractor';
import { SubTopicQuery, SubTopicResult, ResearchResult, StreamEvent, UsageEntry, OutputLength } from '@/types';
import { SYSTEM_PROMPTS } from './prompts';
import { RESEARCH_TIERS, TierConfig } from '@/config/research-tiers';

type StreamCallback = (event: StreamEvent) => void;

const LENGTH_INSTRUCTIONS: Record<OutputLength, string> = {
  short: 'Keep the report concise: 3-5 paragraphs maximum. Focus only on the most important findings. Key Takeaways should have 3 bullet points.',
  medium: 'Write a moderately detailed report: 6-12 paragraphs. Cover all major findings with some analysis.',
  long: 'Write an extremely comprehensive and detailed report. Cover every sub-topic in depth with thorough analysis, comparisons, and nuance. Use many subheadings.',
};

const LENGTH_MAX_TOKENS: Record<OutputLength, number> = {
  short: 2000,
  medium: 5000,
  long: 10000,
};

function checkAbort(signal?: AbortSignal) {
  if (signal?.aborted) throw new Error('Research cancelled');
}

function extractUsage(response: any, model: string, purpose: string, startTime?: number, endpoint: string = 'openrouter'): UsageEntry {
  const u = response.usage || {};
  return {
    model,
    promptTokens: u.prompt_tokens || 0,
    completionTokens: u.completion_tokens || 0,
    totalTokens: u.total_tokens || (u.prompt_tokens || 0) + (u.completion_tokens || 0),
    purpose,
    timestamp: Date.now(),
    latencyMs: startTime ? Date.now() - startTime : undefined,
    status: 'success',
    endpoint,
  };
}

function getStopwords(): Set<string> {
  return new Set(['the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'shall', 'should', 'may', 'might', 'must', 'can', 'could', 'of', 'in', 'to', 'for', 'with', 'on', 'at', 'from', 'by', 'about', 'as', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'between', 'and', 'but', 'or', 'nor', 'not', 'so', 'yet', 'both', 'either', 'neither', 'each', 'every', 'all', 'any', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'only', 'own', 'same', 'than', 'too', 'very', 'just', 'because', 'if', 'when', 'where', 'how', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'you', 'your', 'he', 'him', 'his', 'she', 'her', 'it', 'its', 'they', 'them', 'their']);
}

function queryTokenOverlap(q1: string, q2: string): number {
  const stopwords = getStopwords();
  const tokens1 = q1.toLowerCase().replace(/[^a-z0-9\s]/g, '').split(/\s+/).filter(t => !stopwords.has(t) && t.length > 1);
  const tokens2 = q2.toLowerCase().replace(/[^a-z0-9\s]/g, '').split(/\s+/).filter(t => !stopwords.has(t) && t.length > 1);
  if (tokens1.length === 0 || tokens2.length === 0) return 0;
  const set2 = new Set(tokens2);
  const overlap = tokens1.filter(t => set2.has(t)).length;
  return overlap / Math.max(tokens1.length, tokens2.length);
}

function getPhase2ExtractionBudget(topicIndex: number, tierConfig: TierConfig): number {
  if (tierConfig.contentExtractionCount <= 0 || tierConfig.crawl4aiSubTopics <= 0 || topicIndex > tierConfig.crawl4aiSubTopics) {
    return 0;
  }

  const base = Math.floor(tierConfig.contentExtractionCount / tierConfig.crawl4aiSubTopics);
  const remainder = tierConfig.contentExtractionCount % tierConfig.crawl4aiSubTopics;
  return base + (topicIndex <= remainder ? 1 : 0);
}

// ============================
// PHASE 1: QUERY DECOMPOSITION
// ============================
async function decomposeQuery(
  query: string,
  tierConfig: TierConfig,
  onStream: StreamCallback,
  usageLog: UsageEntry[],
  signal?: AbortSignal
): Promise<{ subTopics: SubTopicQuery[]; researchBrief: string }> {
  onStream({ type: 'status', data: { step: 1, label: 'Planning', detail: 'Generating optimized search queries...' } });
  checkAbort(signal);

  const model = 'stepfun/step-3.5-flash:free';
  const client = getOpenRouterClient();
  const callStart = Date.now();
  const response = await client.chat.completions.create({
    model,
    messages: [
      { role: 'system', content: SYSTEM_PROMPTS.queryDecomposition },
      { role: 'user', content: `Generate sub-topics with ${tierConfig.queriesPerSubTopic} keyword queries each (max ${tierConfig.maxSubTopics} sub-topics).\n\nResearch question: ${query}` },
    ],
    temperature: 0.3,
    response_format: { type: 'json_object' },
  });

  usageLog.push(extractUsage(response, model, 'query_decomposition', callStart));
  checkAbort(signal);

  let parsed: any;
  try { parsed = JSON.parse(response.choices[0].message.content || '{}'); } catch { parsed = {}; }

  let subTopics: SubTopicQuery[] = [];
  if (Array.isArray(parsed.subTopics)) {
    subTopics = parsed.subTopics
      .slice(0, tierConfig.maxSubTopics)
      .map((st: any) => {
        if (typeof st === 'string') {
          return { description: st, queries: [st] };
        }
        return {
          description: st.description || st.topic || st.subTopic || query,
          queries: Array.isArray(st.queries) ? st.queries.slice(0, tierConfig.queriesPerSubTopic) : [st.description || query],
        };
      });
  }

  if (subTopics.length === 0) {
    subTopics = [{ description: query, queries: [query] }];
  }

  const totalQueries = subTopics.reduce((sum, st) => sum + st.queries.length, 0);
  onStream({ type: 'status', data: { step: 1, label: 'Plan ready', detail: `${subTopics.length} sub-topic(s), ${totalQueries} search queries` } });

  for (const st of subTopics) {
    onStream({ type: 'detail', data: { message: `Sub-topic: ${st.description} → ${st.queries.length} queries` } });
  }

  return { subTopics, researchBrief: parsed.researchBrief || query };
}

// ============================
// PHASE 2: SEARCH EXECUTION
// ============================
async function searchSubTopic(
  subTopic: SubTopicQuery,
  topicIndex: number,
  totalTopics: number,
  tierConfig: TierConfig,
  onStream: StreamCallback,
  usageLog: UsageEntry[],
  signal?: AbortSignal
): Promise<SubTopicResult & { rawSourceCount: number }> {
  onStream({ type: 'sub_topic', data: { topic: subTopic.description, status: 'searching', index: topicIndex, total: totalTopics } });
  checkAbort(signal);

  // Stream each query being dispatched
  for (const q of subTopic.queries) {
    onStream({ type: 'search_progress', data: { subTopic: topicIndex, query: q, engine: 'SearXNG' } });
  }

  // SearXNG: send ALL queries for this sub-topic
  const searxngStart = Date.now();
  const searxngResult = await Promise.allSettled([searchSearXNGMultiQuery(subTopic.queries, signal)]);
  checkAbort(signal);

  const searxngSources: ScoredSource[] = searxngResult[0].status === 'fulfilled' ? searxngResult[0].value : [];

  // Log SearXNG usage
  usageLog.push({
    model: 'searxng', purpose: `search: ${subTopic.description.substring(0, 50)}`,
    promptTokens: 0, completionTokens: 0, totalTokens: 0, timestamp: Date.now(),
    latencyMs: Date.now() - searxngStart, status: searxngResult[0].status === 'fulfilled' ? 'success' : 'error',
    endpoint: 'searxng',
  });

  if (searxngResult[0].status === 'rejected') {
    onStream({ type: 'detail', data: { topic: subTopic.description, message: `SearXNG error: ${searxngResult[0].reason?.message || 'unknown'}` } });
  }

  const allSources: ScoredSource[] = [...searxngSources];

  let crawl4aiEnriched = 0;
  const extractionBudget = getPhase2ExtractionBudget(topicIndex, tierConfig);
  if (extractionBudget > 0 && allSources.length > 0) {
    const extractionStart = Date.now();
    const extractionStats = await extractContentForTopSources(
      allSources,
      extractionBudget,
      signal,
      { query: subTopic.description, timeoutMs: 20000 },
    );
    crawl4aiEnriched = extractionStats.crawl4aiSucceeded;
    if (extractionStats.crawl4aiAttempts > 0) {
      usageLog.push({
        model: 'crawl4ai',
        purpose: `extract: ${subTopic.description.substring(0, 50)}`,
        promptTokens: 0,
        completionTokens: 0,
        totalTokens: 0,
        timestamp: Date.now(),
        latencyMs: Date.now() - extractionStart,
        status: extractionStats.crawl4aiSucceeded > 0 ? 'success' : 'error',
        endpoint: 'crawl4ai',
      });
    }
  }

  onStream({ type: 'search_progress', data: { subTopic: topicIndex, query: 'all', engine: 'combined', resultCount: allSources.length } });
  onStream({ type: 'sources', data: { topic: subTopic.description, count: allSources.length, searxng: searxngSources.length, crawl4ai: crawl4aiEnriched } });

  // Sub-topic synthesis
  let subAnswer = '';
  if (allSources.length > 0) {
    onStream({ type: 'detail', data: { topic: subTopic.description, message: `Synthesizing ${allSources.length} sources...` } });
    checkAbort(signal);
    try {
      const synthModel = 'stepfun/step-3.5-flash:free';
      const client = getOpenRouterClient();
      const sourcesText = allSources.slice(0, 12).map((s, i) => `[${i + 1}] ${s.title}: ${(s.extractedContent || s.content).substring(0, 600)}`).join('\n\n');
      const synthStart = Date.now();
      const synthResponse = await client.chat.completions.create({
        model: synthModel,
        messages: [
          { role: 'system', content: 'Summarize these search results into a concise answer. Include specific facts, numbers, and data points. 2-4 paragraphs.' },
          { role: 'user', content: `Topic: ${subTopic.description}\n\nResults:\n${sourcesText}` },
        ],
        temperature: 0.3,
        max_tokens: 1000,
      });
      subAnswer = synthResponse.choices[0].message.content || '';
      usageLog.push(extractUsage(synthResponse, synthModel, `sub_topic_synthesis: ${subTopic.description.substring(0, 50)}`, synthStart));
    } catch (err: any) {
      onStream({ type: 'detail', data: { topic: subTopic.description, message: `Synthesis warning: ${err.message}` } });
    }
  }

  onStream({ type: 'sub_topic', data: { topic: subTopic.description, status: 'complete', index: topicIndex, total: totalTopics, sourceCount: allSources.length } });
  return { subTopic: subTopic.description, answer: subAnswer, sources: allSources, rawSourceCount: allSources.length };
}

// ============================
// PHASE 3: FILTERING & SCORING
// ============================
async function filterAndScoreSources(
  allSources: ScoredSource[],
  query: string,
  tierConfig: TierConfig,
  onStream: StreamCallback,
  signal?: AbortSignal
): Promise<ScoredSource[]> {
  onStream({ type: 'status', data: { step: 3, label: 'Filtering sources', detail: `Evaluating ${allSources.length} raw sources...` } });
  checkAbort(signal);

  const totalRaw = allSources.length;

  // Tier 1: Rule-based filtering
  const { kept: afterRules, removed: rulesRemoved } = filterSourcesByRules(allSources, query);
  onStream({ type: 'detail', data: { message: `Rule filter: removed ${rulesRemoved}, kept ${afterRules.length}` } });

  let finalSources = afterRules;
  let afterRelevanceCount = afterRules.length;
  let topScore = 0;
  let bottomScore = 0;

  // Tier 2: Embedding-based scoring (if enabled and enough sources)
  if (tierConfig.useEmbeddingFilter && afterRules.length > 5) {
    onStream({ type: 'detail', data: { message: 'Running embedding-based relevance scoring...' } });
    checkAbort(signal);
    const { scored, dropped } = await scoreSourcesByRelevance(afterRules, query, 0.35, tierConfig.maxSources);
    finalSources = scored;
    afterRelevanceCount = scored.length;
    topScore = scored[0]?.relevanceScore || 0;
    bottomScore = scored[scored.length - 1]?.relevanceScore || 0;
    onStream({ type: 'detail', data: { message: `Relevance filter: kept ${scored.length}, dropped ${dropped} (scores: ${topScore.toFixed(2)} – ${bottomScore.toFixed(2)})` } });
  } else {
    finalSources = afterRules.slice(0, tierConfig.maxSources);
  }

  onStream({
    type: 'filter_progress',
    data: {
      totalRaw,
      afterRules: afterRules.length,
      afterRelevance: afterRelevanceCount,
      topRelevanceScore: Math.round(topScore * 100) / 100,
      bottomRelevanceScore: Math.round(bottomScore * 100) / 100,
    },
  });

  return finalSources;
}

// ============================
// PHASE 4: CONTENT EXTRACTION
// ============================
async function extractTopContent(
  sources: ScoredSource[],
  tierConfig: TierConfig,
  onStream: StreamCallback,
  usageLog: UsageEntry[],
  signal?: AbortSignal
): Promise<void> {
  if (tierConfig.contentExtractionCount <= 0) return;

  onStream({ type: 'status', data: { step: 4, label: 'Extracting content', detail: `Fetching full text from top ${tierConfig.contentExtractionCount} sources...` } });
  checkAbort(signal);

  const extractionStart = Date.now();
  const stats = await extractContentForTopSources(sources, tierConfig.contentExtractionCount, signal);
  if (stats.crawl4aiAttempts > 0) {
    usageLog.push({
      model: 'crawl4ai',
      purpose: 'phase_4_content_extraction',
      promptTokens: 0,
      completionTokens: 0,
      totalTokens: 0,
      timestamp: Date.now(),
      latencyMs: Date.now() - extractionStart,
      status: stats.crawl4aiSucceeded > 0 ? 'success' : 'error',
      endpoint: 'crawl4ai',
    });
  }
  if (stats.crawl4aiAttempts > 0) {
    onStream({ type: 'detail', data: { message: `Crawl4AI extracted ${stats.crawl4aiSucceeded}/${stats.crawl4aiAttempts} pages before fallback.` } });
  }

  onStream({
    type: 'extraction_progress',
    data: {
      attempted: stats.attempted,
      succeeded: stats.succeeded,
      avgContentLength: stats.avgContentLength,
      failed: stats.failedUrls?.length || 0,
    },
  });

  // Replace source content with extracted content where available
  for (const source of sources) {
    if (source.extractedContent) {
      source.content = source.extractedContent;
    }
  }
}

// ============================
// PHASE 5: GAP ANALYSIS
// ============================
interface GapInfo {
  description: string;
  queries: string[];
}

async function analyzeGaps(
  researchBrief: string,
  subTopicResults: SubTopicResult[],
  previousQueries: string[],
  passNumber: number,
  unresolvableGaps: string[],
  onStream: StreamCallback,
  usageLog: UsageEntry[],
  signal?: AbortSignal
): Promise<{ sufficient: boolean; gaps: GapInfo[]; newUnresolvable: string[] }> {
  onStream({ type: 'status', data: { step: 5, label: 'Evaluating', detail: `Pass ${passNumber}: Checking research completeness...` } });
  checkAbort(signal);

  const model = process.env.DEFAULT_MODEL || 'stepfun/step-3.5-flash:free';
  const client = getOpenRouterClient();
  const findingsSummary = subTopicResults.map((r, i) =>
    `## Sub-topic ${i + 1}: ${r.subTopic}\nAnswer: ${r.answer}\nSources: ${r.sources.length}`
  ).join('\n\n');

  const previousSearchNote = previousQueries.length > 0
    ? `\n\nYou have already searched with these queries:\n${previousQueries.join('\n')}\n\nYour new queries MUST target different websites, use different keywords, or approach the topic from a completely different angle. If you cannot generate queries that are meaningfully different from the previous ones, output UNRESOLVABLE for that gap.\n\nGenerate completely different search queries. If you cannot think of genuinely new queries, output {"sufficient": true, "gaps": []} instead.`
    : '';

  const previousGapDescriptions = unresolvableGaps.length > 0 || passNumber > 1
    ? `\n\nPrevious gap descriptions that were already searched for:\n${[...unresolvableGaps, ...subTopicResults.map(r => r.subTopic)].join('\n')}\n\nIf a new gap is substantially the same as a previous gap that was already searched for, mark it as UNRESOLVABLE with reason "previously searched — insufficient public data". Only output genuinely NEW gaps that require DIFFERENT search strategies.`
    : '';

  const unresolvableNote = unresolvableGaps.length > 0
    ? `\n\nThese gaps have been marked unresolvable — do not include them:\n${unresolvableGaps.join('\n')}`
    : '';

  const gapStart = Date.now();
  const response = await client.chat.completions.create({
    model,
    messages: [
      {
        role: 'system',
        content: SYSTEM_PROMPTS.gapAnalysis + `

For each gap, try these strategies (pick the most appropriate):
1. Search for user reviews/experiences on Reddit or HN instead of official platform pages
2. Use "site:" operators to target a specific domain you haven't searched yet
3. Search for the opposite to find explanatory content
4. Search for very recent results by adding the current year to the query
5. Search for comparison/review articles rather than direct platform pages` + previousSearchNote + previousGapDescriptions + unresolvableNote,
      },
      { role: 'user', content: `BRIEF:\n${researchBrief}\n\nFINDINGS:\n${findingsSummary}` },
    ],
    temperature: 0.2,
    response_format: { type: 'json_object' },
  });

  usageLog.push(extractUsage(response, model, `gap_analysis_pass_${passNumber}`, gapStart));
  checkAbort(signal);

  let parsed: any;
  try { parsed = JSON.parse(response.choices[0].message.content || '{}'); } catch { parsed = { sufficient: true, gaps: [] }; }

  // Normalize gaps to new format
  let gaps: GapInfo[] = [];
  const newUnresolvable: string[] = [];

  if (!parsed.sufficient && Array.isArray(parsed.gaps)) {
    for (const gap of parsed.gaps) {
      let gapInfo: GapInfo;
      if (typeof gap === 'string') {
        gapInfo = { description: gap, queries: [gap] };
      } else if (gap.unresolvable) {
        newUnresolvable.push(gap.description || gap.reason || 'Unknown gap');
        continue;
      } else {
        gapInfo = {
          description: gap.description || gap.gap || String(gap),
          queries: Array.isArray(gap.queries) ? gap.queries : [gap.description || String(gap)],
        };
      }

      // Check for query overlap with previous queries (>60% = reject)
      const hasTooMuchOverlap = gapInfo.queries.every((q) =>
        previousQueries.some((prev) => queryTokenOverlap(q, prev) > 0.6)
      );
      if (hasTooMuchOverlap) {
        newUnresolvable.push(gapInfo.description);
        continue;
      }

      // Check if this gap appeared in previous passes (>70% keyword overlap with unresolvable)
      const isRepeatGap = unresolvableGaps.some((ug) => queryTokenOverlap(gapInfo.description, ug) > 0.7);
      if (isRepeatGap) {
        newUnresolvable.push(gapInfo.description);
        continue;
      }

      gaps.push(gapInfo);
    }
  }

  if (gaps.length > 0) {
    onStream({
      type: 'gap_analysis',
      data: {
        pass: passNumber,
        gapsFound: gaps.length + newUnresolvable.length,
        gapsResolved: 0,
        gapsUnresolvable: newUnresolvable.length,
        gapsPending: gaps.length,
        gaps: gaps.map((g) => g.description),
      },
    });
  } else {
    onStream({ type: 'status', data: { step: 5, label: 'Evaluation complete', detail: `Research is comprehensive.${newUnresolvable.length > 0 ? ` ${newUnresolvable.length} gap(s) marked unresolvable.` : ''}` } });
  }

  return { sufficient: gaps.length === 0, gaps, newUnresolvable };
}

// ============================
// PHASE 6: FINAL SYNTHESIS
// ============================
async function synthesizeReport(
  query: string,
  researchBrief: string,
  allSubTopicResults: SubTopicResult[],
  filteredSources: ScoredSource[],
  unresolvableGaps: string[],
  outputLength: OutputLength,
  onStream: StreamCallback,
  usageLog: UsageEntry[],
  signal?: AbortSignal
): Promise<{ report: string; followUps: string[] }> {
  onStream({ type: 'status', data: { step: 6, label: 'Writing report', detail: `Synthesizing ${outputLength} report from ${filteredSources.length} sources...` } });
  checkAbort(signal);

  const model = process.env.DEFAULT_MODEL || 'stepfun/step-3.5-flash:free';
  const client = getOpenRouterClient();

  const sourcesContext = filteredSources.map((s, i) =>
    `[${i + 1}] ${s.title} (${s.url})\n${(s.extractedContent || s.content).substring(0, 2000)}`
  ).join('\n\n---\n\n');

  const findingsContext = allSubTopicResults.map((r) =>
    `### ${r.subTopic}\n${r.answer}`
  ).join('\n\n');

  const gapsNote = unresolvableGaps.length > 0
    ? `\n\nUNRESOLVED RESEARCH GAPS (mention in Research Limitations):\n${unresolvableGaps.map((g) => `- ${g}`).join('\n')}`
    : '';

  const synthStart = Date.now();
  const stream = await client.chat.completions.create({
    model,
    messages: [
      {
        role: 'system',
        content: `${SYSTEM_PROMPTS.synthesis}

OUTPUT LENGTH: ${LENGTH_INSTRUCTIONS[outputLength]}

RESEARCH BRIEF: ${researchBrief}
${gapsNote}

FINDINGS:\n${findingsContext}

SOURCES (cite as [1], [2], etc.):\n${sourcesContext}`,
      },
      { role: 'user', content: query },
    ],
    stream: true,
    temperature: 0.4,
    max_tokens: LENGTH_MAX_TOKENS[outputLength],
  });

  let fullResponse = '';
  let streamTokens = 0;
  for await (const chunk of stream) {
    checkAbort(signal);
    const token = chunk.choices[0]?.delta?.content || '';
    if (token) { fullResponse += token; streamTokens++; onStream({ type: 'token', data: token }); }
    if (chunk.usage) {
      usageLog.push(extractUsage(chunk, model, 'final_synthesis', synthStart));
    }
  }

  if (!usageLog.find((e) => e.purpose === 'final_synthesis')) {
    usageLog.push({
      model, purpose: 'final_synthesis',
      promptTokens: 0, completionTokens: streamTokens, totalTokens: streamTokens,
      timestamp: Date.now(), latencyMs: Date.now() - synthStart,
      status: 'success', endpoint: 'openrouter',
    });
  }

  // Generate follow-ups separately using findings context
  let followUps: string[] = [];
  try {
    checkAbort(signal);
    const fuModel = 'stepfun/step-3.5-flash:free';
    const fuStart = Date.now();
    const fuResponse = await client.chat.completions.create({
      model: fuModel,
      messages: [
        { role: 'system', content: SYSTEM_PROMPTS.followUpGeneration },
        { role: 'user', content: `Original query: ${query}\n\nReport:\n${fullResponse.substring(0, 3000)}` },
      ],
      temperature: 0.4,
      response_format: { type: 'json_object' },
    });
    usageLog.push(extractUsage(fuResponse, fuModel, 'follow_up_generation', fuStart));
    const fuParsed = JSON.parse(fuResponse.choices[0].message.content || '{}');
    if (Array.isArray(fuParsed.followUps)) {
      followUps = fuParsed.followUps.slice(0, 3);
    }
  } catch {
    // Follow-up generation is non-critical
  }

  return { report: fullResponse, followUps };
}

// ============================
// GITHUB VERIFICATION
// ============================
async function verifyGitHubRepos(report: string, onStream: StreamCallback): Promise<string> {
  const ghUrlRegex = /https?:\/\/github\.com\/([^\/\s\)]+)\/([^\/\s\)\]#]+)/g;
  const matches: { full: string; owner: string; repo: string }[] = [];
  let m: RegExpExecArray | null;
  while ((m = ghUrlRegex.exec(report)) !== null) {
    const owner = m[1];
    const repo = m[2].replace(/[.,;:!?]+$/, '');
    const full = m[0];
    if (!matches.find((x) => x.owner === owner && x.repo === repo)) {
      matches.push({ full, owner, repo });
    }
  }

  if (matches.length === 0) return report;

  onStream({ type: 'detail', data: { message: `Verifying ${matches.length} GitHub repo(s)...` } });

  let modified = report;
  const TWELVE_MONTHS_MS = 365 * 24 * 60 * 60 * 1000;

  for (const { full, owner, repo } of matches) {
    try {
      const res = await fetch(`https://api.github.com/repos/${owner}/${repo}`, {
        headers: { 'Accept': 'application/vnd.github.v3+json', 'User-Agent': 'NexusResearch/1.0' },
        signal: AbortSignal.timeout(5000),
      });

      if (res.status === 404) {
        modified = modified.replace(
          new RegExp(`\\[([^\\]]+)\\]\\(${full.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\)`, 'g'),
          '[repo not verified]'
        );
        onStream({ type: 'detail', data: { message: `GitHub: ${owner}/${repo} — not found, removed citation` } });
      } else if (res.ok) {
        const data = await res.json();
        const pushedAt = new Date(data.pushed_at).getTime();
        if (Date.now() - pushedAt > TWELVE_MONTHS_MS || data.archived) {
          const warning = ' *\\[repo may be archived or outdated — verify before using\\]*';
          // Only add warning once per repo URL
          if (!modified.includes(`${full}${warning}`) && !modified.includes(`(${full})${warning}`)) {
            modified = modified.replace(full, `${full} *(repo may be archived or outdated — verify before using)*`);
          }
          onStream({ type: 'detail', data: { message: `GitHub: ${owner}/${repo} — last pushed ${data.pushed_at}, flagged as outdated` } });
        }
      }
    } catch {
      // GitHub API failure is non-critical — skip verification for this repo
    }
  }

  return modified;
}

// ============================
// MAIN ORCHESTRATOR
// ============================
export async function runDeepResearch(
  query: string,
  model: string,
  outputLength: OutputLength,
  onStream: StreamCallback,
  signal?: AbortSignal,
  tier: string = 'deep',
  previousSessionContext?: { sources: ScoredSource[]; report: string }
): Promise<ResearchResult> {
  const id = uuidv4();
  const tierConfig = RESEARCH_TIERS[tier] || RESEARCH_TIERS.deep;
  let totalSearches = 0;
  let totalSourcesRead = 0;
  let allSubTopicResults: SubTopicResult[] = [];
  let allRawSources: ScoredSource[] = [];
  const startTime = Date.now();
  const usageLog: UsageEntry[] = [];
  let gapsResolved = 0;
  let gapsUnresolvable = 0;
  const unresolvableGaps: string[] = [];
  const allPreviousQueries: string[] = [];

  try {
    // Phase 1: Query decomposition
    const { subTopics, researchBrief } = await decomposeQuery(query, tierConfig, onStream, usageLog, signal);

    // Track all queries
    for (const st of subTopics) {
      allPreviousQueries.push(...st.queries);
    }

    // Phase 2: Parallel search
    onStream({ type: 'status', data: { step: 2, label: 'Searching', detail: `${subTopics.length} sub-topic(s) with ${allPreviousQueries.length} queries...` } });

    const initialResults = await Promise.allSettled(
      subTopics.map((st, i) => searchSubTopic(st, i + 1, subTopics.length, tierConfig, onStream, usageLog, signal))
    );

    for (const r of initialResults) {
      if (r.status === 'fulfilled') {
        allSubTopicResults.push(r.value);
        totalSearches += r.value.sources.length > 0 ? 2 : 1;
        totalSourcesRead += r.value.rawSourceCount;
        allRawSources.push(...(r.value.sources as ScoredSource[]));
      }
    }
    checkAbort(signal);

    onStream({ type: 'status', data: { step: 2, label: 'Search complete', detail: `${totalSourcesRead} raw sources, ${totalSearches} searches` } });

    // Phase 3: Filter & score sources
    const filteredSources = await filterAndScoreSources(allRawSources, query, tierConfig, onStream, signal);

    // Add previous session sources if doing follow-up research
    if (previousSessionContext?.sources) {
      const prevSources = previousSessionContext.sources.filter(
        (s) => !filteredSources.find((f) => f.url === s.url) && (s.relevanceScore || 0) > 0.5
      );
      filteredSources.push(...prevSources.slice(0, 10));
    }

    // Phase 4: Content extraction
    await extractTopContent(filteredSources, tierConfig, onStream, usageLog, signal);

    // Phase 5: Gap analysis (max passes from tier config)
    let pass = 1;
    while (pass <= tierConfig.maxGapFillPasses) {
      checkAbort(signal);
      const gapResult = await analyzeGaps(
        researchBrief, allSubTopicResults, allPreviousQueries,
        pass, unresolvableGaps, onStream, usageLog, signal
      );

      unresolvableGaps.push(...gapResult.newUnresolvable);
      gapsUnresolvable += gapResult.newUnresolvable.length;

      if (gapResult.sufficient || gapResult.gaps.length === 0) break;

      pass++;
      onStream({ type: 'status', data: { step: 5, label: `Gap-fill pass ${pass}`, detail: `${gapResult.gaps.length} gap(s) to fill` } });

      // Search for gap-fill queries
      const gapSubTopics: SubTopicQuery[] = gapResult.gaps.map((g) => ({
        description: g.description,
        queries: g.queries,
      }));

      for (const gst of gapSubTopics) {
        allPreviousQueries.push(...gst.queries);
      }

      const gapResults = await Promise.allSettled(
        gapSubTopics.map((gst, i) => searchSubTopic(gst, i + 1, gapSubTopics.length, tierConfig, onStream, usageLog, signal))
      );

      let gapFillSourceCount = 0;
      for (const r of gapResults) {
        if (r.status === 'fulfilled') {
          allSubTopicResults.push(r.value);
          totalSearches += 2;
          gapFillSourceCount += r.value.rawSourceCount;
          allRawSources.push(...(r.value.sources as ScoredSource[]));
        }
      }
      totalSourcesRead += gapFillSourceCount;

      // Re-filter gap-fill sources and add good ones to filtered set
      if (gapFillSourceCount > 0) {
        const newFiltered = await filterAndScoreSources(
          gapResults
            .filter((r) => r.status === 'fulfilled')
            .flatMap((r) => (r as PromiseFulfilledResult<SubTopicResult & { rawSourceCount: number }>).value.sources as ScoredSource[]),
          query, tierConfig, onStream, signal
        );
        for (const s of newFiltered) {
          if (!filteredSources.find((f) => f.url === s.url)) {
            filteredSources.push(s);
          }
        }
        gapsResolved += gapResult.gaps.length;
      }
    }

    checkAbort(signal);

    // Phase 6: Final synthesis
    const { report: rawReport, followUps } = await synthesizeReport(
      query, researchBrief, allSubTopicResults, filteredSources,
      unresolvableGaps, outputLength, onStream, usageLog, signal
    );

    // Phase 6b: Post-processing — verify GitHub repos
    const report = await verifyGitHubRepos(rawReport, onStream);

    // Count cited sources
    const citedNumbers = new Set<number>();
    const citationRegex = /\[(\d+)\]/g;
    let match;
    while ((match = citationRegex.exec(report)) !== null) {
      citedNumbers.add(parseInt(match[1], 10));
    }

    // Mark cited sources
    for (const num of citedNumbers) {
      if (num >= 1 && num <= filteredSources.length) {
        filteredSources[num - 1].cited = true;
      }
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const result: ResearchResult = {
      id, query, report,
      sources: filteredSources,
      sourcesSearched: totalSourcesRead,
      sourcesCited: citedNumbers.size,
      subTopicResults: allSubTopicResults,
      totalSearches, totalSourcesRead,
      passes: pass, model, timestamp: Date.now(),
      apiCalls: usageLog.length, usage: usageLog,
      gapsResolved, gapsUnresolvable,
      unresolvableGaps,
    };

    // Emit follow-ups as part of done data
    onStream({ type: 'status', data: { step: 7, label: 'Complete', detail: `${elapsed}s | ${totalSearches} searches | ${filteredSources.length} sources (${citedNumbers.size} cited) | ${usageLog.length} API calls` } });
    onStream({ type: 'done', data: { ...result, followUps } });
    return result;
  } catch (error: any) {
    onStream({ type: 'error', data: error.message === 'Research cancelled' ? 'Research was cancelled.' : (error.message || 'Unknown error') });
    throw error;
  }
}
