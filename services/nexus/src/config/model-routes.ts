export const MODEL_ROUTES = {
  decomposition: process.env.NEXUS_DECOMPOSITION_MODEL || "stepfun/step-3.5-flash:free",
  search_synthesis: process.env.NEXUS_SEARCH_MODEL || "stepfun/step-3.5-flash:free",
  probability_synthesis: process.env.NEXUS_SYNTHESIS_MODEL || "minimax/minimax-m2.7",
  default: process.env.NEXUS_DEFAULT_MODEL || "stepfun/step-3.5-flash:free",
} as const;
