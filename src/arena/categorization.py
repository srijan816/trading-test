from __future__ import annotations

from typing import Iterable
import re


# ---------------------------------------------------------------------------
# Keyword dictionaries — each category has a tuple of phrases.
# Multi-word phrases are matched as substrings; single words use \b boundary.
# ---------------------------------------------------------------------------

WEATHER_KEYWORDS = (
    "weather",
    "temperature",
    "rain",
    "precipitation",
    "snow",
    "forecast",
    "fahrenheit",
    "celsius",
    "highest temperature",
    "lowest temperature",
    "°f",
    "°c",
    "humidity",
    "wind speed",
    "tornado",
    "hurricane",
    "typhoon",
    "heatwave",
    "heat wave",
    "cold front",
    "warm front",
    "storm",
    "blizzard",
    "drought",
    "flooding",
    "flood",
    "wildfire",
)

CRYPTO_KEYWORDS = (
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "crypto",
    "solana",
    "sol",
    "token",
    "coin",
    "blockchain",
    "defi",
    "nft",
    "halving",
    "spot etf",
    "staking",
    "layer 2",
    "polygon",
    "matic",
    "arbitrum",
    "optimism",
    "binance",
    "coinbase",
    "market cap",
    "altcoin",
    "memecoin",
    "doge",
    "xrp",
    "ripple",
    "dogecoin",
    "shiba",
    "pepe",
    "litecoin",
    "avalanche",
    "cardano",
    "polkadot",
    "uniswap",
    "airdrop",
)

POLITICS_KEYWORDS = (
    "president",
    "election",
    "congress",
    "senate",
    "house of representatives",
    "governor",
    "mayor",
    "democrat",
    "republican",
    "political",
    "legislation",
    "bill",
    "vote",
    "ballot",
    "impeach",
    "executive order",
    "supreme court",
    "cabinet",
    "administration",
    "diplomat",
    "sanctions",
    "tariff",
    "trump",
    "biden",
    "gop",
    "dnc",
    "rnc",
    "primary",
    "caucus",
    "approval rating",
    "government shutdown",
    "federal",
    "state legislature",
    "veto",
    "filibuster",
    "electoral",
    "swing state",
    "polling",
    "midterm",
    "inauguration",
    "pardon",
    "confirmation hearing",
    "speaker of the house",
    "white house",
    "secretary of state",
    "attorney general",
)

ECONOMICS_KEYWORDS = (
    "gdp",
    "inflation",
    "cpi",
    "interest rate",
    "fed",
    "federal reserve",
    "unemployment",
    "jobs report",
    "recession",
    "stock",
    "s&p",
    "s&p 500",
    "nasdaq",
    "dow",
    "ipo",
    "earnings",
    "revenue",
    "trade deficit",
    "debt ceiling",
    "treasury",
    "bond",
    "yield",
    "commodity",
    "oil price",
    "gold price",
    "housing",
    "real estate",
    "merger",
    "acquisition",
    "market crash",
    "bull market",
    "bear market",
    "rate cut",
    "rate hike",
    "quantitative easing",
    "consumer confidence",
    "ppi",
    "payroll",
    "fomc",
    "ecb",
)

SPORTS_KEYWORDS = (
    # Leagues
    "nba",
    "nfl",
    "mlb",
    "nhl",
    "mls",
    "ufc",
    "mma",
    "pga",
    "wta",
    "atp",
    "f1",
    "formula 1",
    "nascar",
    "wnba",
    "ncaa",
    "epl",
    "la liga",
    "serie a",
    "bundesliga",
    "ligue 1",
    "premier league",
    "champions league",
    "europa league",
    "uefa",
    "fifa",
    # Sports
    "soccer",
    "football",
    "basketball",
    "baseball",
    "hockey",
    "tennis",
    "golf",
    "boxing",
    "wrestling",
    "cricket",
    "rugby",
    # Competition terms
    "championship",
    "playoff",
    "playoffs",
    "finals",
    "semifinal",
    "super bowl",
    "world series",
    "stanley cup",
    "world cup",
    "olympic",
    "olympics",
    "medal",
    "tournament",
    "league",
    "cup",
    "derby",
    "grand slam",
    "march madness",
    # Game/match terms
    "match",
    "game",
    "team",
    "player",
    "score",
    "touchdown",
    "goal",
    "assists",
    "mvp",
    "draft",
    "trade deadline",
    "injury",
    "coach",
    "batting",
    "rushing",
    "passing",
    "rebounds",
    "steals",
    "blocks",
    "three pointers",
    "field goals",
    "free throws",
    "strikeout",
    "home run",
    "pitcher",
    "quarterback",
    "halftime",
    "overtime",
    "penalty",
    # Betting/prop patterns
    "points scored",
    "wins by",
    "over under",
    "spread",
    "moneyline",
    # Teams and identifiers commonly seen
    "celtics",
    "lakers",
    "warriors",
    "knicks",
    "nets",
    "bulls",
    "heat",
    "bucks",
    "76ers",
    "sixers",
    "cavaliers",
    "pistons",
    "pacers",
    "hawks",
    "hornets",
    "magic",
    "raptors",
    "wizards",
    "nuggets",
    "timberwolves",
    "thunder",
    "grizzlies",
    "pelicans",
    "spurs",
    "mavericks",
    "rockets",
    "suns",
    "clippers",
    "blazers",
    "kings",
    "jazz",
    "oilers",
    "sabres",
    "lightning",
    "avalanche",
    "mammoth",
    "liverpool",
    "manchester united",
    "manchester city",
    "arsenal",
    "chelsea",
    "tottenham",
    "barcelona",
    "real madrid",
    "villarreal",
    "juventus",
    "bayern",
    "inter milan",
    "ac milan",
    "psg",
    # NBA cities / short names commonly used in prop bets
    "cleveland",
    "houston",
    "detroit",
    "minnesota",
    "denver",
    "golden state",
    "portland",
    "orlando",
    "philadelphia",
    "san antonio",
    "milwaukee",
    "phoenix",
    "sacramento",
    "dallas",
    "memphis",
    "indiana",
    "charlotte",
    "oklahoma city",
    "new orleans",
    # NCAA / March Madness schools
    "texas tech",
    "kansas",
    "uconn",
    "purdue",
    "iowa st",
    "kentucky",
    "arkansas",
    "michigan st",
    "duke",
    "gonzaga",
    "north carolina",
    "baylor",
    "tennessee",
    "illinois",
    "clemson",
    "nebraska",
    "vanderbilt",
    "texas a&m",
    "byu",
    "saint mary",
    "dayton",
    "ucla",
    "sweet sixteen",
    "elite eight",
    "final four",
    # Tennis players commonly in prop bets
    "swiatek",
    "sabalenka",
    "gauff",
    "djokovic",
    "sinner",
    "alcaraz",
    "medvedev",
    "zverev",
    "rublev",
    "ruud",
    "fritz",
    "draper",
    "lehecka",
    "fils",
    "anisimova",
    "putintseva",
    "tsitsipas",
    "dimitrov",
    "opelka",
    "osaka",
    "keys",
    "sakkari",
    "kostyuk",
    "tauson",
    "korda",
    "paul",
    # Additional NBA/NCAA cities
    "boston",
    "atlanta",
    "miami",
    "auburn",
    "wake forest",
    "nevada",
    "tulsa",
    "san diego st",
    "lehigh",
    "mcneese",
    "akron",
    "hofstra",
    "siena",
    "ucf",
    "high point",
    "penn",
    "saint louis",
    "santa clara",
    # NHL teams
    "wild",
    "canucks",
    "kraken",
    "sharks",
    "bruins",
    "rangers",
    "penguins",
    "capitals",
    "panthers",
    "hurricanes",
    "maple leafs",
    "canadiens",
    "red wings",
    "blackhawks",
    "flames",
    "senators",
    "islanders",
    "flyers",
    "predators",
    "stars",
    "jets",
    "blue jackets",
    "coyotes",
    "devils",
    # UFC / MMA fighters commonly in prop bets
    "evloev",
    "michael page",
    "nathaniel wood",
    "ufc",
)

ENTERTAINMENT_KEYWORDS = (
    "movie",
    "film",
    "oscar",
    "oscars",
    "emmy",
    "grammy",
    "tony award",
    "album",
    "song",
    "artist",
    "actor",
    "actress",
    "director",
    "box office",
    "streaming",
    "netflix",
    "disney",
    "tv show",
    "series",
    "season",
    "premiere",
    "celebrity",
    "award",
    "nomination",
    "concert",
    "tour",
    "music",
    "marvel",
    "star wars",
    "anime",
    "bestseller",
    "gta vi",
    "gta 6",
    "video game",
    "gaming",
    "twitch",
    "youtube",
    "tiktok",
    "viral",
    "spotify",
    "hbo",
    "reality tv",
    "bachelor",
    "survivor",
    "american idol",
    "grammy",
)

SCIENCE_TECH_KEYWORDS = (
    "artificial intelligence",
    "spacex",
    "nasa",
    "rocket",
    "satellite",
    "climate change",
    "carbon emission",
    "vaccine",
    "fda",
    "drug approval",
    "clinical trial",
    "patent",
    "startup",
    "chip",
    "semiconductor",
    "quantum",
    "fusion",
    "crispr",
    "gene",
    "breakthrough",
    "discovery",
    "openai",
    "chatgpt",
    "google ai",
    "apple",
    "microsoft",
    "amazon",
    "tesla",
    "meta",
    "nvidia",
    "android",
    "iphone",
    "self driving",
    "autonomous",
    "robotics",
    "5g",
    "6g",
    "starlink",
)

LEGAL_KEYWORDS = (
    "trial",
    "verdict",
    "sentence",
    "guilty",
    "not guilty",
    "plea",
    "lawsuit",
    "court",
    "judge",
    "jury",
    "indictment",
    "investigation",
    "fbi",
    "doj",
    "sec",
    "regulatory",
    "fine",
    "settlement",
    "appeal",
    "ruling",
    "constitutional",
    "prison",
    "parole",
    "bail",
    "felony",
    "misdemeanor",
    "deposition",
    "subpoena",
)

GEOPOLITICS_KEYWORDS = (
    "war",
    "conflict",
    "ceasefire",
    "peace",
    "treaty",
    "nato",
    "united nations",
    "sanctions",
    "military",
    "invasion",
    "border",
    "refugee",
    "territory",
    "nuclear",
    "missile",
    "drone",
    "ukraine",
    "taiwan",
    "iran",
    "israel",
    "gaza",
    "middle east",
    "north korea",
    "arms deal",
    "coup",
    "insurgent",
    "terrorism",
    "hostage",
    "embargo",
    "annexation",
)


# ---------------------------------------------------------------------------
# Additional regex patterns that catch structured prop-bet formats not
# reachable via simple keyword matching.
# ---------------------------------------------------------------------------

_PLAYER_STAT_RE = re.compile(
    r"\b[a-z]+ [a-z]+:\s*\d+\+",  # "lebron james: 15+" (text is lowercased)
)
_SPREAD_RE = re.compile(
    r"\bwins? by over \d+\.?\d*\s*(?:points?|goals?)\b",
    re.IGNORECASE,
)
_OVER_UNDER_RE = re.compile(
    r"\b(?:over|under)\s+\d+\.?\d*\s+(?:points?|goals?)\s+scored\b",
    re.IGNORECASE,
)
# Multi-outcome prop bet format: "yes playername: N+,yes otherplayer: N+"
_MULTI_PROP_RE = re.compile(
    r"(?:yes\s+[a-z][\w' .-]+:\s*\d+\+[,\s]*){2,}",
)

# ---------------------------------------------------------------------------
# Market format detection regexes
# ---------------------------------------------------------------------------
_MULTI_OUTCOME_RE = re.compile(
    r"(?:(?:yes|no)\s+[^,]{2,}[,]\s*){2,}(?:yes|no)\s+[^,]{2,}",
    re.IGNORECASE,
)
_NUMERIC_BRACKET_RE = re.compile(
    r"(?:between\s+\d+[\s-]+\d+|(?:over|under|above|below|at or above|at or below|or higher|or lower)\s+\d+\.?\d*)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Crypto disambiguation — names that are also non-crypto entities
# ---------------------------------------------------------------------------
_CRYPTO_AMBIGUOUS = {
    "solana": "sierra",      # Solana Sierra = tennis player
    "avalanche": "col",      # Colorado Avalanche = NHL team
}

# ---------------------------------------------------------------------------
# Category definitions (order matters only for tie-breaking)
# ---------------------------------------------------------------------------

_CATEGORIES: list[tuple[str, tuple[str, ...]]] = [
    ("weather", WEATHER_KEYWORDS),
    ("crypto", CRYPTO_KEYWORDS),
    ("politics", POLITICS_KEYWORDS),
    ("economics", ECONOMICS_KEYWORDS),
    ("sports", SPORTS_KEYWORDS),
    ("entertainment", ENTERTAINMENT_KEYWORDS),
    ("science_tech", SCIENCE_TECH_KEYWORDS),
    ("legal", LEGAL_KEYWORDS),
    ("geopolitics", GEOPOLITICS_KEYWORDS),
]


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _normalize_text(question: str, extra: str = "", tags: Iterable[str] | None = None) -> str:
    tag_text = " ".join(tag for tag in (tags or []) if tag)
    combined = f"{question} {extra} {tag_text}".lower()
    combined = combined.replace("-", " ")
    combined = re.sub(r"\s+", " ", combined).strip()
    return combined


def _count_matches(text: str, phrases: tuple[str, ...]) -> int:
    """Return the number of distinct phrases from *phrases* found in *text*."""
    hits = 0
    for phrase in phrases:
        if " " in phrase:
            if phrase in text:
                hits += 1
        else:
            if re.search(rf"\b{re.escape(phrase)}\b", text):
                hits += 1
    return hits


def _contains_any(text: str, phrases: tuple[str, ...]) -> bool:
    return _count_matches(text, phrases) > 0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_market_format(question: str) -> str:
    """Classify the market question format.

    Returns one of: ``"binary"``, ``"multi_outcome"``, ``"numeric_bracket"``,
    or ``"unknown"``.
    """
    text = question.strip()
    text_lower = text.lower()

    # Multi-outcome: multiple comma-separated "yes …" / "no …" items
    if _MULTI_OUTCOME_RE.search(text_lower):
        return "multi_outcome"

    # Also catch simpler comma-separated yes/no lists (3+ items)
    yes_no_items = re.split(r"\s*,\s*", text_lower)
    if len(yes_no_items) >= 3 and all(
        item.startswith("yes ") or item.startswith("no ") for item in yes_no_items
    ):
        return "multi_outcome"

    # Numeric bracket: contains range / threshold as part of a binary question
    if _NUMERIC_BRACKET_RE.search(text_lower):
        return "numeric_bracket"

    # Binary: starts with "Will" or contains "?" and is a single clause
    if text.startswith("Will ") or text.startswith("will "):
        return "binary"
    if "?" in text and "," not in text:
        return "binary"

    return "unknown"


def _compute_sports_bonus(text: str) -> int:
    """Return bonus score for regex-based sports detection."""
    bonus = 0
    if _PLAYER_STAT_RE.search(text):
        bonus += 3
    if _SPREAD_RE.search(text):
        bonus += 2
    if _OVER_UNDER_RE.search(text):
        bonus += 2
    if _MULTI_PROP_RE.search(text):
        bonus += 5
    return bonus


def _disambiguate_crypto(text: str, crypto_score: int, sports_score: int) -> tuple[int, int]:
    """Reduce crypto score when ambiguous keywords match non-crypto context."""
    for crypto_kw, context_hint in _CRYPTO_AMBIGUOUS.items():
        if re.search(rf"\b{re.escape(crypto_kw)}\b", text):
            # If the context hint also appears, this is NOT crypto
            if re.search(rf"\b{re.escape(context_hint)}\b", text):
                crypto_score = max(crypto_score - 1, 0)
                sports_score += 1
            # If the text is multi-outcome format (prop bet), likely sports
            elif detect_market_format(text) == "multi_outcome":
                crypto_score = max(crypto_score - 1, 0)
                sports_score += 1
    return crypto_score, sports_score


def categorize_market(
    question: str,
    extra: str = "",
    tags: Iterable[str] | None = None,
    current_category: str | None = None,
) -> str:
    """Assign a primary category using a keyword-scoring system.

    Counts keyword matches for every category and returns the one with the
    highest score.  Ties are broken by the category order in ``_CATEGORIES``
    (more specific categories appear earlier).
    """
    text = _normalize_text(question, extra=extra, tags=tags)

    sports_bonus = _compute_sports_bonus(text)

    scores: dict[str, int] = {}
    for category, keywords in _CATEGORIES:
        score = _count_matches(text, keywords)
        if category == "sports":
            score += sports_bonus
        scores[category] = score

    # Disambiguate crypto vs sports for ambiguous keywords
    scores["crypto"], scores["sports"] = _disambiguate_crypto(
        text, scores["crypto"], scores["sports"]
    )

    # TASK 2 heuristic: multi_outcome format with 3+ items → almost certainly sports
    fmt = detect_market_format(question)
    if fmt == "multi_outcome":
        items = [i.strip() for i in question.split(",") if i.strip()]
        if len(items) >= 3:
            scores["sports"] = max(scores["sports"], 1) + 3

    best_category = "event"
    best_score = 0
    for category, score in scores.items():
        if score > best_score:
            best_score = score
            best_category = category

    if best_score == 0:
        if current_category in {cat for cat, _ in _CATEGORIES}:
            return current_category
        return "event"

    return best_category


def categorize_market_detailed(
    question: str,
    extra: str = "",
    tags: Iterable[str] | None = None,
    current_category: str | None = None,
) -> tuple[str, str | None]:
    """Return ``(primary_category, secondary_category)``."""
    text = _normalize_text(question, extra=extra, tags=tags)

    sports_bonus = _compute_sports_bonus(text)

    score_map: dict[str, int] = {}
    for category, keywords in _CATEGORIES:
        score = _count_matches(text, keywords)
        if category == "sports":
            score += sports_bonus
        score_map[category] = score

    score_map["crypto"], score_map["sports"] = _disambiguate_crypto(
        text, score_map["crypto"], score_map["sports"]
    )

    fmt = detect_market_format(question)
    if fmt == "multi_outcome":
        items = [i.strip() for i in question.split(",") if i.strip()]
        if len(items) >= 3:
            score_map["sports"] = max(score_map["sports"], 1) + 3

    scores: list[tuple[str, int]] = sorted(
        score_map.items(), key=lambda item: item[1], reverse=True
    )

    primary = scores[0][0] if scores[0][1] > 0 else "event"
    secondary = None
    if len(scores) > 1 and scores[1][1] > 0 and scores[1][1] >= scores[0][1] * 0.5:
        secondary = scores[1][0]

    if primary == "event" and current_category in {cat for cat, _ in _CATEGORIES}:
        primary = current_category

    return primary, secondary
