import datetime
import os
import re
import time
import logging
import feedparser
import httpx
import pandas as pd
import trafilatura
from textblob import TextBlob
import spacy

# LOGGING
os.makedirs("data", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("mediapulse")

# ─────────────────────────────────────────────
# NER MODEL — loaded once at module level
# ─────────────────────────────────────────────
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    nlp = None
    log.warning("[NER] spaCy model not found — run: python -m spacy download en_core_web_sm")

# CONFIGURATION

# MONITORING TARGETS

MONITORING_TARGETS = {
[
    # ── ACTIVE CLIENTS
        "Mastercard Foundation Africa Secondary Education",
        "Mastercard Foundation scholars",
        "Mastercard Foundation CITL",
        "Mastercard FFoundation centre for innovative teaching and learning",
        "Mastercard Foundation Transitions",
        "Mastercard Foundation Scholars Programme",
        "Mastercard Foundation COVID",
        "Mastercard Foundation Seconadry Education",
    ],

    "Safaricom": [
        "Safaricom",
        "M-Pesa",
        "MPESA",
        "Safaricom PLC",
        "Safaricom 5G",
    ],

    "Equity Group": [
        "Equity Bank",
        "Equity Group",
        "Equity Group Holdings",
        "James Mwangi",
    ],

    "KCB Group": [
        "KCB Bank",
        "KCB Group",
        "Kenya Commercial Bank",
    ],

    # ── SECTOR MONITORING — for trend intelligence ─────────────
    "Africa Fintech": [
        "Africa fintech",
        "Africa mobile money",
        "Africa digital payments",
        "Flutterwave",
        "Paystack",
        "OPay",
        "Chipper Cash",
        "Wave money",
        "Moniepoint",
    ],

    "Africa Tech & AI": [
        "Africa artificial intelligence",
        "Africa AI",
        "Africa machine learning",
        "Africa tech startup",
        "Africa deep tech",
        "African developer",
    ],

    "Africa Health": [
        "Africa health",
        "Africa malaria",
        "Africa HIV",
        "Africa maternal health",
        "Africa vaccination",
        "Africa pandemic",
        "KEMSA Kenya",
        "Africa health system",
    ],

    "Africa Education": [
        "Africa education",
        "Africa university",
        "Africa scholarship",
        "Africa EdTech",
        "Africa TVET",
        "youth employment Africa",
    ],

    "Africa Climate": [
        "Africa climate change",
        "Africa flooding",
        "Africa drought",
        "Africa renewable energy",
        "Africa solar",
        "Africa green economy",
        "Africa carbon",
    ],

    "Africa Development Finance": [
        "African Development Bank",
        "AfDB",
        "World Bank Africa",
        "IMF Africa",
        "Africa development aid",
        "Africa foreign direct investment",
        "Africa FDI",
    ],

    # ── MEDIA & COMMUNICATIONS — your core Distory beat ────────
    "Kenya Media": [
        "Nation Media Group",
        "Standard Group Kenya",
        "Royal Media Kenya",
        "Kenya journalism",
        "Kenya press freedom",
        "Kenya media",
    ],

    "Africa PR & Communications": [
        "Africa public relations",
        "Africa communications",
        "Africa reputation",
        "Africa brand",
        "Africa marketing",
        "Africa crisis communications",
    ],

    # ── POLITICAL & GOVERNANCE — for journalist clients ─────────
    "Kenya Politics": [
        "Kenya government",
        "Kenya parliament",
        "Kenya cabinet",
        "William Ruto",
        "Kenya Treasury",
        "Kenya elections",
    ],

    "East Africa Economy": [
        "East Africa economy",
        "East African Community",
        "EAC trade",
        "Kenya economy",
        "Uganda economy",
        "Tanzania economy",
        "Rwanda economy",
    ],
}

# ─────────────────────────────────────────────────────────────
# DERIVED — don't edit these, they're built from targets above
# ─────────────────────────────────────────────────────────────
# Flat list of all search terms across all targets
ALL_SEARCH_TERMS = list({
    term
    for terms in MONITORING_TARGETS.values()
    for term in terms
})

# Reverse lookup: term → label (for tagging articles)
TERM_TO_LABEL = {
    term: label
    for label, terms in MONITORING_TARGETS.items()
    for term in terms
}

# Legacy alias — used by GDELT loop (first target's first term)
GLOBAL_ENTITY = list(MONITORING_TARGETS.keys())[0]

# ── API KEYS

NEWS_API_KEY = "e1f9967a17244ba1af8092bf56388485"

# ── OUTPUT & TIMING ───────────────────────────────────────────
OUTPUT_FILE         = "daily_news.csv"
REQUEST_TIMEOUT     = 15
RATE_LIMIT_DELAY    = 0.3   # seconds between requests — be polite

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# ─────────────────────────────────────────────
# ENTITY & COMPANY WATCHLIST
# ─────────────────────────────────────────────
COMPANIES = [
    # Kenya
    "safaricom", "kcb", "equity bank", "kenya airways", "nation media",
    "standard group", "co-operative bank", "stanbic kenya", "icea lion",
    "britam", "jubilee insurance", "mpesa", "mpesa foundation",
    # Nigeria
    "dangote", "guaranty trust bank", "gtbank", "access bank", "zenith bank",
    "first bank nigeria", "uba", "fidelity bank", "sterling bank",
    "stanbic ibtc", "lafarge africa", "nestle nigeria",
    # Pan-African financial
    "equity group", "ecobank", "absa", "standard bank", "nedbank",
    "first national bank", "fnb", "old mutual", "sanlam",
    # Telecoms
    "mtn", "airtel", "vodacom", "telkom", "orange africa", "glo mobile",
    # Tech & innovation
    "flutterwave", "paystack", "andela", "interswitch", "opay",
    "moniepoint", "wave mobile money", "chipper cash", "sendwave",
    # Development & foundations
    "mastercard foundation", "african wildlife foundation", "awf",
    "science for africa", "sfa", "cema", "gates foundation africa",
    "ford foundation africa", "rockefeller foundation", "usaid africa",
    "giz africa", "dfid", "fcdo", "world bank africa", "afdb",
    "african development bank",
    # Energy & resources
    "totalenergies", "shell", "bp africa", "sasol", "eskom",
    "kenya power", "kengen", "tanesco",
    # Retail & FMCG
    "shoprite", "pick n pay", "woolworths south africa", "bidcorp",
    "unilever africa", "diageo africa", "heineken africa",
    # Airlines
    "ethiopian airlines", "kenya airways", "rwandair", "air senegal",
    "fastjet", "flysafair",
    # Global tech in Africa context
    "google africa", "microsoft africa", "amazon africa",
    "meta africa", "uber africa", "bolt africa",
]

# MONITOR_KEYWORDS is now derived from MONITORING_TARGETS above
# All search terms across all clients and topics — auto-built
MONITOR_KEYWORDS = ALL_SEARCH_TERMS

# ─────────────────────────────────────────────
# TEXT PROCESSING HELPERS
# ─────────────────────────────────────────────
def clean_text(text: str) -> str:
    text = str(text)
    text = re.sub(r'<[^>]+>', '', text)       # strip HTML
    text = re.sub(r'http\S+', '', text)        # remove URLs
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def get_sentiment(text: str) -> tuple[float, str]:
    try:
        blob = TextBlob(text)
        p = blob.sentiment.polarity
        label = "Positive" if p > 0.05 else "Negative" if p < -0.05 else "Neutral"
        return round(p, 4), label
    except Exception:
        return 0.0, "Neutral"


def classify_article(text: str) -> str:
    t = text.lower()
    rules = [
        (["artificial intelligence", " ai ", "machine learning", "deep learning",
          "generative ai", "llm", "agentic"], "AI & Tech"),
        (["health", "hospital", "disease", "malaria", "covid", "hiv",
          "maternal", "vaccination", "clinic"], "Health"),
        (["election", "government", "president", "parliament", "senate",
          "minister", "cabinet", "policy", "legislation"], "Politics"),
        (["business", "market", "finance", "economy", "gdp", "inflation",
          "investment", "startup", "ipo", "funding"], "Business"),
        (["climate", "flood", "drought", "weather", "carbon", "emissions",
          "renewable", "solar", "green energy"], "Climate & Environment"),
        (["education", "school", "university", "students", "learning",
          "curriculum", "teacher", "scholarship"], "Education"),
        (["agriculture", "farming", "crop", "harvest", "food security",
          "smallholder", "irrigation"], "Agriculture"),
        (["security", "conflict", "terrorism", "militia", "peacekeeping",
          "coup", "protest", "strike"], "Security & Conflict"),
    ]
    for keywords, category in rules:
        if any(kw in t for kw in keywords):
            return category
    return "General"


def extract_keywords(text: str, n: int = 8) -> str:
    stopwords = {
        "about", "after", "again", "before", "between", "could",
        "every", "first", "found", "great", "group", "here",
        "large", "later", "light", "might", "never", "other",
        "often", "place", "right", "should", "since", "small",
        "still", "their", "there", "these", "thing", "think",
        "those", "three", "under", "until", "where", "which",
        "while", "world", "would", "years", "your"
    }
    words = re.findall(r'\b[a-z]{5,}\b', text.lower())
    filtered = [w for w in words if w not in stopwords]
    freq = {}
    for w in filtered:
        freq[w] = freq.get(w, 0) + 1
    top = sorted(freq, key=freq.get, reverse=True)[:n]
    return ", ".join(top)


def extract_companies(text: str) -> str:
    t = text.lower()
    found = [c for c in COMPANIES if c.lower() in t]
    return ", ".join(sorted(set(found)))


def extract_companies_ner(text: str) -> str:
    """Detects ANY organization mention via NER, not just names in
    the static COMPANIES list above. This is what surfaces brands
    you haven't pre-loaded — extract_companies() only ever finds
    names already in that ~90-entry list, no matter how often an
    unlisted brand is mentioned across your sources."""
    if not nlp or not text:
        return ""
    doc = nlp(text[:5000])  # cap length — NER on huge text is slow, gains little
    orgs = {ent.text.strip() for ent in doc.ents if ent.label_ == "ORG"}
    return ", ".join(sorted(orgs))


def extract_full_text(url: str) -> str:
    """Use trafilatura to get full article text from URL."""
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            result = trafilatura.extract(downloaded, include_comments=False,
                                          include_tables=False)
            return clean_text(result or "")
    except Exception:
        pass
    return ""


def match_monitoring_targets(text: str) -> str:
    """
    Returns a comma-separated string of matched monitoring target labels.
    e.g. "Mastercard Foundation, Africa Education"
    An article can match multiple targets simultaneously.
    """
    t = text.lower()
    matched = set()
    for term, label in TERM_TO_LABEL.items():
        if term.lower() in t:
            matched.add(label)
    return ", ".join(sorted(matched)) if matched else ""


def build_article(source: str, title: str, summary: str,
                  link: str, author: str, published: str,
                  extra_text: str = "") -> dict:
    full_text = clean_text(f"{title} {summary} {extra_text}")
    sentiment_score, sentiment_label = get_sentiment(full_text)
    return {
        "source":               source,
        "title":                clean_text(title),
        "summary":              clean_text(summary)[:500],
        "link":                 link,
        "author":               author or "Unknown",
        "published_date":       published,
        "collected_date":       datetime.datetime.now().isoformat(),
        "category":             classify_article(full_text),
        "monitoring_targets":   match_monitoring_targets(full_text),
        "sentiment_score":      sentiment_score,
        "sentiment_label":      sentiment_label,
        "companies_mentioned":  extract_companies(full_text),      # curated watchlist — high precision
        "companies_detected":   extract_companies_ner(full_text),  # NER — broad, noisier, catches unlisted brands
        "keywords":             extract_keywords(full_text),
        "char_count":           len(full_text),
    }


# ─────────────────────────────────────────────
# RSS FEEDS — EXPANDED TO 130+ SOURCES
# ─────────────────────────────────────────────
RSS_FEEDS = {

    # ── EAST AFRICA ────────────────────────────────────────────
    "Nation Africa":                "https://nation.africa/rss",
    "Business Daily Africa":        "https://www.businessdailyafrica.com/rss",
    "The Standard Kenya":           "https://www.standardmedia.co.ke/rss",
    "Capital FM Kenya":             "https://www.capitalfm.co.ke/news/feed/",
    "Kenyans.co.ke":                "https://www.kenyans.co.ke/rss.xml",
    "Daily Nation Business":        "https://nation.africa/kenya/business/rss",
    "Kenya Wallstreet":             "https://kenyawallstreet.com/feed/",
    "The Star Kenya":               "https://www.the-star.co.ke/rss",
    "Citizen Digital Kenya":        "https://www.citizentv.co.ke/feed/",
    "NTV Kenya":                    "https://www.ntv.co.ke/feed/",
    "K24 Kenya":                    "https://www.k24tv.co.ke/feed/",
    "People Daily Kenya":           "https://www.pd.co.ke/feed/",

    # Uganda
    "Daily Monitor Uganda":         "https://www.monitor.co.ug/rss",
    "New Vision Uganda":            "https://www.newvision.co.ug/rss",
    "The Observer Uganda":          "https://observer.ug/feed/",
    "Nile Post Uganda":             "https://nilepost.co.ug/feed/",

    # Tanzania
    "The Citizen Tanzania":         "https://www.thecitizen.co.tz/feed/",
    "Daily News Tanzania":          "https://www.dailynews.co.tz/rss.php",
    "IPP Media Tanzania":           "https://www.ippmedia.com/rss",

    # Rwanda / Burundi
    "The New Times Rwanda":         "https://www.newtimes.co.rw/feed/",
    "KT Press Rwanda":              "https://www.ktpress.rw/feed/",

    # Ethiopia / Somalia / DRC
    "Addis Standard Ethiopia":      "https://addisstandard.com/feed/",
    "Ethiopian Reporter":           "https://www.ethiopianreporter.com/feed/",
    "Hiiraan Online Somalia":       "https://hiiraan.com/rss/news4.xml",
    "Radio France Intl Africa FR":  "https://www.rfi.fr/fr/rss",

    # ── WEST AFRICA ─────────────────────────────────────────────
    "Premium Times Nigeria":        "https://www.premiumtimesng.com/feed",
    "Guardian Nigeria":             "https://guardian.ng/feed/",
    "BusinessDay Nigeria":          "https://businessday.ng/feed/",
    "Punch Nigeria":                "https://punchng.com/feed/",
    "Vanguard Nigeria":             "https://www.vanguardngr.com/feed/",
    "Daily Trust Nigeria":          "https://dailytrust.com/feed/",
    "Sahara Reporters":             "https://saharareporters.com/feeds/latest",
    "Pulse Nigeria":                "https://www.pulse.ng/rss",
    "The Cable Nigeria":            "https://www.thecable.ng/feed",
    "Nairametrics":                 "https://nairametrics.com/feed/",
    "Legit Nigeria":                "https://www.legit.ng/rss/all.rss",
    "Leadership Nigeria":           "https://leadership.ng/feed/",
    "Channels TV Nigeria":          "https://www.channelstv.com/feed/",
    "Arise News Nigeria":           "https://www.arise.tv/feed/",

    # Ghana
    "GhanaWeb":                     "https://www.ghanaweb.com/GhanaHomePage/rss.xml",
    "Graphic Online Ghana":         "https://www.graphic.com.gh/rss.html",
    "Citi Newsroom Ghana":          "https://citinewsroom.com/feed/",
    "Joy Online Ghana":             "https://www.myjoyonline.com/feed/",
    "Modern Ghana":                 "https://www.modernghana.com/rss/news.xml",
    "Ghana Business News":          "https://www.ghanabusinessnews.com/feed/",
    "Daily Graphic Ghana":          "https://www.graphic.com.gh/feed/",

    # Senegal / Côte d'Ivoire / Francophone
    "Jeune Afrique":                "https://www.jeuneafrique.com/feed/",
    "Africanews FR":                "https://fr.africanews.com/feed/",
    "RFI Afrique":                  "https://www.rfi.fr/afrique/rss",
    "Le Monde Afrique":             "https://www.lemonde.fr/afrique/rss_full.xml",

    # ── SOUTHERN AFRICA ─────────────────────────────────────────
    "News24 South Africa":          "https://www.news24.com/news24/rss",
    "Daily Maverick":               "https://www.dailymaverick.co.za/feed/",
    "Mail and Guardian":            "https://mg.co.za/feed/",
    "BusinessTech SA":              "https://businesstech.co.za/news/feed/",
    "TimesLive SA":                 "https://www.timeslive.co.za/rss/",
    "IOL South Africa":             "https://www.iol.co.za/cmlink/1.640",
    "Engineering News SA":          "https://www.engineeringnews.co.za/page/rss",
    "SA News Gov":                  "https://www.sanews.gov.za/rss.xml",
    "Fin24":                        "https://www.news24.com/fin24/rss",
    "Moneyweb":                     "https://www.moneyweb.co.za/feed/",
    "Bizcommunity SA":              "https://www.bizcommunity.com/rss/196/91.rss",

    # Zimbabwe
    "ZimLive":                      "https://www.zimlive.com/feed/",
    "The Zimbabwe Mail":            "https://www.thezimbabwemail.com/feed/",
    "NewsDay Zimbabwe":             "https://www.newsday.co.zw/feed/",
    "The Herald Zimbabwe":          "https://www.herald.co.zw/feed/",

    # Zambia / Malawi / Mozambique
    "Zambia Daily Mail":            "https://www.daily-mail.co.zm/feed/",
    "Nyasa Times Malawi":           "https://www.nyasatimes.com/feed/",
    "The Namibian":                 "https://www.namibian.com.na/feed/",

    # Botswana / Lesotho / eSwatini
    "Mmegi Botswana":               "https://www.mmegi.bw/feed/",

    # ── NORTH AFRICA ────────────────────────────────────────────
    "Egypt Today":                  "https://www.egypttoday.com/feed",
    "Ahram Online":                 "https://english.ahram.org.eg/RSS/",
    "Morocco World News":           "https://www.moroccoworldnews.com/feed/",
    "Algerie Presse Service":       "https://www.aps.dz/en/feed",
    "Tunisia Live":                 "https://www.tunisialive.net/feed/",
    "Libya Herald":                 "https://libyaherald.com/feed/",

    # ── PAN-AFRICAN & CONTINENTAL ───────────────────────────────
    "Africanews EN":                "https://www.africanews.com/feed/",
    "AllAfrica":                    "https://allafrica.com/tools/headlines/rdf/latest/headlines.rdf",
    "The Africa Report":            "https://www.theafricareport.com/feed/",
    "African Arguments":            "https://africanarguments.org/feed/",
    "Africa Is a Country":          "https://africasacountry.com/feed/",
    "OkayAfrica":                   "https://www.okayafrica.com/feed/",
    "The Continent":                "https://thecontinent.org/feed/",
    "African Business Magazine":    "https://africanbusinessmagazine.com/feed/",
    "Ventures Africa":              "https://venturesafrica.com/feed/",
    "How We Made It In Africa":     "https://www.howwemadeitinafrica.com/feed/",
    "Africa Finance":               "https://africafinance.com/feed/",
    "Further Africa":               "https://furtherafrica.com/feed/",
    "Ecofin Agency":                "https://www.ecofinagency.com/rss",
    "Africa Briefing":              "https://www.africabriefing.com/feed",
    "Africa Intelligence":          "https://www.africaintelligence.com/rss",
    "Africa.com":                   "https://www.africa.com/feed/",

    # ── TECH & INNOVATION ───────────────────────────────────────
    "TechCabal":                    "https://techcabal.com/feed/",
    "TechPoint Africa":             "https://techpoint.africa/feed/",
    "Disrupt Africa":               "https://disruptafrica.com/feed/",
    "IT News Africa":               "https://www.itnewsafrica.com/feed/",
    "Connecting Africa":            "https://www.connectingafrica.com/rss.xml",
    "TechTrends Kenya":             "https://techtrendske.co.ke/feed/",
    "Benjamindada.com":             "https://www.benjamindada.com/feed/",
    "Technext Nigeria":             "https://technext.ng/feed/",
    "Rest of World":                "https://restofworld.org/feed/",
    "African Business Tech":        "https://africanbusinessmagazine.com/category/technology/feed/",

    # ── DEVELOPMENT / NGO / MULTILATERAL ────────────────────────
    "Devex":                        "https://www.devex.com/news/rss",
    "ReliefWeb Africa":             "https://reliefweb.int/updates/rss.xml",
    "Thomson Reuters Foundation":   "https://news.trust.org/feed/",
    "World Bank Africa":            "https://www.worldbank.org/en/region/afr/rss",
    "UN News Africa":               "https://news.un.org/feed/subscribe/en/news/region/africa/feed/rss.xml",
    "UNICEF Africa":                "https://www.unicef.org/press-releases/rss.xml",
    "WHO Africa":                   "https://www.afro.who.int/rss.xml",
    "African Union":                "https://au.int/en/rss.xml",
    "UNECA":                        "https://www.uneca.org/rss.xml",
    "AfDB News":                    "https://www.afdb.org/en/rss",
    "APO Group Wire":               "https://apo-opa.com/feed/",

    # ── GLOBAL WITH AFRICA LENS ─────────────────────────────────
    "BBC Africa":                   "http://feeds.bbci.co.uk/news/world/africa/rss.xml",
    "Al Jazeera Africa":            "https://www.aljazeera.com/xml/rss/all.xml",
    "VOA Africa":                   "https://www.voanews.com/rss/zaXQy5BVYQ",
    "RFI Africa EN":                "https://www.rfi.fr/en/rss",
    "DW Africa":                    "https://rss.dw.com/rdf/rss-en-africa",

    # ── SECTOR-SPECIFIC ─────────────────────────────────────────
    "Africa Oil and Power":         "https://www.africaoilandpower.com/feed/",
    "Africa Energy Portal":         "https://africa-energy-portal.org/feed/",
    "Africa Science News":          "https://africasciencenews.org/feed/",
    "Africa Climate News":          "https://climate-africa.com/feed/",
    "Africa Mining":                "https://www.miningafrica.net/feed/",
    "Africa Renewable Energy":      "https://renewableenergyafrica.com/feed/",
    "Africa Food Security":         "https://africafoodsecurity.org/feed/",
    "Kenya Wallstreet Finance":     "https://kenyawallstreet.com/category/finance/feed/",
    "African Leadership Magazine":  "https://afrleadership.com/feed/",

    # OWNED MEDIA BASELINE
    "Mastercard Foundation":        "https://mastercardfdn.org/feed/",
}

# GOOGLE NEWS RSS
def get_google_news_feeds() -> dict:

    queries = [
        GLOBAL_ENTITY,
        "Africa fintech",
        "Africa AI",
        "Africa health",
        "Africa education",
        "Safaricom",
        "Kenya economy",
        "Nigeria economy",
        "South Africa economy",
        "Africa climate",
        "Africa startup",
        "Africa development",
    ]
    feeds = {}
    for q in queries:
        encoded = q.replace(" ", "+").replace('"', "%22")
        feeds[f"Google News: {q}"] = (
            f"https://news.google.com/rss/search?"
            f"q={encoded}&hl=en-US&gl=US&ceid=US:en"
        )
    return feeds


# COLLECTOR FUNCTIONS

def collect_rss(feeds: dict, client: httpx.Client) -> list[dict]:
    articles = []
    for source, url in feeds.items():
        try:
            time.sleep(RATE_LIMIT_DELAY)
            res = client.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                             follow_redirects=True)
            if res.status_code != 200:
                log.warning(f"[RSS] {source}: HTTP {res.status_code}")
                continue
            feed = feedparser.parse(res.text)
            count = 0
            for entry in feed.entries:
                title     = entry.get("title", "")
                summary   = entry.get("summary", "")
                link      = entry.get("link", "")
                author    = entry.get("author", "Unknown")
                published = entry.get("published", "")
                content   = ""
                if entry.get("content"):
                    content = entry["content"][0].get("value", "")

                if not title and not link:
                    continue

                articles.append(build_article(
                    source, title, summary, link, author, published, content
                ))
                count += 1
            if count:
                log.info(f"[RSS] {source}: {count} entries")
        except Exception as e:
            log.error(f"[RSS Error] {source}: {e}")
    return articles


def collect_gdelt(client: httpx.Client, query: str = None) -> list[dict]:
    articles = []
    query = query or GLOBAL_ENTITY
    try:
        res = client.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={
                "query": f'"{query}" sourcelang:english',
                "mode": "artlist",
                "maxrecords": "250",
                "format": "json",
            },
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if res.status_code == 200:
            data = res.json().get("articles", [])
            for art in data:
                title     = art.get("title", "")
                url       = art.get("url", "")
                published = art.get("seendate", "")
                src_name  = f"GDELT — {art.get('sourcecountry', 'Global')}"
                articles.append(build_article(
                    src_name, title, "", url,
                    "GDELT", published
                ))
            log.info(f"[GDELT] {len(articles)} articles for query: {query}")
    except Exception as e:
        log.error(f"[GDELT Error]: {e}")
    return articles


def collect_newsapi(client: httpx.Client, api_key: str) -> list[dict]:
    articles = []
    if not api_key:
        log.info("[NewsAPI] No key found — skipping")
        return articles
    for keyword in MONITOR_KEYWORDS[:5]:   # stay within free tier limits
        try:
            res = client.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": f'"{keyword}"',
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 100,
                    "apiKey": api_key,
                },
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            if res.status_code == 200:
                for art in res.json().get("articles", []):
                    title     = art.get("title", "")
                    desc      = art.get("description", "") or ""
                    link      = art.get("url", "")
                    published = art.get("publishedAt", "")
                    src_name  = f"NewsAPI — {art.get('source', {}).get('name', 'Unknown')}"
                    author    = art.get("author", "Unknown")
                    articles.append(build_article(
                        src_name, title, desc, link, author, published
                    ))
                log.info(f"[NewsAPI] {keyword}: {len(articles)} total so far")
            time.sleep(1)   # NewsAPI rate limit
        except Exception as e:
            log.error(f"[NewsAPI Error] {keyword}: {e}")
    return articles



# SAVE
def save_articles(new_articles: list[dict]) -> tuple[int, int]:
    if not new_articles:
        log.warning("No articles to save.")
        return 0, 0

    new_df = pd.DataFrame(new_articles)
    new_df = new_df[new_df["title"].str.strip() != ""]   # drop empty titles

    if os.path.exists(OUTPUT_FILE):
        existing_df = pd.read_csv(OUTPUT_FILE)
        combined   = pd.concat([existing_df, new_df], ignore_index=True)
        combined   = combined.drop_duplicates(subset="link", keep="last")
        combined.to_csv(OUTPUT_FILE, index=False)
        return len(new_df), len(combined)
    else:
        new_df.to_csv(OUTPUT_FILE, index=False)
        return len(new_df), len(new_df)


# MASTER PIPELINE
def collect_data():
    start = time.time()
    log.info("=" * 60)
    log.info("MediaPulse Africa Pipeline v3.0 — Starting")
    log.info("=" * 60)

    all_articles = []

    with httpx.Client(timeout=REQUEST_TIMEOUT) as client:

        # 1. RSS — static African feeds
        log.info(f"[Step 1] RSS collection: {len(RSS_FEEDS)} feeds")
        all_articles += collect_rss(RSS_FEEDS, client)

        # 2. Google News RSS — dynamic keyword feeds
        google_feeds = get_google_news_feeds()
        log.info(f"[Step 2] Google News RSS: {len(google_feeds)} keyword feeds")
        all_articles += collect_rss(google_feeds, client)

        # 3. GDELT — for each monitored keyword
        log.info("[Step 3] GDELT global intelligence layer")
        for kw in MONITOR_KEYWORDS:
            all_articles += collect_gdelt(client, kw)
            time.sleep(1)

        # 4. NewsAPI
        log.info("[Step 4] NewsAPI aggregator")
        all_articles += collect_newsapi(client, NEWS_API_KEY)


    # 6. Save
    new_count, total_count = save_articles(all_articles)

    elapsed = round(time.time() - start, 1)

    # Execution log
    with open("data/log.txt", "a") as f:
        f.write(
            f"{datetime.datetime.now().isoformat()} | "
            f"new={new_count} | total={total_count} | "
            f"elapsed={elapsed}s\n"
        )

    log.info("=" * 60)
    log.info(f"Pipeline complete in {elapsed}s")
    log.info(f"New articles collected : {new_count}")
    log.info(f"Total dataset size     : {total_count}")
    log.info(f"Output file            : {OUTPUT_FILE}")
    log.info("=" * 60)


if __name__ == "__main__":
    collect_data()
