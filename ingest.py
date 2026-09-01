import os
import sys
import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.dialects.postgresql import insert

# LOGGING SETUP
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("mediapulse_ingest")

log.info("Running MediaPulse Ingestion Pipeline...")

# ROBUST DATA LOADING
INPUT_FILE = "daily_news.csv"
if not os.path.exists(INPUT_FILE):
    log.error(f"No data file found at {INPUT_FILE}. Run scraper first.")
    sys.exit(0)

# low_memory=False fixes the DtypeWarning on mixed type columns
df = pd.read_csv(INPUT_FILE, low_memory=False)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    log.error("DATABASE_URL environment variable is missing! Ingestion halted.")
    sys.exit(1)

engine = create_engine(DATABASE_URL)

# SMART DEDUPLICATION (In-Memory & Database Check)
# Drop essential NaNs and internal file duplicates first
initial_count = len(df)
df = df.dropna(subset=['title', 'link'])
df = df.drop_duplicates(subset=['title', 'link'])

# Check existing records against (title, link) composite unique constraint
try:
    with engine.connect() as conn:
        existing = pd.read_sql("SELECT title, link FROM news", conn)
    
    if not existing.empty:
        merged = df.merge(existing, on=['title', 'link'], how='left', indicator=True)
        df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    log.info(f"Found {len(df)} new articles (Filtered out {initial_count - len(df)} duplicate/invalid entries).")
except Exception as e:
    log.warning(f"Database deduplication check bypassed (Table might be empty or fresh): {e}")

if df.empty:
    log.info("No new unique data to process. Exiting cleanly.")
    sys.exit(0)

# DATA CLEANING
df['summary'] = df['summary'].fillna("Summary unavailable")

# FEATURE ENGINEERING
# Parse dates with logical fallback priority: published_date -> collected_date -> UTC now
pub_date = pd.to_datetime(df['published_date'], errors='coerce', utc=True)
coll_date = pd.to_datetime(df.get('collected_date', pd.Series(dtype=object)), errors='coerce', utc=True)
now_utc = pd.Timestamp.now(tz=datetime.timezone.utc)

df['published_date'] = pub_date.fillna(coll_date).fillna(now_utc)

# Derive temporal features once
df['date'] = df['published_date'].dt.date
df['hour'] = df['published_date'].dt.hour
df['day_of_week'] = df['published_date'].dt.day_name()

# Safely extract metrics
df['text_length'] = df['char_count'].fillna(0) if 'char_count' in df.columns else 0
df['keyword_count'] = df['keywords'].fillna("").str.count(",") + 1 if 'keywords' in df.columns else 0
df['sentiment_score'] = df['sentiment_score'].fillna(0.0) if 'sentiment_score' in df.columns else 0.0

# Virality score calculation
df['virality_score'] = (
    df['text_length'] * 0.1 + 
    df['keyword_count'] * 5 + 
    df['sentiment_score'].abs() * 20
)

# 5. UPLOAD TIMESTAMPS
df['created_at'] = datetime.datetime.now(datetime.timezone.utc)

# 6. PUSH TO NEON POSTGRESQL (CRASH-PROOF UPSERT)
columns_to_drop = [
    'date', 'hour', 'day_of_week', 
    'text_length', 'keyword_count', 
    'monitoring_targets', 'char_count', 'virality_score',
    'companies_detected'  # NER output — used by sync_entity_graph() below,
                          # not stored on `news` (that table's schema doesn't have it)
]

db_payload = df.drop(columns=columns_to_drop, errors='ignore')

# PostgreSQL custom engine function to target unique key columns directly
def postgres_on_conflict_do_nothing(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return
    
    stmt = insert(table.table).values(data)
    
    # Replaced named constraint reference with direct index target columns
    stmt = stmt.on_conflict_do_nothing(
        index_elements=['title', 'link']
    )
    
    conn.execute(stmt)

log.info(f"Syncing {len(db_payload)} records to the 'news' table in Neon...")
db_payload.to_sql(
    "news", 
    engine, 
    if_exists="append", 
    index=False, 
    method=postgres_on_conflict_do_nothing
)
log.info("New data successfully synced to Neon PostgreSQL.")

# ═════════════════════════════════════════════════════════════
# ENTITY KNOWLEDGE GRAPH (built on the same `engine`)
# ═════════════════════════════════════════════════════════════

ENTITY_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS entities;

-- If news.id is currently TEXT/VARCHAR, convert it safely to BIGINT first.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'news' 
          AND column_name = 'id' 
          AND data_type IN ('text', 'character varying')
    ) THEN
        ALTER TABLE news 
        ALTER COLUMN id TYPE BIGINT USING (NULLIF(id, '')::BIGINT);
    END IF;
END $$;

-- Backfill missing IDs, set auto-increment sequence, and attach Primary Key
DO $$
DECLARE
    max_id BIGINT;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        WHERE t.relname = 'news' AND c.contype IN ('p', 'u')
    ) THEN
        -- Safely pull current max id as numeric
        SELECT COALESCE(MAX(id), 0) INTO max_id FROM news;

        WITH numbered AS (
            SELECT ctid, row_number() OVER (ORDER BY ctid) AS rn
            FROM news WHERE id IS NULL
        )
        UPDATE news SET id = max_id + numbered.rn
        FROM numbered
        WHERE news.ctid = numbered.ctid;

        -- Create sequence starting after highest current ID
        CREATE SEQUENCE IF NOT EXISTS news_id_seq;
        PERFORM setval('news_id_seq', (SELECT COALESCE(MAX(id), 0) FROM news));
        ALTER TABLE news ALTER COLUMN id SET DEFAULT nextval('news_id_seq');
        ALTER SEQUENCE news_id_seq OWNED BY news.id;

        ALTER TABLE news ALTER COLUMN id SET NOT NULL;
        ALTER TABLE news ADD PRIMARY KEY (id);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS entities.entities (
    id SERIAL PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    country TEXT,
    sector TEXT,
    confidence_score FLOAT DEFAULT 0.0,
    source TEXT, -- 'ner_pipeline' | 'client_onboarding' | 'registry_enrichment'
    verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS entities.entity_aliases (
    id SERIAL PRIMARY KEY,
    entity_id INT REFERENCES entities.entities(id),
    alias_text TEXT NOT NULL,
    UNIQUE(entity_id, alias_text)
);

CREATE TABLE IF NOT EXISTS entities.entity_mentions (
    id SERIAL PRIMARY KEY,
    entity_id INT REFERENCES entities.entities(id),
    article_id BIGINT REFERENCES news(id),
    mention_date TIMESTAMP,
    mention_count INT DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_entity_aliases_text
    ON entities.entity_aliases (alias_text);
CREATE INDEX IF NOT EXISTS idx_entity_mentions_entity_date
    ON entities.entity_mentions (entity_id, mention_date);
"""

ENTITY_WATCHLIST = [
    "safaricom", "kcb", "equity bank", "mtn", "airtel",
    "vodacom", "standard bank", "absa", "ecobank", "kenya airways",
    "google", "microsoft", "amazon", "Centre for epidemiological modelling",
    "CEMA", "SFA", "Africa wildlife foundation", "AWF", "MPESA Foundation",
    "Mastercard Foundation", "Garnet partners", "African women in agricultural research and development",
    "Kenyatta National Hospital", "Institute of engineering rwanda", "rwanda stock exchange",
]


def init_entity_schema(engine) -> None:
    """Idempotent — safe to run on every ingestion pass. Creates the
    schema/tables if missing, then seeds entities from the watchlist
    as verified, source='client_onboarding'."""
    with engine.begin() as conn:
        conn.execute(text(ENTITY_SCHEMA_SQL))

        for name in ENTITY_WATCHLIST:
            existing = conn.execute(
                text("SELECT entity_id FROM entities.entity_aliases WHERE alias_text = :name"),
                {"name": name},
            ).fetchone()
            if existing:
                continue

            entity_id = conn.execute(
                text("""
                    INSERT INTO entities.entities (canonical_name, source, verified, confidence_score)
                    VALUES (:name, 'client_onboarding', TRUE, 1.0)
                    RETURNING id
                """),
                {"name": name},
            ).scalar()

            conn.execute(
                text("""
                    INSERT INTO entities.entity_aliases (entity_id, alias_text)
                    VALUES (:entity_id, :name)
                    ON CONFLICT (entity_id, alias_text) DO NOTHING
                """),
                {"entity_id": entity_id, "name": name},
            )
    log.info("[EntityGraph] Schema ready, watchlist seeded")


def get_or_create_entity(conn, alias_text: str) -> int:
    """Exact-match alias lookup, else creates a new low-confidence
    entity with source='ner_pipeline'."""
    row = conn.execute(
        text("SELECT entity_id FROM entities.entity_aliases WHERE alias_text = :alias"),
        {"alias": alias_text},
    ).fetchone()
    if row:
        return row[0]

    entity_id = conn.execute(
        text("""
            INSERT INTO entities.entities (canonical_name, source, verified, confidence_score)
            VALUES (:alias, 'ner_pipeline', FALSE, 0.3)
            RETURNING id
        """),
        {"alias": alias_text},
    ).scalar()

    conn.execute(
        text("""
            INSERT INTO entities.entity_aliases (entity_id, alias_text)
            VALUES (:entity_id, :alias)
            ON CONFLICT (entity_id, alias_text) DO NOTHING
        """),
        {"entity_id": entity_id, "alias": alias_text},
    )
    return entity_id


def sync_entity_graph(engine, df: pd.DataFrame) -> None:
    """Looks up the just-inserted rows' `news.id` by (link), matches
    each article's text against ENTITY_WATCHLIST (verified, high
    confidence), and separately reads the scraper's companies_detected
    column — NER output — for anything outside the watchlist (unverified,
    confidence 0.3). Writes one entity_mentions row per match, from
    either source. No-ops cleanly if df is empty."""
    if df.empty:
        return

    init_entity_schema(engine)

    with engine.connect() as conn:
        stmt = text("SELECT id, link FROM news WHERE link IN :links").bindparams(
            bindparam("links", expanding=True)
        )
        news_ids = pd.read_sql(stmt, conn, params={"links": df['link'].tolist()})

    id_map = dict(zip(news_ids['link'], news_ids['id']))

    search_text = (df['title'].fillna("") + " " + df['summary'].fillna("")).str.lower()
    has_ner_column = 'companies_detected' in df.columns

    mention_count = 0
    with engine.begin() as conn:
        for idx, row in df.iterrows():
            article_id = id_map.get(row['link'])
            if article_id is None:
                continue  # row didn't make it into `news` (e.g. conflict skip)

            text_blob = search_text.loc[idx]
            watchlist_matches = {c for c in ENTITY_WATCHLIST if c.lower() in text_blob}

            ner_matches = set()
            if has_ner_column:
                raw = row.get('companies_detected', "")
                if isinstance(raw, str) and raw.strip():
                    ner_matches = {c.strip() for c in raw.split(",") if c.strip()}

            all_matches = watchlist_matches | ner_matches
            if not all_matches:
                continue

            for company in all_matches:
                entity_id = get_or_create_entity(conn, company)
                conn.execute(
                    text("""
                        INSERT INTO entities.entity_mentions (entity_id, article_id, mention_date)
                        VALUES (:entity_id, :article_id, :mention_date)
                    """),
                    {
                        "entity_id": entity_id,
                        "article_id": int(article_id),
                        "mention_date": row['published_date'],
                    },
                )
                mention_count += 1

    log.info(f"[EntityGraph] Recorded {mention_count} entity mentions across {len(id_map)} articles")


sync_entity_graph(engine, df)

# BRAND TRACKING & REPUTATION ENGINE
companies = [
    "safaricom", "kcb", "equity bank", "mtn", "airtel",
    "vodacom", "standard bank", "absa", "ecobank", "kenya airways",
    "google", "microsoft", "amazon", "Centre for epidemiological modelling", 
    "CEMA", "SFA", "Africa wildlife foundation", "AWF", "MPESA Foundation",
    "Mastercard Foundation", "Garnet partners", "African women in agricultural research and development",
    "Kenyatta National Hospital", "Institute of engineering rwanda", "rwanda stock exchange",
]

df['temp_search_text'] = (df['title'].fillna("") + " " + df['summary'].fillna("")).str.lower()

brand_results = []
for company in companies:
    comp_clean = company.strip()
    mask = df['temp_search_text'].str.contains(comp_clean.lower(), regex=False, na=False)
    brand_df = df[mask]
    
    if not brand_df.empty:
        brand_results.append({
            "company": comp_clean,
            "mentions": len(brand_df),
            "avg_sentiment": brand_df['sentiment_score'].mean(),
            "status": brand_df['sentiment_label'].iloc[0] if 'sentiment_label' in brand_df.columns else "Neutral"
        })

brand_df_final = pd.DataFrame(brand_results)

# ANOMALY DETECTION & ALERT SYSTEM
alerts = []
latest_date = df['date'].max()
prev_date = df[df['date'] < latest_date]['date'].max()

if pd.notna(prev_date):
    recent_volume = len(df[df['date'] == latest_date])
    prev_volume = len(df[df['date'] == prev_date])
    
    if prev_volume > 0 and recent_volume > prev_volume * 1.5:
        alerts.append(f"Volume Spike: {recent_volume} articles today vs {prev_volume} yesterday.")

# SAVE LOCAL OUTPUTS & ARTIFACTS
os.makedirs("data", exist_ok=True)

clean_local_export = df.drop(columns=['temp_search_text'], errors='ignore')
clean_local_export.to_csv("data/processed_news.csv", index=False)
brand_df_final.to_csv("data/brand_mentions.csv", index=False)

with open("data/alerts.txt", "w") as f:
    for a in alerts: 
        f.write(a + "\n")

log.info("=" * 60)
log.info("Ingestion processing complete.")
log.info(f"New Articles Added : {len(df)}")
log.info(f"System Alerts Fired : {len(alerts)}")
log.info("=" * 60)
