import os
import sys
import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine
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
    'monitoring_targets', 'char_count', 'virality_score'
]

db_payload = df.drop(columns=columns_to_drop, errors='ignore')

# PostgreSQL custom engine function to ignore constraint conflicts gracefully
def postgres_on_conflict_do_nothing(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return
    stmt = insert(table.table).values(data).on_conflict_do_nothing(
        constraint='news_title_link_uq'
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

# BRAND TRACKING & REPUTATION ENGINE
companies = [
    "safaricom", "kcb", "equity bank", "mtn", "airtel",
    "vodacom", "standard bank", "absa", "ecobank", "kenya airways",
    "google", "microsoft", "amazon", "Centre for epidemiological modelling", 
    "CEMA", "SFA", "Africa wildlife foundation", "AWF", "MPESA Foundation",
    "Mastercard Foundation", "Garnet partners", "African women in agricultural research and development",
    "Kenyatta National Hospital", "Institute of engineering rwanda","rwanda stock exchange",
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
