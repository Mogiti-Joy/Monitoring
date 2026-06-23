import os
import datetime
import logging
import pandas as pd
from collections import Counter
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("mediapulse_ingest")

log.info("Running MediaPulse Ingestion Pipeline...")

# 1. ROBUST DATA LOADING
INPUT_FILE = "daily_news.csv"
if not os.path.exists(INPUT_FILE):
    log.error(f"No data file found at {INPUT_FILE}. Run scraper first.")
    exit()

# Load raw data from your daily scrape
df = pd.read_csv(INPUT_FILE)

# Fetch Neon connection string from your local environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    log.error("DATABASE_URL environment variable is missing! Ingestion halted.")
    exit()

engine = create_engine(DATABASE_URL)

# 2. SMART DEDUPLICATION (Checking against Neon)
try:
    with engine.connect() as conn:
        # Pull only the links array to minimize memory usage on your desktop
        existing_links = pd.read_sql("SELECT link FROM news", conn)['link'].tolist()
    
    initial_count = len(df)
    # Filter: Keep only links NOT already present in your database
    df = df[~df['link'].isin(existing_links)]
    log.info(f"Found {len(df)} new articles (Filtered out {initial_count - len(df)} duplicates).")
except Exception as e:
    log.warning(f"Database deduplication check bypassed (Table might be empty or fresh): {e}")

if df.empty:
    log.info("No new unique data to process. Exiting cleanly.")
    exit()

# 3. DATA CLEANING
# Fill missing summaries instead of dropping the row to prevent data loss
df['summary'] = df['summary'].fillna("Summary unavailable")
# Drop only if essential identification fields are missing
df = df.dropna(subset=['title', 'link'])

# 4. FEATURE ENGINEERING
# Convert strings to actual datetime objects for accurate time-series analysis
df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')

# Fallback to collection timestamp if published_date string fails parsing
df['published_date'] = df['published_date'].fillna(pd.to_datetime(df['collected_date'], errors='coerce'))
# If both fail, fallback to current time
df['published_date'] = df['published_date'].fillna(datetime.datetime.now())

df['date'] = df['published_date'].dt.date
df['hour'] = df['published_date'].dt.hour
df['day_of_week'] = df['published_date'].dt.day_name()

# Note: scraper.py calculates length using title + summary + content inside 'char_count' 
df['text_length'] = df['char_count'].fillna(0)
df['keyword_count'] = df['keywords'].fillna("").str.count(",") + 1

# Calculate custom structural Virality Score
df['virality_score'] = (
    df['text_length'] * 0.1 + 
    df['keyword_count'] * 5 + 
    df['sentiment_score'].abs() * 20
)

# 5. UPLOAD TIMESTAMPS
# This matches the audit column tracking on your Neon target instances
df['created_at'] = datetime.datetime.now()

# 6. PUSH TO NEON POSTGRESQL
# Drop columns we computed purely for local analytical calculations (like date and counts) 
columns_to_drop = [
    'date', 'hour', 'day_of_week', 
    'text_length', 'keyword_count', 
    'monitoring_targets', 'char_count', 'virality_score'
]

db_payload = df.drop(columns=columns_to_drop, errors='ignore')

log.info(f"Syncing {len(db_payload)} records to the 'news' table in Neon...")
db_payload.to_sql("news", engine, if_exists="append", index=False)
print("New data synced to Neon PostgreSQL with timestamps.")
log.info(f"Syncing {len(db_payload)} records to the 'news' table in Neon...")
db_payload.to_sql("news", engine, if_exists="append", index=False)
log.info("New data successfully synced to Neon PostgreSQL.")

# 7. BRAND TRACKING & REPUTATION ENGINE (Optimized Loop)
companies = [
    "safaricom", "kcb", "equity bank", "mtn", "airtel",
    "vodacom", "standard bank", "absa", "ecobank", "kenya airways",
    "google", "microsoft", "amazon", "Centre for epidemiological modelling", "CEMA", "SFA", " Africa wildlife foundation", "AWF", "MPESA Foundation",
    "Mastercard Foundation"
]

# Create a temporary unified lowercased text block from title + summary for structural searching
df['temp_search_text'] = (df['title'].fillna("") + " " + df['summary'].fillna("")).str.lower()

brand_results = []
for company in companies:
    mask = df['temp_search_text'].str.contains(company.lower(), na=False)
    brand_df = df[mask]
    
    if not brand_df.empty:
        brand_results.append({
            "company": company,
            "mentions": len(brand_df),
            "avg_sentiment": brand_df['sentiment_score'].mean(),
            "status": brand_df['sentiment_label'].iloc[0] if 'sentiment_label' in brand_df.columns else "Neutral"
        })

brand_df_final = pd.DataFrame(brand_results)

# 8. ANOMALY DETECTION & ALERT SYSTEM
alerts = []
latest_date = df['date'].max()
prev_date = df[df['date'] < latest_date]['date'].max()

if pd.notna(prev_date):
    recent_volume = len(df[df['date'] == latest_date])
    prev_volume = len(df[df['date'] == prev_date])
    
    if recent_volume > prev_volume * 1.5:
        alerts.append(f"Volume Spike: {recent_volume} articles today vs {prev_volume} yesterday.")

# 9. SAVE LOCAL OUTPUTS & ARTIFACTS
os.makedirs("data", exist_ok=True)

# Save processed analytics files locally for immediate Streamlit dashboard loading
clean_local_export = df.drop(columns=['temp_search_text'], errors='ignore')
clean_local_export.to_csv("data/processed_news.csv", index=False)
brand_df_final.to_csv("data/brand_mentions.csv", index=False)

with open("data/alerts.txt", "w") as f:
    for a in alerts: 
        f.write(a + "\n")

log.info("=" * 60)
log.info(f"Ingestion processing complete.")
log.info(f"New Articles Added : {len(df)}")
log.info(f"System Alerts Fired : {len(alerts)}")
log.info("=" * 60)
