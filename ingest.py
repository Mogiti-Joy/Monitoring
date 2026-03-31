import pandas as pd
import os
import datetime
from collections import Counter
from sqlalchemy import create_engine, text

print("Running MediaPulse Ingestion Pipeline...")

# 1. ROBUST DATA LOADING
# Ensures the scraper output exists before proceeding
if not os.path.exists("daily_news.csv"):
    print("No data file found. Run scraper first.")
    exit()

# Load raw data from your daily scrape
df = pd.read_csv("daily_news.csv")

# Create connection to Neon PostgreSQL using environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# 2. SMART DEDUPLICATION (Checking against Neon)
# This prevents uploading the same article twice
try:
    with engine.connect() as conn:
        # We only pull the links to minimize memory usage
        existing_links = pd.read_sql("SELECT link FROM news", conn)['link'].tolist()
    
    initial_count = len(df)
    # Filter: Keep only links NOT already in the database
    df = df[~df['link'].isin(existing_links)]
    print(f"Found {len(df)} new articles (Filtered out {initial_count - len(df)} duplicates).")
except Exception as e:
    print(f"Database check skipped (Table might be empty): {e}")

if df.empty:
    print("No new data to process. Exiting.")
    exit()

# 3. DATA CLEANING
# Fill missing summaries instead of dropping the whole row to prevent data loss
df['summary'] = df['summary'].fillna("Summary unavailable")
# Drop only if essential identification fields are missing
df = df.dropna(subset=['title', 'link']) 

# 4. FEATURE ENGINEERING
# Convert strings to actual datetime objects for accurate time-series analysis
df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
df = df.dropna(subset=['published_date'])

df['date'] = df['published_date'].dt.date
df['hour'] = df['published_date'].dt.hour
df['day_of_week'] = df['published_date'].dt.day_name()

# Vectorized length calculations for speed
df['text_length'] = df['full_text'].str.len().fillna(0)
df['keyword_count'] = df['keywords'].fillna("").str.count(",") + 1

# Virality score calculation
df['virality_score'] = (
    df['text_length'] * 0.1 + 
    df['keyword_count'] * 5 + 
    df['sentiment_score'].abs() * 20
)

# 5. THE FIX: ADDING TIMESTAMP BEFORE UPLOAD
# This matches the 'created_at' column we added to your Neon table
df['created_at'] = datetime.datetime.now()

# 6. PUSH TO NEON POSTGRESQL
# Appends new rows to your existing 'news' table
df.to_sql("news", engine, if_exists="append", index=False)
print("New data synced to Neon PostgreSQL with timestamps.")

# 7. BRAND TRACKING (Optimized Loop)
# Tracks mentions for specific organizations
companies = [
    "safaricom", "kcb", "equity bank", "mtn", "airtel",
    "vodacom", "standard bank", "absa", "ecobank", "kenya airways",
    "google", "microsoft", "amazon", "CEMA", "SFA", "AWF", "MPESA Foundation"
]

# Lowercase column for faster string matching
df['full_text_lower'] = df['full_text'].str.lower().fillna("")

brand_results = []
for company in companies:
    mask = df['full_text_lower'].str.contains(company.lower(), na=False)
    brand_df = df[mask]
    
    if not brand_df.empty:
        brand_results.append({
            "company": company,
            "mentions": len(brand_df),
            "avg_sentiment": brand_df['sentiment_score'].mean(),
            "status": brand_df['sentiment_label'].iloc[0] if 'sentiment_label' in brand_df.columns else "Neutral"
        })

brand_df_final = pd.DataFrame(brand_results)

# 8. ALERT SYSTEM
alerts = []
latest_date = df['date'].max()
# Find the volume spike compared to the previous date in the current batch
prev_date = df[df['date'] < latest_date]['date'].max()

if pd.notna(prev_date):
    recent_volume = len(df[df['date'] == latest_date])
    prev_volume = len(df[df['date'] == prev_date])
    
    if recent_volume > prev_volume * 1.5:
        alerts.append(f"Volume Spike: {recent_volume} articles today vs {prev_volume} yesterday.")

# 9. SAVE OUTPUTS & ALERTS
os.makedirs("data", exist_ok=True)
# Save processed data locally for Streamlit or manual review
df.drop(columns=['full_text_lower']).to_csv("data/processed_news.csv", index=False)
brand_df_final.to_csv("data/brand_mentions.csv", index=False)

with open("data/alerts.txt", "w") as f:
    for a in alerts: 
        f.write(a + "\n")

print(f"🏁 Processing complete. New articles: {len(df)} | Alerts: {len(alerts)}")
