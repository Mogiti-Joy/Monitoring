import pandas as pd
import os
from collections import Counter
from sqlalchemy import create_engine, text

print("Running MediaPulse Ingestion Pipeline...")

# 1. ROBUST DATA LOADING
if not os.path.exists("daily_news.csv"):
    print("No data file found. Run scraper first.")
    exit()

df = pd.read_csv("daily_news.csv")
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# 2. SMART DEDUPLICATION (Checking against Neon)
try:
    with engine.connect() as conn:
        existing_links = pd.read_sql("SELECT link FROM news", conn)['link'].tolist()
    
    # Only keep links NOT already in the database
    initial_count = len(df)
    df = df[~df['link'].isin(existing_links)]
    print(f"Found {len(df)} new articles (Filtered out {initial_count - len(df)} duplicates).")
except Exception as e:
    print(f"Database check skipped (Table might not exist): {e}")

if df.empty:
    print("No new data to process. Exiting.")
    exit()

# 3. DATA CLEANING & UPLOAD
# Don't drop entire rows for missing summaries; fill them instead
df['summary'] = df['summary'].fillna("Summary unavailable")
df = df.dropna(subset=['title', 'link']) 

# Push NEW data to Neon
df.to_sql("news", engine, if_exists="append", index=False)
print("New data synced to Neon PostgreSQL")

# 4. REFINED FEATURE ENGINEERING
df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
df = df.dropna(subset=['published_date']) # Drop if date is unparseable

df['date'] = df['published_date'].dt.date
df['hour'] = df['published_date'].dt.hour
df['day_of_week'] = df['published_date'].dt.day_name()

# Vectorized length calculations (much faster than .apply)
df['text_length'] = df['full_text'].str.len().fillna(0)
df['keyword_count'] = df['keywords'].fillna("").str.count(",") + 1

# Improved Virality Score
df['virality_score'] = (
    df['text_length'] * 0.1 + 
    df['keyword_count'] * 5 + 
    df['sentiment_score'].abs() * 20
)

# 5. BRAND TRACKING (Optimized Loop)
companies = [
    "safaricom", "kcb", "equity bank", "mtn", "airtel",
    "vodacom", "standard bank", "absa", "ecobank", "kenya airways",
    "google", "microsoft", "amazon", "CEMA", "SFA", "AWF", "MPESA Foundation"
]

# Pre-convert to lowercase for faster matching
df['full_text_lower'] = df['full_text'].str.lower().fillna("")

brand_results = []
for company in companies:
    # Use vectorized string contains
    mask = df['full_text_lower'].str.contains(company.lower(), na=False)
    brand_df = df[mask]
    
    if not brand_df.empty:
        brand_results.append({
            "company": company,
            "mentions": len(brand_df),
            "avg_sentiment": brand_df['sentiment_score'].mean(),
            "status": "Positive" if brand_df['sentiment_score'].mean() > 0.1 else "Negative" if brand_df['sentiment_score'].mean() < -0.1 else "Neutral"
        })

brand_df_final = pd.DataFrame(brand_results)

# 6. ALERT SYSTEM (Speed Optimized)
alerts = []
latest_date = df['date'].max()
prev_date = df[df['date'] < latest_date]['date'].max()

if pd.notna(prev_date):
    recent_volume = len(df[df['date'] == latest_date])
    prev_volume = len(df[df['date'] == prev_date])
    
    if recent_volume > prev_volume * 1.5:
        alerts.append(f"Volume Spike: {recent_volume} articles today vs {prev_volume} yesterday.")

# Negative Sentiment Watchdog
neg_df = df[df['sentiment_label'] == 'Negative']
if len(neg_df) / len(df) > 0.4:
    alerts.append("High Crisis Alert: Over 40% of news today is negative.")

# 7. EXPORTING
os.makedirs("data", exist_ok=True)
df.drop(columns=['full_text_lower']).to_csv("data/processed_news.csv", index=False)
brand_df_final.to_csv("data/brand_mentions.csv", index=False)

with open("data/alerts.txt", "w") as f:
    for a in alerts: f.write(a + "\n")

print(f"🏁 Processing complete. Articles: {len(df)} | Alerts: {len(alerts)}")
