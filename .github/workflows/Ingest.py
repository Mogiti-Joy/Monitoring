import pandas as pd
import os
from collections import Counter

print(" Running ingestion pipeline...")

# Safety check
if not os.path.exists("daily_news.csv"):
    print(" No data file found. Run scraper first.")
    exit()

# Load data
df = pd.read_csv("daily_news.csv")

if df.empty:
    print(" No data to process.")
    exit()


# CLEANING
df = df.drop_duplicates(subset='link')
df = df.dropna(subset=['title', 'summary'])

df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')


# FEATURE ENGINEERING
df['date'] = df['published_date'].dt.date
df['hour'] = df['published_date'].dt.hour
df['day_of_week'] = df['published_date'].dt.day_name()

df['text_length'] = df['full_text'].apply(lambda x: len(str(x)))
df['keyword_count'] = df['keywords'].fillna(
    "").apply(lambda x: len(str(x).split(",")))

# ANALYTICS (FROM NOTEBOOK)

# 1. Source distribution
source_counts = df['source'].value_counts().reset_index()
source_counts.columns = ['source', 'article_count']

# 2. Sentiment distribution
sentiment_counts = df['sentiment_label'].value_counts().reset_index()
sentiment_counts.columns = ['sentiment', 'count']

# 3. Category distribution
category_counts = df['category'].value_counts().reset_index()
category_counts.columns = ['category', 'count']

# 4. Brand tracking
companies = ["safaricom", "kcb", "equity bank", "mtn"]

brand_results = []

for company in companies:
    brand_df = df[df['full_text'].str.lower().str.contains(company, na=False)]

    brand_results.append({
        "company": company,
        "mentions": len(brand_df),
        "positive": (brand_df['sentiment_label'] == "Positive").sum(),
        "negative": (brand_df['sentiment_label'] == "Negative").sum(),
        "neutral": (brand_df['sentiment_label'] == "Neutral").sum()
    })

brand_df_final = pd.DataFrame(brand_results)

# 5. Time series (mentions over time)
daily_trend = df.groupby('date').size().reset_index(name='articles_per_day')

# 6. Sentiment trend over time
sentiment_trend = df.groupby(['date', 'sentiment_label']).size().unstack(
    fill_value=0).reset_index()

# 7. Keyword trends
all_keywords = ", ".join(df['keywords'].dropna()).split(", ")
keyword_counts = Counter(all_keywords)
keyword_df = pd.DataFrame(keyword_counts.most_common(
    20), columns=['keyword', 'count'])

# 8. Source influence scoring
source_volume = df['source'].value_counts()
source_sentiment = df.groupby('source')['sentiment_score'].mean()

source_score = pd.DataFrame({
    "volume": source_volume,
    "sentiment": source_sentiment
}).fillna(0).reset_index().rename(columns={"index": "source"})

# ALERT SYSTEM
alerts = []

# Article spike detection
daily_counts = df.groupby('date').size()

if len(daily_counts) > 1:
    today = daily_counts.iloc[-1]
    yesterday = daily_counts.iloc[-2]

    if today > yesterday * 1.5:
        alerts.append("Spike in news volume detected")

# Negative sentiment alert
negative_ratio = (df['sentiment_label'] == 'Negative').mean()

if negative_ratio > 0.5:
    alerts.append("High negative sentiment detected")

# SAVE OUTPUTS
os.makedirs("data", exist_ok=True)

df.to_csv("data/processed_news.csv", index=False)
source_counts.to_csv("data/source_summary.csv", index=False)
sentiment_counts.to_csv("data/sentiment_summary.csv", index=False)
category_counts.to_csv("data/category_summary.csv", index=False)
brand_df_final.to_csv("data/brand_mentions.csv", index=False)
daily_trend.to_csv("data/daily_trend.csv", index=False)
sentiment_trend.to_csv("data/sentiment_trend.csv", index=False)
keyword_df.to_csv("data/keyword_trends.csv", index=False)
source_score.to_csv("data/source_influence.csv", index=False)

with open("data/alerts.txt", "w") as f:
    for alert in alerts:
        f.write(alert + "\n")

print(" Processing complete")
print(f" Articles processed: {len(df)}")
print(f"Alerts triggered: {len(alerts)}")
