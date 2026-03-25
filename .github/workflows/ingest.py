import pandas as pd
import os
from collections import Counter

def run_ingestion():
    print("Running ingestion pipeline...")

    #LOAD & VALIDATE
    file_path = "daily_news.csv"

    if not os.path.exists(file_path):
        print(" No data file found. Run scraper first.")
        return

    df = pd.read_csv(file_path)

    if df.empty:
        print("No data to process.")
        return

    # CLEANING
    df = df.drop_duplicates(subset='link')
    df = df.dropna(subset=['title', 'summary'])

    df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')

    # FEATURE ENGINEERING
    df['date'] = df['published_date'].dt.date
    df['hour'] = df['published_date'].dt.hour
    df['day_of_week'] = df['published_date'].dt.day_name()

    df['text_length'] = df['full_text'].apply(lambda x: len(str(x)))
    df['keyword_count'] = df['keywords'].fillna("").apply(lambda x: len(str(x).split(",")))

  # ANALYTICS
    # Source distribution
    source_summary = df.groupby('source').size().reset_index(name='article_count')

    # Daily trend
    daily_trend = df.groupby('date').size().reset_index(name='articles_per_day')

    # Sentiment summary
    sentiment_summary = df['sentiment_label'].value_counts().reset_index()
    sentiment_summary.columns = ['sentiment', 'count']

    # Category summary
    category_summary = df['category'].value_counts().reset_index()
    category_summary.columns = ['category', 'count']

    #BRAND TRACKING
    companies = ["safaricom", "kcb", "equity bank", "mtn"]

    def find_companies(text):
        text = str(text).lower()
        return [c for c in companies if c in text]

    df['companies_mentioned'] = df['full_text'].apply(find_companies)

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

    brand_summary = pd.DataFrame(brand_results)

    #KEYWORD TRENDS
    all_keywords = ", ".join(df['keywords'].dropna()).split(", ")
    keyword_counts = Counter(all_keywords)

    keyword_trends = pd.DataFrame(
        keyword_counts.most_common(20),
        columns=['keyword', 'count']
    )

    #SENTIMENT TREND
    sentiment_trend = (
        df.groupby(['date', 'sentiment_label'])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    # 8. SOURCE INFLUENCE
    source_volume = df['source'].value_counts()
    source_sentiment = df.groupby('source')['sentiment_score'].mean()

    source_influence = pd.DataFrame({
        "volume": source_volume,
        "avg_sentiment": source_sentiment
    }).fillna(0).reset_index().rename(columns={"index": "source"})

    #ALERT SYSTEM
    alerts = []

    # Negative sentiment alert
    if df['sentiment_score'].mean() < -0.2:
        alerts.append(" Negative sentiment spike detected")

    # Volume spike alert
    daily_counts = df.groupby('date').size()

    if len(daily_counts) > 1:
        today = daily_counts.iloc[-1]
        yesterday = daily_counts.iloc[-2]

        if today > yesterday * 1.5:
            alerts.append("Spike in news volume detected")
    #SAVE OUTPUTS
    os.makedirs("data", exist_ok=True)

    df.to_csv("data/processed_news.csv", index=False)
    source_summary.to_csv("data/source_summary.csv", index=False)
    daily_trend.to_csv("data/daily_trend.csv", index=False)
    sentiment_summary.to_csv("data/sentiment_summary.csv", index=False)
    category_summary.to_csv("data/category_summary.csv", index=False)
    brand_summary.to_csv("data/brand_summary.csv", index=False)
    keyword_trends.to_csv("data/keyword_trends.csv", index=False)
    sentiment_trend.to_csv("data/sentiment_trend.csv", index=False)
    source_influence.to_csv("data/source_influence.csv", index=False)

    with open("data/alerts.txt", "w") as f:
        for alert in alerts:
            f.write(alert + "\n")

    # LOG OUTPUT
    print("Processing complete")
    print(f"Articles processed: {len(df)}")
    print(f"Alerts triggered: {len(alerts)}")


# RUN SCRIPT
if __name__ == "__main__":
    run_ingestion()
