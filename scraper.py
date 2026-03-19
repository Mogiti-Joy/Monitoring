import datetime
import feedparser
import pandas as pd
import os
import re
from textblob import TextBlob
def extract_keywords(text):
    words = text.lower().split()
    return ", ".join(list(set([w for w in words if len(w) > 5]))[:5])
def clean_text(text):
    text = str(text)
    text = re.sub(r'<.*?>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()
def classify_article(text):
    text = text.lower()
    if any(word in text for word in ["ai", "artificial intelligence", "machine learning"]):
        return "AI"
    elif any(word in text for word in ["health", "hospital", "disease", "malaria", "covid"]):
        return "Health"
    elif any(word in text for word in ["election", "government", "president", "parliament"]):
        return "Politics"
    elif any(word in text for word in ["business", "market", "finance", "economy"]):
        return "Business"
    elif any(word in text for word in ["climate", "flood", "drought", "weather"]):
        return "Climate"
    else:
        return "Other"
def get_sentiment(text):
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    if polarity > 0:
        label = "Positive"
    elif polarity < 0:
        label = "Negative"
    else:
        label = "Neutral"
    return polarity, label
def collect_data():
    print("Collecting data...")
    
    # RSS feeds (you can paste ALL yours here)
    rss_feeds = {
        "Africanews": "https://www.africanews.com/feed/",
        "AllAfrica": "https://allafrica.com/tools/headlines/rdf/latest/headlines.rdf",
        "Nation Africa": "https://nation.africa/rss",
        "Business Daily Africa": "https://www.businessdailyafrica.com/rss",
        "The Standard Kenya": "https://www.standardmedia.co.ke/rss",
        "Capital FM Kenya": "https://www.capitalfm.co.ke/news/feed/",
        "Kenyans.co.ke": "https://www.kenyans.co.ke/rss.xml",
        # West Africa
"Premium Times Nigeria": "https://www.premiumtimesng.com/feed",
"Guardian Nigeria": "https://guardian.ng/feed/",
"BusinessDay Nigeria": "https://businessday.ng/feed/",
"Punch Nigeria": "https://punchng.com/feed/",
"Vanguard Nigeria": "https://www.vanguardngr.com/feed/",
"Daily Trust Nigeria": "https://dailytrust.com/feed/",
"Sahara Reporters": "https://saharareporters.com/feeds/latest",
"Pulse Nigeria": "https://www.pulse.ng/rss",
"GhanaWeb": "https://www.ghanaweb.com/GhanaHomePage/rss.xml",
"Graphic Online Ghana": "https://www.graphic.com.gh/rss.html",
"Citi Newsroom Ghana": "https://citinewsroom.com/feed/",
"Joy Online Ghana": "https://www.myjoyonline.com/feed/",
"Modern Ghana": "https://www.modernghana.com/rss/news.xml",

# Southern Africa
"News24 South Africa": "https://www.news24.com/news24/rss",
"Daily Maverick": "https://www.dailymaverick.co.za/feed/",
"Mail and Guardian": "https://mg.co.za/feed/",
"BusinessTech": "https://businesstech.co.za/news/feed/",
"TimesLive": "https://www.timeslive.co.za/rss/",
"IOL South Africa": "https://www.iol.co.za/cmlink/1.640",
"Engineering News": "https://www.engineeringnews.co.za/page/rss",
"SA News": "https://www.sanews.gov.za/rss.xml",
"The Namibian": "https://www.namibian.com.na/feed/",
"ZimLive": "https://www.zimlive.com/feed/",
"The Zimbabwe Mail": "https://www.thezimbabwemail.com/feed/",

# North Africa
"Egypt Today": "https://www.egypttoday.com/feed",
"Ahram Online": "https://english.ahram.org.eg/RSS/",
"Morocco World News": "https://www.moroccoworldnews.com/feed/",
"Algerie Presse Service": "https://www.aps.dz/en/feed",
"Tunisia Live": "https://www.tunisialive.net/feed/",

# Tech & Innovation
"TechCabal": "https://techcabal.com/feed/",
"TechPoint Africa": "https://techpoint.africa/feed/",
"Disrupt Africa": "https://disruptafrica.com/feed/",
"African Business Tech": "https://africanbusinessmagazine.com/category/technology/feed/",
"IT News Africa": "https://www.itnewsafrica.com/feed/",
"Connecting Africa": "https://www.connectingafrica.com/rss.xml",
"TechTrends Kenya": "https://techtrendske.co.ke/feed/",
"Benjamindada.com": "https://www.benjamindada.com/feed/",
"Condia": "https://thecondia.com/feed/",
"Technext Nigeria": "https://technext.ng/feed/",

# Business & Economy
"African Business Magazine": "https://africanbusinessmagazine.com/feed/",
"Africa.com": "https://www.africa.com/feed/",
"Africa Finance": "https://africafinance.com/feed/",
"Further Africa": "https://furtherafrica.com/feed/",
"How We Made It In Africa": "https://www.howwemadeitinafrica.com/feed/",
"Ventures Africa": "https://venturesafrica.com/feed/",
"Africa Intelligence": "https://www.africaintelligence.com/rss",
"Ecofin Agency": "https://www.ecofinagency.com/rss",
"Africa Briefing": "https://www.africabriefing.com/feed",

# Development & NGOs
"Devex": "https://www.devex.com/news/rss",
"ReliefWeb Africa": "https://reliefweb.int/updates/rss.xml",
"Thomson Reuters Foundation": "https://news.trust.org/feed/",
"World Bank Africa": "https://www.worldbank.org/en/region/afr/rss",
"UN News Africa": "https://news.un.org/feed/subscribe/en/news/region/africa/feed/rss.xml",

# Additional African media
"Daily Nation Business": "https://nation.africa/kenya/business/rss",
"Kenya Wallstreet": "https://kenyawallstreet.com/feed/",
"The Africa Logistics": "https://www.theafricalogistics.com/feed/",
"African Leadership Magazine": "https://afrleadership.com/feed/",
"Africa Feeds": "https://africafeeds.com/feed/",
"Africa Daily": "https://www.africadaily.net/feed/",
"Africa Newsroom": "https://www.africanewsroom.com/feed/",
"Africa Oil and Power": "https://www.africaoilandpower.com/feed/",
"Africa Energy Portal": "https://africa-energy-portal.org/feed/",
"Africa Science News": "https://africasciencenews.org/feed/",
"Africa Agriculture": "https://africulture.net/feed/",
"Africa Climate News": "https://climate-africa.com/feed/",
"Africa Mining": "https://www.miningafrica.net/feed/",
"Africa Ports": "https://africaports.co.za/feed/",
"Africa Infrastructure": "https://africainfrastructure.com/feed/",
"Africa Renewable Energy": "https://renewableenergyafrica.com/feed/",
"Africa Water Journal": "https://africawaterjournal.com/feed/",
"Africa Food Security": "https://africafoodsecurity.org/feed/",
"Africa Transport": "https://africatransportpolicy.org/feed/",
"Africa Urban Development": "https://africanurban.org/feed/"
    }
    all_articles = []
    for source, feed in rss_feeds.items():
        try:
            for entry in feed.entries:
                try:
                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    link = entry.get("link", "")
                    author = entry.get("author", "Unknown")
                    published = entry.get("published", "")
                    full_text = f"{title} {summary}"
                    category = classify_article(full_text)
                    sentiment_score, sentiment_label = get_sentiment(full_text)
                    keywords = extract_keywords(full_text)
                    article = {
                        "id": link,
                        "source": source,
                        "title": title,
                        "summary": summary,
                        "full_text": full_text,
                        "link": link,
                        "author": author,
                        "published_date": published,
                        "collected_date": datetime.datetime.now(),
                        "category": category,
                        "sentiment_score": sentiment_score,
                        "sentiment_label": sentiment_label,
                        "keywords": keywords
                }
                all_articles.append(article)
            except Exception as e:
                print(f"Error processing entry from {source}: {e}")
    except Exception as e:
        print(f"Error with source {source}: {e}")
                    try:
                        if all_articles:
                            df = pd.DataFrame(all_articles)
                            df.drop_duplicates(subset=["id"], inplace=True)
                            file_name = "news_dataset.csv"
                            if os.path.exists(file_name):
                                existing_df = pd.read_csv(file_name)
                                combined_df = pd.concat([existing_df, df], ignore_index=True)
                                combined_df.drop_duplicates(subset=["id"], inplace=True)
                                combined_df.to_csv(file_name, index=False)
                                new_count = len(combined_df) - len(existing_df)
                                print(f"Added {new_count} new articles")
                            else:
                                df.to_csv(file_name, index=False)
                                print(f"Collected {len(df)} articles (first run)")
                        else:
                            print("No articles collected")
                    except Exception as e:
                        print(f"Error saving data: {e}")
                    collect_data()

                        
                
                        
                        

