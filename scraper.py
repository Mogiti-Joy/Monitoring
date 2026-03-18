import datetime
import feedparser
import pandas as pd
import os
from textblob import TextBlob

# Classification function
def classify_article(text):
    text = str(text).lower()

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
        
# Sentiment Function
def get_sentiment(text):
    return TextBlob(str(text)).sentiment.polarity

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
def collect_data():
    print("Collecting data...")

    # Define the dictionary INSIDE the function
    rss_feeds = {
        "Africanews": "https://www.africanews.com/feed/",
        # ... (rest of your feeds) ...
        "Africa Urban Development": "https://africanurban.org/feed/"
    }
    
    all_articles = []

    # MOVE THE LOOP INSIDE THE FUNCTION
    for source, url in rss_feeds.items():
        feed = feedparser.parse(url)

        for entry in feed.entries:
            title = entry.get("title", "")
            # ... (rest of your entry parsing code) ...

            article = {
                "source": source,
                "title": title,
                # ...
                "date_collected": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            all_articles.append(article)

    # CREATE DATAFRAME AND SAVE INSIDE THE FUNCTION
    df = pd.DataFrame(all_articles)
    file_name = "daily_news.csv"

    if os.path.exists(file_name):
        df.to_csv(file_name, mode='a', header=False, index=False)
    else:
        df.to_csv(file_name, index=False)

    print(f"Collected {len(df)} articles")

# This calls the function and runs everything inside it
if __name__ == "__main__":
    collect_data()
