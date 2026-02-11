import marimo

__generated_with = "0.19.9"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Setup
    """)
    return


@app.cell
def _():
    print(f"✅ Starting to pull news articles...")
    return


@app.cell
def _():
    import os
    from dotenv import load_dotenv
    load_dotenv()

    import pandas as pd
    import json
    import datetime
    import requests, time
    from urllib.parse import urlparse
    from supabase import create_client, Client

    #Supbase
    SUPABASE_URL = os.environ["SUPABASE_URL"]
    SUPABASE_KEY = os.environ["SUPABASE_KEY"]
    SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

    supabase: Client = create_client(SUPABASE_URL, SERVICE_ROLE_KEY)
    return datetime, os, pd, requests, supabase, time, urlparse


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Grab data from database
    """)
    return


@app.cell
def _(pd, supabase: "Client"):
    def fetch_data(table_name, batch_size=500, filters=None, related_tables=None):
        try:
            all_data = []
            start = 0
            if related_tables:
                select_string = '*, ' + ', '.join((f'{tbl}(*)' for tbl in related_tables))
            else:
                select_string = '*'
            while True:
                query = supabase.table(table_name).select(select_string)
                if filters:
                    for column, value in filters.items():
                        if isinstance(value, list):
                            query = query.in_(column, value)
                        elif value is None:
                            query = query.is_(column, None)
                        else:
                            query = query.eq(column, value)
                _response = query.range(start, start + batch_size - 1).execute()
                if _response.data:
                    all_data.extend(_response.data)
                    start = start + batch_size
                    if len(_response.data) < batch_size:
                        break
                else:
                    break
            if all_data:
                print(f"✅ Successfully fetched `{table_name}` table with filter '{filters}' and {len(all_data)} rows.")
                return pd.DataFrame(all_data)
            else:
                print(f'⚠️ `{table_name}` is empty.')
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ Error fetching data from '{table_name}': {e}")
            return pd.DataFrame()
    companies = fetch_data('companies', filters={'status': ['trial', 'active']})
    competitors = fetch_data('competitors')
    filtered_competitors = competitors[competitors['company_id'].isin(companies['id'])]
    filtered_competitors = filtered_competitors.reset_index(drop=True)
    competitors = filtered_competitors
    print(f'✅ Filtered to {len(companies)} trial and active accounts with {len(competitors)} competitors')
    return companies, competitors


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Collect articles
    """)
    return


@app.cell
def _(os):
    url = "https://google-news13.p.rapidapi.com/search"
    headers = {
        "x-rapidapi-key": os.environ["RAPIDAPI_KEY"],
        "x-rapidapi-host": "google-news13.p.rapidapi.com"
    }
    return headers, url


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Collects news for competitors
    """)
    return


@app.cell
def _(competitors, headers, pd, requests, time, url, urlparse):
    news_rows = []
    for index, _row in competitors.iterrows():
        competitor_id = _row['id']
        website_url = _row['website_url']
        competitor_name = _row['competitor_name']
        parsed = urlparse(website_url)
        domain = (parsed.netloc or parsed.path).replace('www.', '').strip('/')
        query = _row['competitor_name']  # Extract domain
        search_terms = [query, domain]
        print(f'\n🔎 Searching for competitor {competitor_name} using:')
        print('   1) Query:', query)
        print('   2) Domain:', domain)  # LLM-generated search query
        for term in search_terms:  # query = row["news_search_query"]
            params = {'keyword': term, 'lr': 'en-US'}
            try:  # Temporarily replacing search query with competitor name
                _response = requests.get(url, headers=headers, params=params)
                data = _response.json()
                items = data.get('items', [])  # We now search *both*:
                print(f"   → Found {len(items)} articles for '{term}'")
                for article in items:
                    news_rows.append({'competitor_id': competitor_id, 'domain': domain, 'search_term': term, 'timestamp': article.get('timestamp'), 'title': article.get('title'), 'snippet': article.get('snippet'), 'url': article.get('newsUrl'), 'publisher': article.get('publisher'), 'thumbnail': article.get('images', {}).get('thumbnail')})
            except Exception as e:
                print(f"❌ Error searching '{term}': {e}")
            time.sleep(0.7)
    news_df = pd.DataFrame(news_rows)
    news_df = news_df.drop_duplicates(subset=['url'])
    # Build DataFrame + dedupe URLs
    news_df.head(5)  # Store results
    return (news_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Collect news for company related keywords
    """)
    return


@app.cell
def _(companies):
    companies_with_keywords = companies[companies["news_topics_search"].notnull()]
    companies_with_keywords.head()
    return


@app.cell
def _():
    # keyword_news_rows = []

    # for _, row in companies_with_keywords.iterrows():
    #     company_id = row["id"]
    #     company_name = row.get("company_name") or row.get("name")  # just in case naming differs
    #     clusters = row["news_topics_search"] or []

    #     print(f"\n🔎 Searching for company {company_name} ({company_id}):")

    #     # clusters is expected to be:
    #     # [{"label": "...", "keywords": ["...", "..."]}, ...]
    #     for cluster in clusters:
    #         label = cluster.get("label", "Unlabeled")
    #         keywords = cluster.get("keywords", []) or []

    #         for term in keywords:
    #             params = {"keyword": term, "lr": "en-US"}  # IMPORTANT: term (not the whole list)

    #             try:
    #                 response = requests.get(url, headers=headers, params=params, timeout=30)
    #                 data = response.json()
    #                 items = data.get("items", [])

    #                 print(f"   → [{label}] '{term}': {len(items)} articles")

    #                 for article in items:
    #                     keyword_news_rows.append({
    #                         "company_id": company_id,
    #                         "company_name": company_name,
    #                         "topic_label": label,
    #                         "search_term": term,
    #                         "timestamp": article.get("timestamp"),
    #                         "title": article.get("title"),
    #                         "snippet": article.get("snippet"),
    #                         "url": article.get("newsUrl"),
    #                         "publisher": article.get("publisher"),
    #                         "thumbnail": (article.get("images") or {}).get("thumbnail"),
    #                         "source": "google_news_api"
    #                     })

    #             except Exception as e:
    #                 print(f"❌ Error searching [{label}] '{term}': {e}")

    #             time.sleep(0.7)

    # # Build DataFrame + dedupe URLs
    # keyword_news_df = pd.DataFrame(keyword_news_rows)

    # if not keyword_news_df.empty:
    #     keyword_news_df = keyword_news_df.drop_duplicates(subset=["url"])

    # keyword_news_df.head(5)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Convert timestamp
    """)
    return


@app.cell
def _(datetime, news_df):
    news_df["timestamp"] = news_df["timestamp"].apply(
        lambda ts: datetime.datetime.fromtimestamp(int(ts)/1000, tz=datetime.timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S+00:00")
    )
    news_df

    # keyword_news_df["timestamp"] = keyword_news_df["timestamp"].apply(
    #     lambda ts: datetime.datetime.fromtimestamp(int(ts)/1000, tz=datetime.timezone.utc)
    #         .strftime("%Y-%m-%dT%H:%M:%S+00:00")
    # )
    # keyword_news_df
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Drop all rows old than two weeks
    """)
    return


@app.cell
def _(news_df, pd):
    news_df['timestamp'] = pd.to_datetime(news_df['timestamp'], utc=True, errors='coerce')
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(weeks=1)
    filtered_df = news_df[news_df['timestamp'] >= cutoff].copy()
    print(f'Before: {len(news_df)} rows')
    print(f'After:  {len(filtered_df)} rows')
    news_df_1 = filtered_df
    # Define cutoff 1 week ago
    # Filter
    news_df_1.head(5)
    return (news_df_1,)


@app.cell
def _():
    # # Ensure proper datetime format
    # keyword_news_df["timestamp"] = pd.to_datetime(
    #     keyword_news_df["timestamp"],
    #     # unit="ms",
    #     utc=True
    # )

    # # Define cutoff 1 week ago
    # cutoff = pd.Timestamp.utcnow() - pd.Timedelta(weeks=1)

    # # Filter
    # filtered_df = keyword_news_df[keyword_news_df["timestamp"] >= cutoff].copy()

    # print(f"Before: {len(keyword_news_df)} rows")
    # print(f"After:  {len(filtered_df)} rows")
    # keyword_news_df = filtered_df
    # keyword_news_df
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Send to supabase
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Competitor related news
    """)
    return


@app.cell
def _(news_df_1, pd, supabase: "Client"):
    rows = []
    for _, _row in news_df_1.iterrows():
        rows.append({'competitor_id': _row['competitor_id'], 'published_date': _row['timestamp'].isoformat() if pd.notna(_row['timestamp']) else None, 'publisher': _row['publisher'], 'title': _row['title'], 'description': _row['snippet'], 'url': _row['url'], 'thumbnail': _row['thumbnail']})
    _response = supabase.table('news_feed').upsert(rows, on_conflict='url').execute()
    count = len(_response.data) if _response.data else 0
    print(f'Upsert complete — {count} rows inserted/updated')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Company keyword related news
    """)
    return


@app.cell
def _():
    # keyword_news_df.head(1)
    return


@app.cell
def _():
    # rows = []

    # for _, row in keyword_news_df.iterrows():
    #     rows.append({
    #         "company_id": row["company_id"],
    #         "published_date": row["timestamp"].isoformat() if pd.notna(row["timestamp"]) else None,
    #         "publisher": row["publisher"],
    #         "title": row["title"],
    #         "description": row["snippet"],
    #         "url": row["url"],
    #         "thumbnail": row["thumbnail"]
    #     })

    # response = supabase.table("news_feed").upsert(
    #     rows,
    #     on_conflict="url"
    # ).execute()

    # count = len(response.data) if response.data else 0

    # print(f"Upsert complete — {count} rows inserted/updated")
    return


if __name__ == "__main__":
    app.run()