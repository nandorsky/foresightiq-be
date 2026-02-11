import marimo

__generated_with = "0.19.9"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    print("Starting linkedin feed script...")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Setup
    """)
    return


@app.cell
def _():
    import os
    from dotenv import load_dotenv
    load_dotenv()

    import requests
    import json
    import pandas as pd
    import anthropic
    from supabase import create_client, Client
    import time
    import math
    from time import perf_counter
    from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
    import asyncio
    import aiohttp

    # Supabase API
    SUPABASE_URL = os.environ["SUPABASE_URL"]
    SUPABASE_KEY = os.environ["SUPABASE_KEY"]
    SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

    supabase: Client = create_client(SUPABASE_URL, SERVICE_ROLE_KEY)

    anthropic_api_key = os.environ["ANTHROPIC_API_KEY"]
    client = anthropic.Anthropic(
        api_key=anthropic_api_key,
    )
    return aiohttp, asyncio, os, pd, supabase


@app.cell
def _(pd, supabase: "Client"):
    # ✅ Function to Fetch Data from Supabase
    def fetch_data(table_name, batch_size=500, filters=None, related_tables=None):
        try:
            all_data = []
            start = 0
            if related_tables:
                select_string = '*, ' + ', '.join((f'{tbl}(*)' for tbl in related_tables))  # Build select string
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
                response = query.range(start, start + batch_size - 1).execute()
                if response.data:
                    all_data.extend(response.data)
                    start = start + batch_size
                    if len(response.data) < batch_size:
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
    # ✅ Fetch data from tables
    competitors = fetch_data('competitors')
    return companies, competitors


@app.cell
def _(companies, competitors):
    # Filter competitors to only trial accounts
    filtered_competitors = competitors[competitors['company_id'].isin(companies['id'])]
    filtered_competitors = filtered_competitors.reset_index(drop=True)
    # Optional: reset index if you want a clean one
    competitors_1 = filtered_competitors
    print(f'✅ Filtered to {len(companies)} trial accounts with {len(competitors_1)} competitors')
    return (competitors_1,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # LinkedIn Feed
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Search by keyword
    """)
    return


@app.cell
def _(competitors_1):
    # competitors = competitors[competitors["company_id"] == 73]
    competitors_1
    return


@app.cell
async def _(aiohttp, asyncio, competitors_1, os, pd):
    print('Starting linkedin feed keyword competitor search..')
    API_URL = 'https://professional-network-data.p.rapidapi.com/search-posts'
    HEADERS = {'x-rapidapi-key': os.environ["RAPIDAPI_KEY"], 'x-rapidapi-host': 'professional-network-data.p.rapidapi.com', 'Content-Type': 'application/json'}
    MAX_PAGES = 5
    CONCURRENCY_LIMIT = 1

    async def fetch_page(session, competitor_id, competitor_name, linkedin_id, page, sem, search_type):
        payload = {'keyword': '', 'sortBy': 'date_posted', 'datePosted': 'pastWeek', 'page': page, 'contentType': '', 'fromMember': [], 'fromCompany': [], 'mentionsMember': [], 'mentionsOrganization': [], 'authorIndustry': [], 'authorCompany': [], 'authorTitle': ''}
        if search_type == 'mentions':
            payload['mentionsOrganization'] = [linkedin_id]
            payload['fromCompany'] = []
        elif search_type == 'from_company':
            payload['fromCompany'] = [linkedin_id]
            payload['mentionsOrganization'] = []
        else:  # Base payload
            raise ValueError(f'Unknown search_type: {search_type}')
        async with sem:
            try:
                async with session.post(API_URL, json=payload, headers=HEADERS, timeout=30) as resp:
                    data = await resp.json()
                    posts = data.get('data', {}).get('items') or []
                    if not posts:
                        print(f'No more posts for {competitor_name} [{search_type}] page {page}')
                        return []
                    results = []
                    for post in posts:
                        author = post.get('author') or {}
                        pics = author.get('profilePictures') or []
                        highest_res_pic = sorted(pics, key=lambda x: x.get('width', 0), reverse=True)[0]['url'] if pics else None
                        results.append({'competitor_id': competitor_id, 'competitor_name': competitor_name, 'search_type': search_type, 'text': post.get('text'), 'postUrl': post.get('url'), 'postedDate': post.get('postedDate'), 'author_id': author.get('id'), 'author_fullName': author.get('fullName'), 'author_username': author.get('username'), 'author_url': author.get('url'), 'headline': author.get('headline'), 'author_profile_pic': highest_res_pic})
                    print(f'Processed {competitor_name} [{search_type}] page {page} — {len(posts)} posts')  # Apply OR logic via separate requests:
                    return results
            except Exception as e:
                print(f'❌ Error {competitor_name} [{search_type}] page {page}: {e}')
                return []

    async def fetch_all_competitors():
        linkedin_posts = []
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        async with aiohttp.ClientSession() as session:
            tasks = []  # rate limit
            for _, row in competitors_1.iterrows():
                competitor_name = row['competitor_name']
                competitor_id = int(row['id'])
                linkedin_id = row.get('linkedin_id')
                if not linkedin_id:
                    print(f'Skipping {competitor_name} — no linkedin_id')
                    continue
                for search_type in ('mentions', 'from_company'):
                    for page in range(1, MAX_PAGES + 1):
                        tasks.append(fetch_page(session=session, competitor_id=competitor_id, competitor_name=competitor_name, linkedin_id=linkedin_id, page=page, sem=sem, search_type=search_type))
            results = await asyncio.gather(*tasks)
            for batch in results:
                if batch:
                    linkedin_posts.extend(batch)
            return linkedin_posts
    linkedin_posts = await fetch_all_competitors()
    linkedin_feed = pd.DataFrame(linkedin_posts)
    # run async function in Jupyter
    print('Completed linkedin feed keyword competitor search..')  # Run everything concurrently  # Flatten list-of-lists  # safety check
    return (linkedin_feed,)


@app.cell
def _(linkedin_feed):
    linkedin_feed.head(50)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Send data back to supabase
    """)
    return


@app.cell
def _(linkedin_feed, pd):
    # Remove trailing " UTC"
    linkedin_feed['postedDate'] = pd.to_datetime(
        linkedin_feed['postedDate'].str.replace(" UTC", "", regex=False),
        format='mixed',
        errors='coerce'
    ).dt.date.astype(str)

    # Convert to datetime and extract date as string in YYYY-MM-DD format
    linkedin_feed['postedDate'] = pd.to_datetime(linkedin_feed['postedDate']).dt.date.astype(str)
    return


@app.cell
def _(linkedin_feed, pd, supabase: "Client"):
    # 1. Remove duplicates in-memory before upsert
    linkedin_feed_1 = linkedin_feed.drop_duplicates(subset=['postUrl'], keep='last')
    rows = []
    for _, row in linkedin_feed_1.iterrows():
        rows.append({'author_id': int(row['author_id']) if pd.notna(row['author_id']) else None, 'author_fullName': row.get('author_fullName'), 'author_profile_pic': row.get('author_profile_pic'), 'postUrl': row.get('postUrl'), 'headline': row.get('headline'), 'author_url': row.get('author_url'), 'text': row.get('text'), 'postedDate': row.get('postedDate'), 'competitor_id': int(row['competitor_id']) if pd.notna(row['competitor_id']) else None})
    response = supabase.table('linkedin_feed').upsert(rows, on_conflict='postUrl').execute()
    count = len(response.data) if response.data else 0
    # 2. Upsert batch
    print(f'Upsert complete — {count} rows inserted/updated')
    return


if __name__ == "__main__":
    app.run()