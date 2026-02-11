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
    print("Starting jobs feed script...")

    import requests
    import json
    import pandas as pd
    import anthropic
    import pyperclip
    from supabase import create_client, Client
    import time
    from typing import List, Dict
    import asyncio
    import aiohttp
    import nest_asyncio

    # Supabase API
    SUPABASE_URL = "https://joiadqjirxyxvikqfmqh.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpvaWFkcWppcnh5eHZpa3FmbXFoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzEzNDk4MzEsImV4cCI6MjA0NjkyNTgzMX0.ETQn27LPXwovdMD4cApUUTI0d9z13G5XLRVEQMu_3Oc"
    SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpvaWFkcWppcnh5eHZpa3FmbXFoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMTM0OTgzMSwiZXhwIjoyMDQ2OTI1ODMxfQ.-_RI7buYH5I_CbxWvjuNNtrnfZexpmjdeSdTG3VJGEc"

    supabase: Client = create_client(SUPABASE_URL, SERVICE_ROLE_KEY)
    return aiohttp, asyncio, nest_asyncio, pd, requests, supabase, time


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Fetch data from supabase
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
    jobs = fetch_data('jobs')
    return companies, competitors, jobs


@app.cell
def _(companies, competitors):
    # Filter competitors to only trial accounts
    filtered_competitors = competitors[competitors['company_id'].isin(companies['id'])]
    filtered_competitors = filtered_competitors.reset_index(drop=True)
    # Optional: reset index if you want a clean one
    competitors_1 = filtered_competitors
    print(f'✅ Filtered to {len(companies)} trial and active accounts with {len(competitors_1)} competitors')
    return (competitors_1,)


@app.cell
def _(competitors_1, pd):
    filtered_df = competitors_1[competitors_1['linkedin_id'].notna() & (competitors_1['linkedin_id'] != 0)]
    competitors_2 = filtered_df
    competitors_2['linkedin_id'] = pd.to_numeric(competitors_2['linkedin_id'], errors='coerce').fillna(0).astype(int)
    # Replace NaN with 0, convert to numeric safely, then cast to int
    # Print number of competitors kept
    print(f'✅ {len(competitors_2)} competitors kept after filtering.')
    return (competitors_2,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Job Data API
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Get list of jobs
    """)
    return


@app.cell
def _(competitors_2, pd, requests, time):
    _url = 'https://professional-network-data.p.rapidapi.com/search-jobs'
    _headers = {'x-rapidapi-key': 'e88f5d3d95msh96c8e7a091f4a90p1bee3cjsn2b537c4486ca', 'x-rapidapi-host': 'professional-network-data.p.rapidapi.com'}
    all_jobs = []
    for _, _row in competitors_2.iterrows():
        competitor_name = _row['competitor_name']
        _competitor_id = _row['id']
        linkedin_id = _row['linkedin_id']
        querystring = {'companyIds': str(linkedin_id), 'locationId': '92000000', 'datePosted': 'anyTime', 'sort': 'mostRecent'}
        print(querystring)
        try:
            _response = requests.get(_url, headers=_headers, params=querystring)
            _response.raise_for_status()
            result = _response.json()
            if isinstance(result, dict) and isinstance(result.get('data'), list):
                for job in result['data']:
                    job['competitor_name'] = competitor_name
                    job['competitor_id'] = _competitor_id
                    all_jobs.append(job)
                print(f'Jobs collected for {competitor_name}')
            else:
                print(f'Unexpected response format for {competitor_name}: {result}')
        except Exception as e:
            print(f'Error fetching jobs for {competitor_name}: {e}')
        time.sleep(1)
    jobs_df = pd.DataFrame(all_jobs)
    print('Grabbed jobs via API...')
    return (jobs_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Remove jobs that already exist
    """)
    return


@app.cell
def _(jobs, jobs_df):
    # Extract ID and append to dataframe
    jobs_df['job_id'] = jobs_df['url'].str.extract(r'/view/(\d+)')
    jobs['job_id'] = jobs['url'].str.extract(r'/view/(\d+)')
    return


@app.cell
def _(jobs, jobs_df):
    # Ensure both id columns are strings
    jobs['job_id'] = jobs['job_id'].astype(str)
    jobs_df['job_id'] = jobs_df['job_id'].astype(str)
    count_before = len(jobs_df)
    # Count before filtering
    jobs_df_filtered = jobs_df[~jobs_df['job_id'].isin(jobs['job_id'])]
    count_after = len(jobs_df_filtered)
    # Filter out matching job_ids
    jobs_df_1 = jobs_df_filtered
    # Count after filtering
    # Set dataframe back to jobs_df
    print(f'Dropped duplicate jobs, {len(jobs_df_1)} jobs remaining...')
    return (jobs_df_1,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Get job details
    """)
    return


@app.cell
async def _(aiohttp, asyncio, jobs_df_1, nest_asyncio, pd, time):
    nest_asyncio.apply()
    _url = 'https://professional-network-data.p.rapidapi.com/get-job-details'
    _headers = {'x-rapidapi-key': 'e88f5d3d95msh96c8e7a091f4a90p1bee3cjsn2b537c4486ca', 'x-rapidapi-host': 'professional-network-data.p.rapidapi.com'}
    MAX_REQUESTS_PER_MINUTE = 50
    MIN_INTERVAL = 60 / MAX_REQUESTS_PER_MINUTE
    last_request_time = 0
    rate_lock = asyncio.Lock()

    async def rate_limited_get(session, params):
        global last_request_time
        async with rate_lock:
            elapsed = time.time() - last_request_time
            if elapsed < MIN_INTERVAL:
                await asyncio.sleep(MIN_INTERVAL - elapsed)
            last_request_time = time.time()
            return await session.get(_url, headers=_headers, params=params, timeout=20)

    async def fetch_job(session, row):
        job_id = _row['id']
        competitor_name = _row['competitor_name']
        params = {'id': job_id}
        try:
            resp = await rate_limited_get(session, params)
            if resp.status == 429:
                print(f'⚠️ STILL got 429 for {competitor_name} ({job_id}) — slowing further')
                await asyncio.sleep(2)
                return None
            if resp.status != 200:
                print(f'⚠️ {competitor_name} ({job_id}) HTTP {resp.status}')
                return None
            data = await resp.json()
            if 'data' in data and isinstance(data['data'], dict):
                job = data['data']
                job['competitor_id'] = _row['competitor_id']
                job['competitor_name'] = competitor_name
                print(f'✅ Job: {competitor_name} ({job_id})')
                return job
            print(f'⚠️ Unexpected format for {competitor_name}')
            return None
        except Exception as e:
            print(f'❌ Error {competitor_name} ({job_id}): {e}')
            return None

    async def fetch_all(df):
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_job(session, _row) for _, _row in df.iterrows()]
            return await asyncio.gather(*tasks)
    results = await fetch_all(jobs_df_1)
    job_descriptions_df = pd.DataFrame([r for r in results if r])
    job_descriptions_df.head()
    return (job_descriptions_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Send to supabase
    """)
    return


@app.cell
def _(job_descriptions_df):
    job_descriptions_df.groupby("competitor_name").size()
    return


@app.cell
def _(job_descriptions_df, supabase: "Client"):
    for _, _row in job_descriptions_df.iterrows():
        _competitor_id = int(_row['competitor_id'])
        title = _row['title']
        description = _row['description']
        postedAt_raw = _row['originalListedDate']
        postedAt = postedAt_raw.split(' ')[0]
        _url = _row['url']
        _response = supabase.table('jobs').insert({'competitor_id': _competitor_id, 'title': title, 'postedAt': postedAt, 'description': description, 'url': _url, 'processed': False}).execute()
        if _response.data:
            print(f'✅ Insert record {title}')
        else:
            print(f'❌ Failed to insert record {title}')
    print('Inserted job descriptions into supabase...')  # "relevant": True,
    return


if __name__ == "__main__":
    app.run()