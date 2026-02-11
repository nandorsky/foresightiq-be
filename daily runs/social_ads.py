import marimo

__generated_with = "0.19.9"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    import pandas as pd
    import datetime as dt
    import praw
    import requests
    import json
    from supabase import create_client, Client
    import time
    import datetime
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    SUPABASE_URL = os.environ['SUPABASE_URL']
    SUPABASE_KEY = os.environ['SUPABASE_KEY']
    SERVICE_ROLE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
    supabase: Client = create_client(SUPABASE_URL, SERVICE_ROLE_KEY)
    return datetime, dt, json, os, pd, requests, supabase, time


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Get competitor data
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
                _response = query.range(start, start + _batch_size - 1).execute()
                if _response.data:
                    all_data.extend(_response.data)
                    start = start + _batch_size
                    if len(_response.data) < _batch_size:
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
    ads = fetch_data('ad_library')
    return companies, competitors


@app.cell
def _(companies, competitors):
    # Filter competitors to only trial accounts
    filtered_competitors = competitors[competitors['company_id'].isin(companies['id'])]
    filtered_competitors = filtered_competitors.reset_index(drop=True)
    # Optional: reset index if you want a clean one
    competitors_1 = filtered_competitors
    print(f'✅ Filtered to {len(companies)} trial and active accounts with {len(competitors_1)} competitors')
    return (competitors_1,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Facebook ads
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Grab facebook JSON response
    """)
    return


@app.cell
def _(competitors_1, os, pd, requests):
    _url = 'https://meta-facebook-ad-library.p.rapidapi.com/getPageAds'
    _headers = {'x-rapidapi-key': os.environ["RAPIDAPI_KEY"], 'x-rapidapi-host': 'meta-facebook-ad-library.p.rapidapi.com'}
    _records = []
    for _, _row in competitors_1.iterrows():
        fb_id = _row.get('facebook_id')
        if pd.isna(fb_id) or fb_id == 0:
            continue
        page_id = str(int(fb_id))
        _params = {'page_id': page_id, 'active_status': 'all', 'ad_type': 'all', 'media_type': 'all'}
        _resp = requests.get(_url, headers=_headers, params=_params)
        try:
            _ads_json = _resp.json()
        except ValueError:
            _ads_json = None
        _records.append({'competitor_id': _row['id'], 'facebook_id': page_id, 'ads_response': _ads_json})
    ads_df = pd.DataFrame(_records)
    return (ads_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Parse out Facebook JSON response
    """)
    return


@app.cell
def _(ads_df, datetime, json, pd):
    def _parse_ads_from_df(ads_df: pd.DataFrame) -> pd.DataFrame:
        _records = []
        for _, _row in ads_df.iterrows():
            _competitor_id = _row['competitor_id']
            response_json = _row['ads_response']
            if not isinstance(response_json, dict):
                continue
            for ad in response_json.get('ads', []):  # Skip if it's not a dict (e.g. None or raw text)
                ad_archive_id = ad.get('ad_archive_id')
                raw_json = json.dumps(ad)
                posted_at = ad.get('start_date')
                if isinstance(posted_at, (int, float)):  # ads is already a list of ad‐dicts
                    posted_at = datetime.datetime.fromtimestamp(posted_at).strftime('%Y-%m-%d')
                _records.append({'competitor_id': _competitor_id, 'ad_id': ad_archive_id, 'json_response': raw_json, 'postedAt': posted_at, 'type': 'meta'})
        return pd.DataFrame(_records)
    facebook_ads = _parse_ads_from_df(ads_df)
    # Example usage
    facebook_ads.head()  # Convert start_date if it's a valid Unix timestamp
    return (facebook_ads,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Delete existing Facebook ads in supabase table
    """)
    return


@app.cell
def _():
    # # Table name
    # TABLE_NAME = "ad_library"

    # # Delete rows where type == "meta"
    # response = supabase.table(TABLE_NAME).delete().eq("type", "meta").execute()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Send to supabase
    """)
    return


@app.cell
def _(facebook_ads, supabase: "Client", time):
    def _chunk_list(data, chunk_size):
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
    _rows = facebook_ads.to_dict(orient='records')
    # Convert to list of dicts
    _batch_size = 500
    _total_upserted = 0
    for _batch in _chunk_list(_rows, _batch_size):
        _response = supabase.table('ad_library').upsert(_batch, on_conflict='ad_id').execute()
        if _response.data:
            print(f'✅ Inserted {len(_batch)} records...')  # specify on_conflict="ad_id"
            time.sleep(2)
        else:
            print(f'❌ Error inserting batch: {_response.error}')
            break
    print(f'🎉 Completed importing Facebook ads!')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Google Ads
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Get ads
    """)
    return


@app.cell
def _(competitors_1, os, pd, requests):
    _url = 'https://google-ads-library.p.rapidapi.com/advertiser_ads'
    _headers = {'x-rapidapi-key': os.environ["RAPIDAPI_KEY"], 'x-rapidapi-host': 'google-ads-library.p.rapidapi.com'}
    _records = []
    for _, _row in competitors_1.iterrows():
        google_id = _row.get('google_ads_id')
        if pd.isna(google_id) or str(google_id).lower() == 'none':
            continue
        advertiser_id = str(google_id)
        print(f'Searching for... {advertiser_id}')
        _params = {'advertiser_id': google_id, 'country_code': 'US', 'format': 'ALL', 'limit': '20'}
        _resp = requests.get(_url, headers=_headers, params=_params)
        try:
            _ads_json = _resp.json()
        except ValueError:
            _ads_json = None
        _records.append({'competitor_id': _row['id'], 'google_ads_id': google_id, 'response_json': _ads_json})
    google_ads_df = pd.DataFrame(_records)
    return (google_ads_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Parse out Google ad response
    """)
    return


@app.cell
def _(google_ads_df, json, pd):
    def _parse_ads_from_df(google_ads_df):
        _records = []
        for _, _row in google_ads_df.iterrows():
            _competitor_id = _row.get('competitor_id')
            rj = _row.get('response_json')
            if isinstance(rj, str):
                try:
                    rj = json.loads(rj)  # if rj is a string, try to load it
                except Exception:
                    continue
            if not isinstance(rj, dict):
                continue
            ads = rj.get('ads')
            if not isinstance(ads, list):  # skip if still not a dict
                continue
            for ad in ads:
                if not isinstance(ad, dict):
                    continue
                _records.append({'competitor_id': _competitor_id, 'ad_id': ad.get('creative_id'), 'json_response': json.dumps(ad, ensure_ascii=False), 'postedAt': ad.get('start'), 'type': 'google'})
        return pd.DataFrame(_records)
    # usage
    google_ads = _parse_ads_from_df(google_ads_df)
    return (google_ads,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Send to supabase
    """)
    return


@app.cell
def _(google_ads, supabase: "Client", time):
    def _chunk_list(data, chunk_size):
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
    _rows = google_ads.to_dict(orient='records')
    _batch_size = 500
    total_inserted = 0
    for _batch in _chunk_list(_rows, _batch_size):
        _response = supabase.table('ad_library').upsert(_batch, on_conflict='ad_id').execute()
        if _response.data:
            total_inserted = total_inserted + len(_batch)
            print(f'✅ Inserted/Updated {len(_batch)} records...')
            time.sleep(2)
        else:
            print(f'❌ Error inserting batch: {_response.error}')
            break
    print(f'🎉 Completed importing Google ads!')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Linkedin
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Get linkedin ads
    """)
    return


@app.cell
def _(competitors_1, os, pd, requests):
    URL = 'https://api.adyntel.com/linkedin'
    HEADERS = {'Content-Type': 'application/json'}
    API_KEY = os.environ["ADYNTEL_API_KEY"]
    EMAIL = os.environ["ADYNTEL_EMAIL"]
    _records = []
    for _, _row in competitors_1.iterrows():
        competitor_name = _row.get('competitor_name')
        _competitor_id = _row['id']
        _linkedin_id = _row['linkedin_id']
        if _linkedin_id in [0, '0'] or pd.isna(_linkedin_id) or str(_linkedin_id).strip().lower() in ['none', 'null', '']:
            print(f'⚠️ Skipping {competitor_name} (no LinkedIn ID)')
            continue
        print(f'\n🔍 Fetching ads for {competitor_name} (linkedin_id={_linkedin_id})...')
        payload = {'api_key': API_KEY, 'email': EMAIL, 'linkedin_page_id': str(_linkedin_id)}
        try:
            _resp = requests.post(URL, json=payload, headers=HEADERS, timeout=15)
            _resp.raise_for_status()
            try:
                _ads_json = _resp.json()
            except ValueError:
                _ads_json = None
            total_ads = _ads_json.get('total_ads') if isinstance(_ads_json, dict) else None
            print(f'   ✅ Retrieved {total_ads} ads for {competitor_name}')
            ads_list = _ads_json.get('ads') if isinstance(_ads_json, dict) else None
            if ads_list and len(ads_list) > 0:
                print('   👀 Ads found...')
            else:
                print('   ⚠️ No ads returned or unexpected structure.')
        except Exception as e:
            _ads_json = {'error': str(e)}
            print(f'   ❌ Error fetching ads for {competitor_name}: {e}')
        _records.append({'competitor_id': _competitor_id, 'linkedin_id': _linkedin_id, 'response_json': _ads_json})
    linkedin_ads_df = pd.DataFrame(_records)
    print('\n🎉 Finished fetching all competitor ads!')
    return (linkedin_ads_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Parse out response
    """)
    return


@app.cell
def _(dt, linkedin_ads_df, pd):
    expanded_rows = []
    posted_date = dt.datetime.utcnow().strftime('%Y-%m-%d')
    # Today's date in YYYY-MM-DD format
    for _, _row in linkedin_ads_df.iterrows():
        _competitor_id = _row['competitor_id']
        _linkedin_id = _row['linkedin_id']
        _resp = _row['response_json']
        if not _resp or 'ads' not in _resp:
            continue
        continuation = _resp.get('continuation_token')
        ad_date = None
        if continuation and '-' in continuation:
            try:
                timestamp_ms = int(continuation.split('-')[1])  # Extract date from continuation token if present
                ad_date = dt.datetime.utcfromtimestamp(timestamp_ms / 1000)
            except:
                ad_date = None
        for ad in _resp['ads']:
            expanded_rows.append({'competitor_id': _competitor_id, 'ad_id': ad.get('ad_id'), 'postedAt': posted_date, 'type': 'linkedin', 'json_response': ad})
    linkedin_ads = pd.DataFrame(expanded_rows)
    linkedin_ads.head()  # "linkedin_in": linkedin_id,
    return (linkedin_ads,)


@app.cell
def _(linkedin_ads):
    total_duplicates = linkedin_ads["ad_id"].duplicated().sum()
    print("Total duplicates:", total_duplicates)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Send to supabase
    """)
    return


@app.cell
def _(linkedin_ads, supabase: "Client", time):
    linkedin_ads_1 = linkedin_ads.drop_duplicates(subset=['ad_id'])

    def _chunk_list(data, chunk_size):
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
    _rows = linkedin_ads_1.to_dict(orient='records')
    _batch_size = 500
    _total_upserted = 0
    for _batch in _chunk_list(_rows, _batch_size):
        _response = supabase.table('ad_library').upsert(_batch, on_conflict='ad_id').execute()
        if _response.data:
            print(f'✅ Inserted records...')
            time.sleep(2)
        else:
            print(f'❌ Error inserting batch: {_response.error}')
            break
    print(f'🎉 Completed importing LinkedIn ads!')
    return


if __name__ == "__main__":
    app.run()