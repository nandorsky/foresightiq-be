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
    ## Company Filter
    """)
    return


@app.cell
def _():
    # company_name = "Nextiva"
    # company_name
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

    import pandas as pd
    import praw
    import requests
    import time
    from datetime import datetime, timezone
    from supabase import create_client, Client
    import ast
    import json
    import anthropic

    SUPABASE_URL = os.environ["SUPABASE_URL"]
    SUPABASE_KEY = os.environ["SUPABASE_KEY"]
    SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

    supabase: Client = create_client(SUPABASE_URL, SERVICE_ROLE_KEY)

    # Anthropic
    anthropic_api_key = os.environ["ANTHROPIC_API_KEY"]
    client = anthropic.Anthropic(
        api_key=anthropic_api_key,
    )

    praw_client = praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ["REDDIT_USER_AGENT"]
    )
    return ast, pd, praw_client, supabase


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Fetch data
    """)
    return


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

    return (fetch_data,)


@app.cell
def _(fetch_data):
    # ✅ Fetch data from tables
    companies = fetch_data(
        "companies",
         filters={"status": ["trial", "active"]},
    )
    competitors = fetch_data(
        "competitors",
    )
    return companies, competitors


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Filter competitors by account status
    """)
    return


@app.cell
def _(companies, competitors):
    # Filter competitors by account status
    filtered_competitors = competitors[competitors['company_id'].isin(companies['id'])]
    filtered_competitors = filtered_competitors.reset_index(drop=True)
    # Optional: reset index if you want a clean one
    competitors_1 = filtered_competitors
    print(f'✅ Filtered to {len(companies)} trial accounts with {len(competitors_1)} competitors')
    return (competitors_1,)


@app.cell
def _(companies):
    # ✅ Keep only rows where 'reddit' is not NaN
    companies_1 = companies[~companies['reddit'].isna()]
    companies_1
    return (companies_1,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Add competitors to keyword query
    """)
    return


@app.cell
def _(companies_1, competitors_1):
    comp_lists = competitors_1.groupby('company_id', as_index=False).agg(competitors=('competitor_name', lambda x: [c for c in x.dropna()]))
    companies_with_comps = companies_1.merge(comp_lists, left_on='id', right_on='company_id', how='left').drop(columns=['company_id'])
    companies_with_comps['competitors'] = companies_with_comps['competitors'].apply(lambda v: v if isinstance(v, list) else [])
    companies_2 = companies_with_comps
    return (companies_2,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Split out keywords and reddit into own columns
    """)
    return


@app.cell
def _(ast, companies_2, pd):
    def safe_parse(value):
        if isinstance(value, str):
            try:
                return ast.literal_eval(value)
            except Exception:
                return {}
        return value

    def extract_keywords_and_subs(row):
        reddit_data = safe_parse(row.get('reddit', {}))
        competitors_data = safe_parse(row.get('competitors', []))
        reddit_keywords = reddit_data.get('keywords', []) if isinstance(reddit_data, dict) else []
        subreddits = reddit_data.get('subreddits', []) if isinstance(reddit_data, dict) else []  # Extract reddit keywords & subreddits
        if not isinstance(reddit_keywords, list):
            reddit_keywords = []
        if not isinstance(subreddits, list):
            subreddits = []  # Ensure both are lists
        if not isinstance(competitors_data, list):
            competitors_data = []
        all_keywords = list({kw.lower().strip() for kw in reddit_keywords + competitors_data if kw})
        return pd.Series({'keywords': all_keywords, 'subreddits': subreddits})
    companies_2[['keywords', 'subreddits']] = companies_2.apply(extract_keywords_and_subs, axis=1)
    # Apply function
    companies_2  # Handle competitors  # Combine competitors + reddit keywords
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Grab posts & comments from subbreddits
    """)
    return


@app.cell
def _(companies_2, pd, praw_client):
    import prawcore
    posts_by_id: dict[str, dict] = {}
    comments: list[dict] = []
    seen_comment_ids: set[str] = set()
    MAX_POSTS_PER_QUERY = 20
    MAX_COMMENTS_PER_POST = None

    def _to_list(value) -> list[str]:
        """Normalize a cell value into a list of unique, lowercase strings."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return []
        if isinstance(value, (list, tuple, pd.Series)):
            items = [str(x) for x in value if pd.notna(x)]
        else:
            items = [s for s in str(value).split(',')]
        out, seen = ([], set())
        for s in items:
            norm = ' '.join(s.strip().split()).lower()
            if norm and norm not in seen:
                out.append(norm)
                seen.add(norm)
        return out
    for _, row in companies_2.iterrows():
        company_id = row['id']
        subs = _to_list(row.get('subreddits'))
        keywords = _to_list(row.get('keywords'))
        if not subs or not keywords:
            continue
        for sub in subs:
            try:
                sr = praw_client.subreddit(sub)
            except Exception as e:
                print(f'⚠️ Unable to init subreddit r/{sub}: {e}')  # Nothing to search for this company
                continue
            for kw in keywords:
                try:
                    submissions = list(sr.search(kw, sort='new', limit=MAX_POSTS_PER_QUERY))
                except prawcore.Forbidden:
                    print(f'⚠️ Skipping r/{sub} (search forbidden)')
                    continue
                except Exception as e:
                    print(f"⚠️ Error searching r/{sub} kw='{kw}': {e}")
                    continue
                if not submissions:
                    continue
                for submission in submissions:
                    sid = getattr(submission, 'id', None)
                    title = getattr(submission, 'title', '(no title)')
                    author = getattr(submission, 'author', None)
                    print(f"📄 Found post in r/{sub} for '{kw}': {title[:100]} by {author}")
                    if not sid:
                        continue
                    if sid not in posts_by_id:
                        posts_by_id[sid] = {'company_id': company_id, 'subreddit': sub, 'post_id': sid, 'post_author': str(submission.author) if getattr(submission, 'author', None) else None, 'post_title': getattr(submission, 'title', None), 'post_url': f'https://www.reddit.com{submission.permalink}' if getattr(submission, 'permalink', None) else None, 'post_created_utc': getattr(submission, 'created_utc', None), 'post_selftext': getattr(submission, 'selftext', None), '_matched_keywords_set': {kw}}  # print(f"ℹ️ No results for r/{sub} kw='{kw}'")
                    else:
                        posts_by_id[sid].setdefault('_matched_keywords_set', set()).add(kw)
                    try:
                        submission.comments.replace_more(limit=0)
                        flat = submission.comments.list()
                        if MAX_COMMENTS_PER_POST is not None:
                            flat = flat[:MAX_COMMENTS_PER_POST]
                        for c in flat:
                            cid = getattr(c, 'id', None)
                            if not cid or cid in seen_comment_ids:
                                continue
                            seen_comment_ids.add(cid)
                            comments.append({'post_id': sid, 'comment_id': cid, 'comment_body': getattr(c, 'body', None), 'comment_author': str(c.author) if getattr(c, 'author', None) else None, 'comment_created_utc': getattr(c, 'created_utc', None), 'parent_id': getattr(c, 'parent_id', None), 'permalink': f'https://www.reddit.com{c.permalink}' if getattr(c, 'permalink', None) else None, 'search_keyword': kw})
                    except Exception:
                        continue
    for v in posts_by_id.values():
        mk = v.pop('_matched_keywords_set', None)
        v['matched_keywords'] = sorted(list(mk)) if mk else []
    posts_payload = list(posts_by_id.values())
    comments_payload = comments
    print(f'✅ Reddit scan complete. Posts: {len(posts_payload)}, Comments: {len(comments_payload)}')  # Fetch comments  # swallow any per-post comment scrape errors
    return comments_payload, posts_payload


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Send to supabase
    """)
    return


@app.cell
def _(comments_payload, pd, posts_payload, supabase: "Client"):
    def to_iso(dt):
        if dt is None:
            return None
        try:
            if isinstance(dt, (int, float)):
                ts = pd.to_datetime(dt, unit='s', utc=True, errors='coerce')
            else:
                ts = pd.to_datetime(dt, utc=True, errors='coerce')
            return ts.isoformat().replace('+00:00', 'Z') if pd.notnull(ts) else None
        except Exception:
            return None

    def normalize_timestamps(records, keys):
        for r in records:
            for k in keys:
                if k in r and r[k] is not None:
                    r[k] = to_iso(r[k])
        return records
    posts_payload_1 = normalize_timestamps(posts_payload, ['post_created_utc'])
    comments_payload_1 = normalize_timestamps(comments_payload, ['comment_created_utc'])

    def chunk_list(data, chunk_size):
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
    BATCH_SIZE = 500
    POST_TABLE = 'reddit_posts'
    COMMENT_TABLE = 'reddit_comments'
    inserted_posts = 0
    for batch in chunk_list(posts_payload_1, BATCH_SIZE):
        resp = supabase.table(POST_TABLE).upsert(batch, on_conflict='post_id').execute()
        if resp.data is not None:
            inserted_posts = inserted_posts + len(batch)
            print(f'✅ Upserted {len(batch)} posts')
        else:
            raise RuntimeError(f'❌ Post upsert failed: {resp.error}')
    print(f'🎉 Total posts sent: {inserted_posts}')
    inserted_comments = 0
    for batch in chunk_list(comments_payload_1, BATCH_SIZE):
        resp = supabase.table(COMMENT_TABLE).upsert(batch, on_conflict='comment_id').execute()
        if resp.data is not None:
            inserted_comments = inserted_comments + len(batch)
            print(f'✅ Upserted {len(batch)} comments')
        else:
            raise RuntimeError(f'❌ Comment upsert failed: {resp.error}')
    print(f'🎉 Total comments sent: {inserted_comments}')
    return


if __name__ == "__main__":
    app.run()