import asyncio
import argparse
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL")
APP_ID = os.getenv("APP_ID")
APP_COUNTRY = os.getenv("APP_COUNTRY", "vn")
APP_PAGES = int(os.getenv("APP_PAGES", "5"))

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS app_reviews (
    id SERIAL PRIMARY KEY,
    app_id TEXT NOT NULL,
    review_id TEXT UNIQUE NOT NULL,
    title TEXT,
    content TEXT,
    rating INT,
    review_date TIMESTAMP,
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


async def init_db() -> None:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute(CREATE_SQL)
    await conn.close()


def parse_app_id_from_url(url: str) -> str | None:
    if not url:
        return None
    match = re.search(r"/id(\d+)", url)
    return match.group(1) if match else None


async def fetch_appstore_reviews(app_id: str, country: str, page: int) -> List[Dict[str, Any]]:
    url = (
        "https://itunes.apple.com/rss/customerreviews/"
        f"page={page}/id={app_id}/sortby=mostrecent/json?cc={country}"
    )
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                return []
            # Apple sometimes returns JSON with text/javascript content-type.
            data = await resp.json(content_type=None)

    entries = data.get("feed", {}).get("entry", [])
    if not entries or len(entries) <= 1:
        return []
    return entries[1:]


def normalize_review(app_id: str, review: Dict[str, Any]) -> Dict[str, Any]:
    rating_value = review.get("im:rating", {}).get("label")
    raw_date = review.get("updated", {}).get("label")
    review_date = None
    if raw_date:
        try:
            parsed = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            # Store naive UTC to match TIMESTAMP (without timezone) column.
            review_date = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        except ValueError:
            review_date = None
    return {
        "app_id": app_id,
        "review_id": review.get("id", {}).get("label"),
        "title": review.get("title", {}).get("label"),
        "content": review.get("content", {}).get("label"),
        "rating": int(rating_value) if rating_value else None,
        "review_date": review_date,
        "raw_json": review,
    }


async def save_reviews(reviews: List[Dict[str, Any]]) -> int:
    if not reviews:
        return 0
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        count = 0
        for r in reviews:
            await conn.execute(
                """
                INSERT INTO app_reviews (app_id, review_id, title, content, rating, review_date, raw_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (review_id) DO NOTHING
                """,
                r["app_id"],
                r["review_id"],
                r["title"],
                r["content"],
                r["rating"],
                r["review_date"],
                json.dumps(r["raw_json"]),
            )
            count += 1
        return count
    finally:
        await conn.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch App Store reviews into PostgreSQL.")
    parser.add_argument("--app-id", dest="app_id", help="Numeric App Store app id.")
    parser.add_argument("--app-url", dest="app_url", help="App Store app URL.")
    parser.add_argument("--country", dest="country", default=None, help="Store country code, e.g. vn.")
    parser.add_argument("--pages", dest="pages", type=int, default=None, help="Number of pages to fetch.")
    args = parser.parse_args()

    app_id = args.app_id or parse_app_id_from_url(args.app_url) or APP_ID
    if not app_id:
        raise RuntimeError("APP_ID is required (or provide --app-id / --app-url).")
    country = args.country or APP_COUNTRY
    pages = args.pages or APP_PAGES

    await init_db()

    all_reviews: List[Dict[str, Any]] = []
    for page in range(1, pages + 1):
        batch = await fetch_appstore_reviews(app_id, country, page)
        if not batch:
            break
        all_reviews.extend([normalize_review(app_id, r) for r in batch])

    saved = await save_reviews(all_reviews)
    print(f"Saved {saved} reviews")


if __name__ == "__main__":
    asyncio.run(main())
