import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

import asyncpg
from google_play_scraper import Sort, reviews

DATABASE_URL = os.getenv("DATABASE_URL")
APP_ID = os.getenv("GP_APP_ID")
APP_COUNTRY = os.getenv("GP_COUNTRY", "vn")
APP_LANG = os.getenv("GP_LANG", "vi")
APP_PAGES = int(os.getenv("GP_PAGES", "5"))
APP_PAGE_SIZE = int(os.getenv("GP_PAGE_SIZE", "100"))

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
    query = parse_qs(urlparse(url).query)
    app_id = query.get("id", [None])[0]
    return app_id


def normalize_review(app_id: str, review: Dict[str, Any]) -> Dict[str, Any] | None:
    raw_id = review.get("reviewId")
    if not raw_id:
        return None
    raw_date = review.get("at")
    review_date = None
    if isinstance(raw_date, datetime):
        if raw_date.tzinfo:
            review_date = raw_date.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            review_date = raw_date
    return {
        "app_id": app_id,
        "review_id": f"gp:{raw_id}",
        "title": review.get("title"),
        "content": review.get("content"),
        "rating": review.get("score"),
        "review_date": review_date,
        "raw_json": review,
    }


async def save_reviews(reviews_data: List[Dict[str, Any]]) -> int:
    if not reviews_data:
        return 0
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        count = 0
        for r in reviews_data:
            if not r or not r.get("review_id"):
                continue
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
                json.dumps(r["raw_json"], default=str),
            )
            count += 1
        return count
    finally:
        await conn.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Google Play reviews into PostgreSQL.")
    parser.add_argument("--app-id", dest="app_id", help="Google Play package id, e.g. com.whatsapp")
    parser.add_argument("--app-url", dest="app_url", help="Google Play app URL.")
    parser.add_argument("--country", dest="country", default=None, help="Store country code, e.g. vn.")
    parser.add_argument("--lang", dest="lang", default=None, help="Language code, e.g. vi.")
    parser.add_argument("--pages", dest="pages", type=int, default=None, help="Number of pages to fetch.")
    parser.add_argument("--page-size", dest="page_size", type=int, default=None, help="Reviews per page.")
    args = parser.parse_args()

    app_id = args.app_id or parse_app_id_from_url(args.app_url) or APP_ID
    if not app_id:
        raise RuntimeError("GP_APP_ID is required (or provide --app-id / --app-url).")
    country = args.country or APP_COUNTRY
    lang = args.lang or APP_LANG
    pages = args.pages or APP_PAGES
    page_size = args.page_size or APP_PAGE_SIZE

    await init_db()

    all_reviews: List[Dict[str, Any]] = []
    token = None
    for _ in range(pages):
        batch, token = reviews(
            app_id,
            lang=lang,
            country=country,
            sort=Sort.NEWEST,
            count=page_size,
            continuation_token=token,
        )
        if not batch:
            break
        all_reviews.extend([normalize_review(app_id, r) for r in batch])
        if not token:
            break

    saved = await save_reviews(all_reviews)
    print(f"Saved {saved} reviews")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
