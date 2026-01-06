import asyncio
import os
import json
import logging
import argparse
from typing import List, Dict, Any
import urllib.parse
import aiohttp

import asyncpg
from dotenv import load_dotenv
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
import litellm

# B: DuckDuckGo Search Library
from duckduckgo_search import DDGS

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") 
LLM_PROVIDER = "gemini/gemini-2.0-flash-exp" 

CHROME_USER_DATA_DIR = os.getenv("CHROME_USER_DATA_DIR")
CHROME_PROFILE = os.getenv("CHROME_PROFILE")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_browser_config() -> BrowserConfig:
    if CHROME_USER_DATA_DIR:
        logger.info(f"Using Chrome Profile: {CHROME_PROFILE}")
        extra_args = ["--disable-blink-features=AutomationControlled"]
        if CHROME_PROFILE:
             extra_args.append(f"--profile-directory={CHROME_PROFILE}")
        return BrowserConfig(
            headless=False,
            user_data_dir=CHROME_USER_DATA_DIR,
            extra_args=extra_args,
            browser_type="chromium", 
            chrome_channel="chrome",
        )
    return BrowserConfig(headless=True)

async def init_db():
    logger.info("Initializing database...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("CREATE TABLE IF NOT EXISTS posts (id SERIAL PRIMARY KEY, url TEXT UNIQUE NOT NULL, title TEXT, content TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        await conn.execute("CREATE TABLE IF NOT EXISTS comments (id SERIAL PRIMARY KEY, post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE, content TEXT, author TEXT, sentiment_score TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        await conn.close()
        logger.info("Database initialized.")
    except Exception as e:
        logger.error(f"Failed to init DB: {e}")

async def save_data(data: Dict[str, Any], url: str):
    if not data:
        return
        
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        title = str(data.get("title") or "No Title")
        content_raw = data.get("content") or ""
        content = "\n\n".join([str(c) for c in content_raw]) if isinstance(content_raw, list) else str(content_raw)
        
        logger.info(f"Saving | Title: {title[:40]}...")
        
        post_id = await conn.fetchval("SELECT id FROM posts WHERE url = $1", url)
        
        if not post_id:
            post_id = await conn.fetchval("INSERT INTO posts (url, title, content) VALUES ($1, $2, $3) RETURNING id", url, title, content)
            logger.info(f"INSERTED Post ID: {post_id}")
        else:
            logger.info(f"Post exists (ID {post_id})")

        comments = data.get("comments", [])
        logger.info(f"Comments to save: {len(comments)}")
        
        if comments:
            for c in comments:
                c_content = c if isinstance(c, str) else c.get("content", "")
                author = c.get("author", "Unknown") if isinstance(c, dict) else "Unknown"
                if c_content:
                    await conn.execute("INSERT INTO comments (post_id, content, author) VALUES ($1, $2, $3)", post_id, str(c_content), str(author))
            logger.info(f"Saved {len(comments)} comments.")
    except Exception as e:
        logger.error(f"Error saving data: {e}")
    finally:
        await conn.close()

def is_discussion_url(url: str) -> bool:
    """Filter to keep only discussion pages."""
    discussion_patterns = ['/thread/', '/comments/', '/posts/', '/t/', '/topic/']
    profile_patterns = ['/u/', '/user/', '/members/', '/profile/']
    url_lower = url.lower()
    for p in profile_patterns:
        if p in url_lower:
            return False
    for p in discussion_patterns:
        if p in url_lower:
            return True
    # Accept Reddit subreddit pages with posts
    if 'reddit.com/r/' in url_lower and '/comments/' in url_lower:
        return True
    return False

# =============================================================================
# STRATEGY B: DuckDuckGo Search Library (Accurate API)
# =============================================================================
def search_ddg_api(keyword: str, num_results: int = 5) -> List[str]:
    """Use duckduckgo_search library for accurate results."""
    logger.info(f"[DDG API] Searching: {keyword}")
    links = []
    try:
        with DDGS() as ddgs:
            results = ddgs.text(keyword, max_results=num_results * 2)
            for r in results:
                url = r.get("href") or r.get("link")
                if url and is_discussion_url(url):
                    links.append(url)
                    if len(links) >= num_results:
                        break
        logger.info(f"[DDG API] Found {len(links)} discussion links")
    except Exception as e:
        logger.error(f"[DDG API] Error: {e}")
    return links

# =============================================================================
# STRATEGY C: Reddit JSON API (Direct Reddit Search)
# =============================================================================
async def search_reddit_api(keyword: str, subreddit: str = None, num_results: int = 5) -> List[str]:
    """Search Reddit directly using JSON API."""
    if subreddit:
        search_url = f"https://www.reddit.com/r/{subreddit}/search.json?q={urllib.parse.quote(keyword)}&restrict_sr=1&limit={num_results}&sort=relevance"
    else:
        search_url = f"https://www.reddit.com/search.json?q={urllib.parse.quote(keyword)}&limit={num_results}&sort=relevance"
    
    logger.info(f"[Reddit API] Searching: {search_url}")
    links = []
    
    try:
        async with aiohttp.ClientSession() as session:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; ProductAnalyst/1.0)"}
            async with session.get(search_url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    posts = data.get("data", {}).get("children", [])
                    for post in posts:
                        post_data = post.get("data", {})
                        permalink = post_data.get("permalink")
                        if permalink:
                            full_url = f"https://www.reddit.com{permalink}"
                            links.append(full_url)
                    logger.info(f"[Reddit API] Found {len(links)} posts")
                else:
                    logger.warning(f"[Reddit API] Status {resp.status}")
    except Exception as e:
        logger.error(f"[Reddit API] Error: {e}")
    
    return links

# =============================================================================
# STRATEGY D: Site-Specific Search (Search each site separately)
# =============================================================================
def search_site_specific(keyword: str, num_per_site: int = 3) -> List[str]:
    """Search each target site individually for better accuracy."""
    sites = ["reddit.com", "voz.vn", "tinhte.vn"]
    all_links = []
    
    for site in sites:
        query = f"{keyword} site:{site}"
        logger.info(f"[Site-Specific] Searching: {query}")
        try:
            with DDGS() as ddgs:
                results = ddgs.text(query, max_results=num_per_site * 2)
                count = 0
                for r in results:
                    url = r.get("href") or r.get("link")
                    if url and is_discussion_url(url):
                        all_links.append(url)
                        count += 1
                        if count >= num_per_site:
                            break
                logger.info(f"[Site-Specific] {site}: Found {count} links")
        except Exception as e:
            logger.error(f"[Site-Specific] {site} Error: {e}")
    
    return all_links

# =============================================================================
# COMBINED SEARCH: Merge all strategies
# =============================================================================
async def search_all_sources(keyword: str, num_results: int = 10) -> List[str]:
    """Combine all search strategies for maximum coverage."""
    all_links = set()
    
    # Strategy B: DDG API
    ddg_links = search_ddg_api(keyword, num_results=num_results)
    all_links.update(ddg_links)
    
    # Strategy C: Reddit API (general search)
    reddit_links = await search_reddit_api(keyword, num_results=num_results)
    all_links.update(reddit_links)
    
    # Strategy C: Reddit API (specific subreddit if applicable)
    # Extract potential subreddit from keyword
    keyword_lower = keyword.lower().replace(" ", "")
    possible_subreddits = ["blockblast", keyword_lower]
    for sub in possible_subreddits:
        sub_links = await search_reddit_api(keyword, subreddit=sub, num_results=3)
        all_links.update(sub_links)
    
    # Strategy D: Site-specific search
    site_links = search_site_specific(keyword, num_per_site=3)
    all_links.update(site_links)
    
    # Dedupe and filter
    final_links = list(all_links)
    logger.info(f"[COMBINED] Total unique links: {len(final_links)}")
    
    return final_links[:num_results]

async def crawl_discussion(url: str):
    """Crawl a URL and use LLM to extract structured data."""
    logger.info(f"Crawling: {url}")
    
    browser_cfg = get_browser_config()
    config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, magic=True)

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        result = await crawler.arun(url=url, config=config)
        
        if not result.success:
            logger.error(f"Crawl failed: {result.error_message}")
            return None
        
        markdown = result.markdown
        if not markdown or len(markdown) < 100:
            logger.warning(f"Page content too short: {len(markdown) if markdown else 0}")
            return None
            
        logger.info(f"Page markdown: {len(markdown)} chars")
        
        prompt = f"""
You are extracting data from a forum/discussion page.

PAGE CONTENT:
{markdown[:15000]}

OUTPUT (JSON only, no markdown):
{{
    "title": "Post title",
    "content": "Main post content",
    "comments": [
        {{"author": "user1", "content": "comment1"}},
        {{"author": "user2", "content": "comment2"}}
    ]
}}

Extract ALL visible comments. Return valid JSON only.
"""
        
        try:
            response = litellm.completion(
                model=LLM_PROVIDER,
                messages=[{"role": "user", "content": prompt}],
                api_key=GEMINI_API_KEY
            )
            
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()
            
            data = json.loads(raw)
            comments = data.get("comments", [])
            logger.info(f"EXTRACTED: title='{data.get('title', 'N/A')[:30]}', comments={len(comments)}")
            return data
            
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return None

async def analyze_sentiment_aggregated(keyword: str):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        count = await conn.fetchval("SELECT COUNT(*) FROM posts")
        comment_count = await conn.fetchval("SELECT COUNT(*) FROM comments")
        logger.info(f"DB Stats: {count} posts, {comment_count} comments")
        
        rows = await conn.fetch("SELECT p.title, c.content FROM posts p JOIN comments c ON p.id = c.post_id ORDER BY c.created_at DESC LIMIT 300")
        
        if not rows:
            logger.warning("No comments in DB.")
            return

        all_comments = "\n".join([f"- {r['content']}" for r in rows])
        
        prompt = f"""
Phân tích ý kiến cộng đồng về: "{keyword}".
Dữ liệu ({len(rows)} comments):
{all_comments[:20000]}

Output (Tiếng Việt):
1. Tóm tắt cảm xúc chung.
2. Top 3 điểm mạnh.
3. Top 3 điểm yếu.
4. Xu hướng nổi bật.
"""
        
        logger.info("Generating Report...")
        response = litellm.completion(
            model=LLM_PROVIDER,
            messages=[{"role": "user", "content": prompt}],
            api_key=GEMINI_API_KEY
        )
        print("\n" + "="*60)
        print(f"BÁO CÁO: {keyword.upper()}")
        print("="*60)
        print(response.choices[0].message.content)
        print("="*60 + "\n")

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
    finally:
        await conn.close()

async def main():
    parser = argparse.ArgumentParser(description="Product Analyst Tool")
    parser.add_argument("input", help="Keyword or URL")
    parser.add_argument("--mode", choices=["search", "url"], default="search")
    parser.add_argument("--clear-db", action="store_true", help="Clear DB before run")
    args = parser.parse_args()

    if not DATABASE_URL:
        logger.error("DATABASE_URL missing")
        return

    await init_db()
    
    # Optional: Clear DB for fresh run
    if args.clear_db:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("DELETE FROM comments")
        await conn.execute("DELETE FROM posts")
        await conn.close()
        logger.info("DB cleared.")

    urls = []
    if args.mode == "search" and not args.input.startswith("http"):
        urls = await search_all_sources(args.input)
    elif args.mode == "url" or args.input.startswith("http"):
        urls = [args.input]

    data_found = False
    for url in urls:
        data = await crawl_discussion(url)
        if data:
            data_found = True
            await save_data(data, url)

    if data_found:
        await analyze_sentiment_aggregated(args.input)
    else:
        logger.warning("No data crawled.")

if __name__ == "__main__":
    asyncio.run(main())
