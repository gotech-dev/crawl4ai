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
from duckduckgo_search import DDGS

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") 
LLM_PROVIDER = "gemini/gemini-2.0-flash-exp" 
CHROME_USER_DATA_DIR = os.getenv("CHROME_USER_DATA_DIR")
CHROME_PROFILE = os.getenv("CHROME_PROFILE")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_browser_config() -> BrowserConfig:
    if CHROME_USER_DATA_DIR:
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
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("CREATE TABLE IF NOT EXISTS posts (id SERIAL PRIMARY KEY, url TEXT UNIQUE NOT NULL, title TEXT, content TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    await conn.execute("CREATE TABLE IF NOT EXISTS comments (id SERIAL PRIMARY KEY, post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE, content TEXT, author TEXT, sentiment_score TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    await conn.close()
    logger.info("Database initialized.")

async def save_data(data: Dict[str, Any], url: str):
    if not data:
        return
        
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        title = str(data.get("title") or "No Title")
        content = str(data.get("content") or "")
        
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
                c_content = c.get("content", "") if isinstance(c, dict) else str(c)
                author = c.get("author", "Unknown") if isinstance(c, dict) else "Unknown"
                if c_content and len(c_content) > 10:  # Skip very short comments
                    await conn.execute("INSERT INTO comments (post_id, content, author) VALUES ($1, $2, $3)", post_id, str(c_content)[:2000], str(author))
            logger.info(f"Saved {len(comments)} comments.")
    except Exception as e:
        logger.error(f"Error saving: {e}")
    finally:
        await conn.close()

def is_discussion_url(url: str) -> bool:
    discussion_patterns = ['/thread/', '/comments/', '/posts/', '/t/', '/topic/']
    profile_patterns = ['/u/', '/user/', '/members/', '/profile/']
    url_lower = url.lower()
    for p in profile_patterns:
        if p in url_lower:
            return False
    for p in discussion_patterns:
        if p in url_lower:
            return True
    if 'reddit.com/r/' in url_lower and '/comments/' in url_lower:
        return True
    return False

# =============================================================================
# REDDIT JSON API - Get comments directly from Reddit's structured data
# =============================================================================
async def get_reddit_post_data(url: str) -> Dict[str, Any]:
    """Fetch Reddit post and comments using Reddit's JSON API."""
    # Convert post URL to JSON API URL
    json_url = url.rstrip('/') + '.json'
    logger.info(f"[Reddit JSON] Fetching: {json_url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; ProductAnalyst/1.0)"}
            async with session.get(json_url, headers=headers) as resp:
                if resp.status != 200:
                    logger.warning(f"[Reddit JSON] Status {resp.status}")
                    return None
                
                data = await resp.json()
                
                # Reddit returns [post_data, comments_data]
                if not isinstance(data, list) or len(data) < 2:
                    return None
                
                # Extract post info
                post_data = data[0].get("data", {}).get("children", [{}])[0].get("data", {})
                title = post_data.get("title", "No Title")
                content = post_data.get("selftext", "")
                
                # Extract comments
                comments = []
                comments_data = data[1].get("data", {}).get("children", [])
                
                def extract_comments(children, depth=0):
                    for child in children:
                        if child.get("kind") != "t1":  # t1 = comment
                            continue
                        c_data = child.get("data", {})
                        author = c_data.get("author", "Unknown")
                        body = c_data.get("body", "")
                        if body and author != "[deleted]" and len(body) > 10:
                            comments.append({"author": author, "content": body})
                        
                        # Get nested replies
                        replies = c_data.get("replies")
                        if isinstance(replies, dict):
                            reply_children = replies.get("data", {}).get("children", [])
                            extract_comments(reply_children, depth + 1)
                
                extract_comments(comments_data)
                
                logger.info(f"[Reddit JSON] Title: {title[:30]}, Comments: {len(comments)}")
                return {
                    "title": title,
                    "content": content,
                    "comments": comments[:50]  # Limit to 50 comments
                }
                
    except Exception as e:
        logger.error(f"[Reddit JSON] Error: {e}")
        return None

# =============================================================================
# SEARCH FUNCTIONS
# =============================================================================
async def search_reddit_api(keyword: str, subreddit: str = None, num_results: int = 5) -> List[str]:
    """Search Reddit using JSON API."""
    if subreddit:
        url = f"https://www.reddit.com/r/{subreddit}/search.json?q={urllib.parse.quote(keyword)}&restrict_sr=1&limit={num_results}&sort=relevance"
    else:
        url = f"https://www.reddit.com/search.json?q={urllib.parse.quote(keyword)}&limit={num_results}&sort=relevance"
    
    logger.info(f"[Reddit Search] {url}")
    links = []
    
    try:
        async with aiohttp.ClientSession() as session:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; ProductAnalyst/1.0)"}
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    posts = data.get("data", {}).get("children", [])
                    for post in posts:
                        permalink = post.get("data", {}).get("permalink")
                        if permalink:
                            links.append(f"https://www.reddit.com{permalink}")
                    logger.info(f"[Reddit Search] Found {len(links)} posts")
    except Exception as e:
        logger.error(f"[Reddit Search] Error: {e}")
    
    return links

def search_ddg_api(keyword: str, num_results: int = 5) -> List[str]:
    """Search using DuckDuckGo library."""
    logger.info(f"[DDG] Searching: {keyword}")
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
        logger.info(f"[DDG] Found {len(links)} discussion links")
    except Exception as e:
        logger.error(f"[DDG] Error: {e}")
    return links

async def search_all_sources(keyword: str, num_results: int = 10) -> List[str]:
    """Combine search strategies - focus on Reddit for reliable comment extraction."""
    all_links = set()
    
    # Reddit API search (most reliable for comments)
    reddit_links = await search_reddit_api(keyword, num_results=num_results)
    all_links.update(reddit_links)
    
    # Search specific subreddits based on keyword
    keyword_clean = keyword.lower().replace(" ", "").replace("game", "")
    subreddits = [keyword_clean, f"{keyword_clean}app", f"{keyword_clean}game"]
    for sub in subreddits[:2]:
        sub_links = await search_reddit_api(keyword, subreddit=sub, num_results=3)
        all_links.update(sub_links)
    
    # DDG for non-Reddit sources
    ddg_links = search_ddg_api(keyword, num_results=5)
    all_links.update(ddg_links)
    
    logger.info(f"[COMBINED] Total unique links: {len(all_links)}")
    return list(all_links)[:num_results]

# =============================================================================
# CRAWL FUNCTION - Use Reddit JSON for Reddit, LLM for others
# =============================================================================
async def crawl_discussion(url: str):
    """Smart crawl: Use Reddit JSON API for Reddit URLs, LLM for others."""
    logger.info(f"Crawling: {url}")
    
    # For Reddit: Use JSON API directly (reliable comment extraction)
    if "reddit.com" in url and "/comments/" in url:
        return await get_reddit_post_data(url)
    
    # For other sites: Use browser + LLM
    browser_cfg = get_browser_config()
    config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, magic=True)

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        result = await crawler.arun(url=url, config=config)
        
        if not result.success or not result.markdown or len(result.markdown) < 100:
            return None
            
        prompt = f"""
Extract from this forum page:
{result.markdown[:15000]}

Return JSON only:
{{"title": "...", "content": "...", "comments": [{{"author": "...", "content": "..."}}]}}
"""
        
        try:
            response = litellm.completion(
                model=LLM_PROVIDER,
                messages=[{"role": "user", "content": prompt}],
                api_key=GEMINI_API_KEY
            )
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1].replace("json", "", 1).strip()
            return json.loads(raw)
        except:
            return None

async def analyze_sentiment(keyword: str):
    """Generate analysis report from collected comments."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        stats = await conn.fetchrow("SELECT COUNT(*) as posts FROM posts")
        comment_stats = await conn.fetchrow("SELECT COUNT(*) as comments FROM comments")
        logger.info(f"DB: {stats['posts']} posts, {comment_stats['comments']} comments")
        
        rows = await conn.fetch("""
            SELECT p.title, c.content, c.author 
            FROM posts p 
            JOIN comments c ON p.id = c.post_id 
            ORDER BY c.created_at DESC LIMIT 200
        """)
        
        if not rows:
            logger.warning("No comments to analyze.")
            return

        comments_text = "\n".join([f"- {r['author']}: {r['content'][:300]}" for r in rows])
        
        prompt = f"""
Phân tích ý kiến cộng đồng về "{keyword}" dựa trên {len(rows)} bình luận:

{comments_text[:15000]}

Trả lời TIẾNG VIỆT:
1. Tóm tắt cảm xúc chung (tích cực/tiêu cực/trung lập)
2. Top 3 điểm mạnh được khen
3. Top 3 điểm yếu bị chê  
4. Xu hướng nổi bật
"""
        
        logger.info("Generating report...")
        response = litellm.completion(
            model=LLM_PROVIDER,
            messages=[{"role": "user", "content": prompt}],
            api_key=GEMINI_API_KEY
        )
        
        print("\n" + "="*60)
        print(f"📊 BÁO CÁO PHÂN TÍCH: {keyword.upper()}")
        print("="*60)
        print(response.choices[0].message.content)
        print("="*60 + "\n")

    finally:
        await conn.close()

async def main():
    parser = argparse.ArgumentParser(description="Product Analyst")
    parser.add_argument("keyword", help="Keyword to analyze")
    parser.add_argument("--mode", choices=["search", "url"], default="search")
    parser.add_argument("--clear-db", action="store_true")
    args = parser.parse_args()

    if not DATABASE_URL:
        logger.error("DATABASE_URL missing")
        return

    await init_db()
    
    if args.clear_db:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("DELETE FROM comments")
        await conn.execute("DELETE FROM posts")
        await conn.close()
        logger.info("DB cleared.")

    urls = []
    if args.mode == "search":
        urls = await search_all_sources(args.keyword)
    else:
        urls = [args.keyword]

    for url in urls:
        data = await crawl_discussion(url)
        if data:
            await save_data(data, url)

    await analyze_sentiment(args.keyword)

if __name__ == "__main__":
    asyncio.run(main())
