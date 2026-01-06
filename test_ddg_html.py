from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
import asyncio
import urllib.parse
import json

async def test_ddg_lite():
    keyword = "Block Blast game"
    # DDG HTML version (POST or GET)
    # GET works for html version: https://duckduckgo.com/html/?q=...
    encoded_query = urllib.parse.quote(f'{keyword} site:reddit.com')
    search_url = f"https://duckduckgo.com/html/?q={encoded_query}"
    
    print(f"Testing URL: {search_url}")

    # Strategy for DDG HTML
    # Structure: .result__a (link)
    extraction_strategy = JsonCssExtractionStrategy({
        "baseSelector": ".result", 
        "fields": [
            {
                "name": "title", 
                "selector": ".result__a", 
                "type": "text", 
            },
            {
                "name": "link", 
                "selector": ".result__a", 
                "type": "attribute", 
                "attribute": "href"
            }
        ]
    })

    config = CrawlerRunConfig(
        extraction_strategy=extraction_strategy,
        cache_mode=CacheMode.BYPASS,
        verbose=True
    )

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=search_url, config=config)
        
        if result.success:
            print("Crawl Success!")
            try:
                data = json.loads(result.extracted_content)
                print(f"Items found: {len(data)}")
                for item in data[:3]:
                    print(item)
            except Exception as e:
                print(f"JSON Parse Error: {e}")
                print(result.extracted_content[:500])
        else:
            print(f"Crawl Failed: {result.error_message}")

if __name__ == "__main__":
    asyncio.run(test_ddg_lite())
