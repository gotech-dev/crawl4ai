import asyncio
import json
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

async def crawl_simple(url):
    """
    Cách 1: Lấy toàn bộ nội dung dưới dạng Markdown (Tốt cho LLM/AI)
    """
    print(f"\n--- Đang crawl (Markdown): {url} ---")
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=url)
        
        if result.success:
            print("Tiêu đề:", result.metadata.get("title", "No Title"))
            print("Độ dài Markdown:", len(result.markdown))
            print("Nội dung (500 ký tự đầu):\n", result.markdown[:500])
        else:
            print("Lỗi:", result.error_message)

async def crawl_structured(url):
    """
    Cách 2: Lấy dữ liệu cụ thể (Ví dụ tiêu đề và giá) bằng CSS Selectors
    Tương tự như cách ta dùng document.querySelector trong JS
    """
    print(f"\n--- Đang crawl (Structured Data): {url} ---")
    
    # Định nghĩa cấu trúc dữ liệu muốn lấy
    # Ví dụ này lấy các thẻ H2 và các đường link (bạn có thể thay đổi tùy trang web)
    schema = {
        "name": "Bai Viet",
        "baseSelector": "body", # Vùng bao quanh cần lấy
        "fields": [
            {
                "name": "cac_tieu_de_h2",
                "selector": "h2", 
                "type": "text", 
            },
            {
                "name": "cac_duong_link",
                "selector": "a",
                "type": "attribute",
                "attribute": "href"
            }
        ]
    }

    extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True)
    
    # Cấu hình chạy: Bỏ qua cache để luôn lấy mới
    config = CrawlerRunConfig(
        extraction_strategy=extraction_strategy,
        cache_mode=CacheMode.BYPASS
    )

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=url, config=config)
        
        if result.success:
            # Dữ liệu trả về là chuỗi JSON, ta parse ra để xem
            data = json.loads(result.extracted_content)
            print("Dữ liệu trích xuất được (JSON):")
            print(json.dumps(data, indent=2, ensure_ascii=False)[:1000]) # In 1000 ký tự đầu
        else:
            print("Lỗi:", result.error_message)

async def main():
    # LINK CẦN CRAWL - BẠN THAY ĐỔI Ở ĐÂY
    target_url = "https://vnexpress.net" 
    
    # 1. Chạy thử crawl lấy markdown
    await crawl_simple(target_url)
    
    # 2. Chạy thử crawl lấy dữ liệu có cấu trúc (bỏ comment để chạy)
    # await crawl_structured(target_url)

if __name__ == "__main__":
    asyncio.run(main())
