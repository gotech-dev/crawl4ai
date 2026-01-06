from duckduckgo_search import DDGS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_search(keyword):
    # Test 3: Simpler Query matches
    queries = [
        f'{keyword} reddit',
        f'{keyword} site:facebook.com',
        f'{keyword} review'
    ]
    
    for q in queries:
        logger.info(f"Testing Query: {q}")
        try:
            results = list(DDGS().text(q, max_results=3))
            logger.info(f"Results: {len(results)} found")
            for r in results:
                print(f" - {r['href']}")
        except Exception as e:
            logger.error(f"Error: {e}")

if __name__ == "__main__":
    test_search("Block Blast game")
