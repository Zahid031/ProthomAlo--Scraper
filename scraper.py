import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import logging
import json

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ----------------------------------
# Elasticsearch Handler
# ----------------------------------
class ElasticsearchManager:
    def __init__(self, host, port, username='', password='', use_ssl=False, verify_certs=False):
        scheme = 'https' if use_ssl else 'http'
        auth_part = f"{username}:{password}@" if username and password else ""
        url = f"{scheme}://{auth_part}{host}:{port}"

        self.es = Elasticsearch(url, verify_certs=verify_certs)

        if not self.es.ping():
            raise ConnectionError("Elasticsearch connection failed.")
        logger.info("Successfully connected to Elasticsearch")

        if not self.es.indices.exists(index="prothomalo_articles"):
            self.es.indices.create(index="prothomalo_articles")
            logger.info("Created index: prothomalo_articles")
    

    def test_search():
        ES_CONFIG = {
            "host": "localhost",
            "port": 9200,
            "username": "elastic",  # Put your username if needed
            "password": "JvQhvZYl",  # Put your password if needed
            "use_ssl": False,
            "verify_certs": False,
        }
        es = ElasticsearchManager(**ES_CONFIG)
        results = es.search_articles("নির্বাচন")
        for i, r in enumerate(results, 1):
            print(f"{i}. {r['title']} - {r['url']}")



    def bulk_insert_articles(self, articles):
        actions = [
            {
                "_index": "prothomalo_articles",
                "_id": article["url"],
                "_source": article
            }
            for article in articles
        ]
        success, _ = helpers.bulk(self.es, actions, raise_on_error=False)
        return success

    def search_articles(self, keyword, limit=5):
        res = self.es.search(
            index="prothomalo_articles",
            body={
                "query": {
                    "multi_match": {
                        "query": keyword,
                        "fields": ["title", "content"]
                    }
                },
                "size": limit
            }
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]

    def get_article_stats(self):
        res = self.es.count(index="prothomalo_articles")
        return {"total": res["count"]}


# ----------------------------------
# Scraper Class
# ----------------------------------
class ProthomAloScraper:
    BASE_URL = "https://www.prothomalo.com/politics"

    def __init__(self):
        self.session = requests.Session()

    def get_article_urls(self, max_pages=3):
        urls = set()
        for page in range(1, max_pages + 1):
            page_url = f"{self.BASE_URL}?page={page}"
            logger.info(f"Scraping page {page}: {page_url}")
            try:
                response = self.session.get(page_url, timeout=10)
                if response.status_code != 200:
                    logger.warning(f"Failed to fetch page {page}: {response.status_code}")
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("/politics/") and len(href.split("/")) > 2:
                        full_url = "https://www.prothomalo.com" + href
                        urls.add(full_url)

            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")

        logger.info(f"Total unique articles found: {len(urls)}")
        return list(urls)

    def scrape_article(self, url):
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Failed to fetch article: {url}")
                return None

            soup = BeautifulSoup(response.text, "html.parser")
            title = soup.find("h1")
            body = soup.select("div.article-content p")

            return {
                "url": url,
                "title": title.get_text(strip=True) if title else "",
                "content": "\n".join(p.get_text(strip=True) for p in body),
                "scraped_at": datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error scraping article {url}: {e}")
            return None

    def scrape_multiple_articles(self, urls, max_articles=20):
        articles = []
        for i, url in enumerate(urls):
            if i >= max_articles:
                break
            article = self.scrape_article(url)
            if article and article["title"] and article["content"]:
                articles.append(article)
        logger.info(f"Scraped {len(articles)} articles")
        return articles


# ----------------------------------
# Main Function
# ----------------------------------
def main():
    ES_CONFIG = {
        "host": "localhost",
        "port": 9200,
        "username": "elastic",  # Set if using secured ES
        "password": "JvQhvZYl",
        "use_ssl": False,
        "verify_certs": False
    }

    try:
        logger.info("Initializing Elasticsearch connection...")
        es_manager = ElasticsearchManager(**ES_CONFIG)

        logger.info("Initializing scraper...")
        scraper = ProthomAloScraper()

        urls = scraper.get_article_urls(max_pages=3)
        if not urls:
            logger.error("No article URLs found.")
            return

        articles = scraper.scrape_multiple_articles(urls, max_articles=20)
        if not articles:
            logger.error("No articles scraped.")
            return

        success_count = es_manager.bulk_insert_articles(articles)
        logger.info(f"Successfully inserted {success_count} articles into Elasticsearch.")

        stats = es_manager.get_article_stats()
        logger.info("Article stats:\n" + json.dumps(stats, indent=2, ensure_ascii=False))

    except Exception as e:
        logger.error(f"Main execution error: {e}")


# ----------------------------------
# Optional Test Search
# ----------------------------------
def test_search():
    es = ElasticsearchManager("localhost", 9200)
    results = es.search_articles("নির্বাচন")
    for i, r in enumerate(results, 1):
        print(f"{i}. {r['title']} - {r['url']}")


if __name__ == "__main__":
    main()
    # test_search()
