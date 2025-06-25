"""
Enhanced Prothom Alo News Scraper with Complete Elasticsearch Operations

This script provides comprehensive functionality for scraping, searching, filtering,
updating, and managing news articles from Prothom Alo's politics section.

New Features Added:
- Advanced search with multiple parameters
- Content filtering by date, author, location
- Individual article retrieval and updates
- Bulk operations and analytics
- Query building helpers
- Data management utilities
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
import time
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError, RequestError
import logging
from typing import Optional, Dict, List, Any, Union
import json

# --- Configuration ---
class Config:
    """Centralized configuration for the scraper."""
    BASE_URL = "https://www.prothomalo.com/"
    API_URL = "https://www.prothomalo.com/api/v1/collections/politics"
    ES_HOST = "http://localhost:9200"
    ES_USER = "elastic"
    ES_PASSWORD = "JvQhvZYl"
    ES_INDEX = "prothomalo_politics"
    DEFAULT_MAX_PAGES = 2
    STORIES_PER_PAGE = 12
    REQUEST_DELAY = 1  # seconds between requests
    BULK_INDEX_SIZE = 100  # documents per bulk operation
    DEFAULT_SEARCH_SIZE = 20  # default number of search results

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('prothomalo_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProthomAloScraperEnhanced:
    """Enhanced scraper class with full Elasticsearch operations."""
    
    def __init__(self):
        self.config = Config()
        self.es_client = None
        self.bengali_to_english_digits = str.maketrans('০১২৩৪৫৬৭৮৯', '0123456789')
        self.bengali_months = {
            'জানুয়ারি': '01', 'ফেব্রুয়ারি': '02', 'মার্চ': '03', 'এপ্রিল': '04',
            'মে': '05', 'জুন': '06', 'জুলাই': '07', 'আগস্ট': '08',
            'সেপ্টেম্বর': '09', 'অক্টোবর': '10', 'নভেম্বর': '11', 'ডিসেম্বর': '12'
        }
    
    def connect_to_elasticsearch(self) -> bool:
        """Establishes connection to Elasticsearch with authentication."""
        try:
            logger.info("Connecting to Elasticsearch...")
            self.es_client = Elasticsearch(
                hosts=[self.config.ES_HOST],
                basic_auth=(self.config.ES_USER, self.config.ES_PASSWORD),
                verify_certs=False
            )
            
            if self.es_client.ping():
                logger.info("Successfully connected to Elasticsearch")
                return True
            else:
                logger.error("Elasticsearch ping failed")
                return False
                
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            return False
    
    def create_index_if_not_exists(self) -> bool:
        """Creates the Elasticsearch index with proper mapping if it doesn't exist."""
        try:
            if self.es_client.indices.exists(index=self.config.ES_INDEX):
                logger.info(f"Index '{self.config.ES_INDEX}' already exists")
                return True
            
            logger.info(f"Creating index '{self.config.ES_INDEX}' with custom mapping...")
            
            index_mapping = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "analysis": {
                        "analyzer": {
                            "bengali_analyzer": {
                                "type": "standard",
                                "stopwords": "_none_"
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "url": {"type": "keyword"},
                        "headline": {
                            "type": "text", 
                            "analyzer": "bengali_analyzer",
                            "fields": {
                                "raw": {"type": "keyword"}
                            }
                        },
                        "author": {"type": "text"},
                        "location": {"type": "keyword"},
                        "published_at": {"type": "date", "format": "yyyy-MM-dd HH:mm"},
                        "content": {
                            "type": "text", 
                            "analyzer": "bengali_analyzer"
                        },
                        "scraped_at": {"type": "date"},
                        "word_count": {"type": "integer"},
                        "last_updated": {"type": "date"}
                    }
                }
            }
            
            self.es_client.indices.create(index=self.config.ES_INDEX, body=index_mapping)
            logger.info("Index created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create index: {e}")
            return False
    
    def parse_bengali_date(self, date_str: str) -> Optional[str]:
        """Converts Bengali datetime string to ISO format with time."""
        if not date_str or "not found" in date_str.lower():
            return None

        try:
            parts = date_str.strip().split(",")
            if len(parts) != 2:
                return None

            date_part_bn = parts[0].strip()
            time_part_bn = parts[1].strip().replace(" ", "")

            date_en = date_part_bn.translate(self.bengali_to_english_digits)
            time_en = time_part_bn.translate(self.bengali_to_english_digits)

            day, month_bn, year = date_en.split()
            month = self.bengali_months.get(month_bn)
            if not month:
                return None

            return f"{year}-{month}-{day} {time_en}"

        except Exception as e:
            logger.warning(f"Failed to parse datetime '{date_str}': {e}")
            return None
    
    def scrape_single_article(self, url: str) -> Optional[Dict[str, Any]]:
        """Scrapes a single article from the given URL."""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            
            headline_tag = soup.select_one("h1.IiRps")
            headline = headline_tag.get_text(strip=True) if headline_tag else "Headline not found"
            
            author_tag = soup.select_one("span.contributor-name._8TSJC")
            author = author_tag.get_text(strip=True) if author_tag else "Author not found"
            
            location_tag = soup.select_one("span.author-location._8-umj")
            location = location_tag.get_text(strip=True) if location_tag else "Location not found"
            location = location.replace("Location: ", "").strip()
            
            date_tag = soup.select_one("div.time-social-share-wrapper span:first-child")
            publication_date_raw = date_tag.get_text(strip=True) if date_tag else "Date not found"
            publication_date_cleaned = publication_date_raw.split(":", 1)[-1].strip()
            publication_date = self.parse_bengali_date(publication_date_cleaned)
            
            content_paragraphs = soup.select("div.story-content p")
            content = "\n".join([p.get_text(strip=True) for p in content_paragraphs])
            word_count = len(content.split()) if content else 0
            
            article_data = {
                "url": url,
                "headline": headline,
                "author": author,
                "location": location,
                "published_at": publication_date,
                "content": content,
                "scraped_at": datetime.now().isoformat(),
                "word_count": word_count,
                "last_updated": datetime.now().isoformat()
            }
            
            logger.info(f"Successfully scraped: {headline[:50]}...")
            return article_data
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None
    
    def get_article_urls_from_api(self, max_pages: int) -> List[str]:
        """Fetches article URLs from the Prothom Alo API."""
        article_urls = []
        
        for page_num in range(max_pages):
            skip = page_num * self.config.STORIES_PER_PAGE
            params = {'skip': skip, 'limit': self.config.STORIES_PER_PAGE}
            
            try:
                logger.info(f"Fetching page {page_num + 1}/{max_pages} from API...")
                response = requests.get(self.config.API_URL, params=params, timeout=10)
                response.raise_for_status()
                
                stories = response.json().get('items', [])
                if not stories:
                    logger.info("No more stories found, stopping pagination")
                    break
                
                for story in stories:
                    slug = story.get('story', {}).get('slug')
                    if slug:
                        url = urljoin(self.config.BASE_URL, slug)
                        article_urls.append(url)
                
                time.sleep(self.config.REQUEST_DELAY)
                
            except Exception as e:
                logger.error(f"Error fetching API page {page_num + 1}: {e}")
                break
        
        logger.info(f"Found {len(article_urls)} article URLs")
        return article_urls
    
    # ========================
    # ELASTICSEARCH OPERATIONS
    # ========================
    
    def insert_article(self, article_data: Dict[str, Any]) -> bool:
        """
        Insert a single article into Elasticsearch.
        
        Args:
            article_data: Dictionary containing article information
            
        Returns:
            bool: True if insertion successful
        """
        try:
            doc_id = quote(article_data['url'], safe='')
            
            response = self.es_client.index(
                index=self.config.ES_INDEX,
                id=doc_id,
                body=article_data
            )
            
            logger.info(f"Article inserted successfully: {response['_id']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert article: {e}")
            return False
    
    def bulk_index_articles(self, articles: List[Dict[str, Any]]) -> bool:
        """Efficiently bulk index articles into Elasticsearch."""
        if not articles:
            logger.warning("No articles to index")
            return False
        
        try:
            actions = []
            for article in articles:
                doc_id = quote(article['url'], safe='')
                
                action = {
                    "_index": self.config.ES_INDEX,
                    "_id": doc_id,
                    "_source": article
                }
                actions.append(action)
            
            logger.info(f"Starting bulk indexing of {len(actions)} documents...")
            
            success, failed = helpers.bulk(
                self.es_client,
                actions,
                chunk_size=self.config.BULK_INDEX_SIZE,
                request_timeout=60,
                raise_on_error=False
            )
            
            logger.info(f"Successfully indexed {success} documents")
            if failed:
                logger.warning(f"Failed to index {len(failed)} documents")
            
            return True
            
        except Exception as e:
            logger.error(f"Bulk indexing failed: {e}")
            return False
    
    def get_article_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific article by its URL.
        
        Args:
            url: The article URL
            
        Returns:
            dict: Article data or None if not found
        """
        try:
            doc_id = quote(url, safe='')
            response = self.es_client.get(
                index=self.config.ES_INDEX,
                id=doc_id
            )
            
            article_data = response['_source']
            article_data['_id'] = response['_id']
            article_data['_score'] = response.get('_score')
            
            logger.info(f"Retrieved article: {article_data.get('headline', 'Unknown')[:50]}...")
            return article_data
            
        except NotFoundError:
            logger.warning(f"Article not found for URL: {url}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving article: {e}")
            return None
    
    def get_article_by_id(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific article by its document ID.
        
        Args:
            doc_id: The Elasticsearch document ID
            
        Returns:
            dict: Article data or None if not found
        """
        try:
            response = self.es_client.get(
                index=self.config.ES_INDEX,
                id=doc_id
            )
            
            article_data = response['_source']
            article_data['_id'] = response['_id']
            
            return article_data
            
        except NotFoundError:
            logger.warning(f"Article not found for ID: {doc_id}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving article by ID: {e}")
            return None
    
    def update_article(self, url: str, updates: Dict[str, Any]) -> bool:
        """
        Update an existing article in Elasticsearch.
        
        Args:
            url: The article URL
            updates: Dictionary of fields to update
            
        Returns:
            bool: True if update successful
        """
        try:
            doc_id = quote(url, safe='')
            
            # Add last_updated timestamp
            updates['last_updated'] = datetime.now().isoformat()
            
            response = self.es_client.update(
                index=self.config.ES_INDEX,
                id=doc_id,
                body={"doc": updates}
            )
            
            logger.info(f"Article updated successfully: {response['_id']}")
            return True
            
        except NotFoundError:
            logger.warning(f"Cannot update - article not found for URL: {url}")
            return False
        except Exception as e:
            logger.error(f"Failed to update article: {e}")
            return False
    
    def delete_article(self, url: str) -> bool:
        """
        Delete an article from Elasticsearch.
        
        Args:
            url: The article URL
            
        Returns:
            bool: True if deletion successful
        """
        try:
            doc_id = quote(url, safe='')
            
            response = self.es_client.delete(
                index=self.config.ES_INDEX,
                id=doc_id
            )
            
            logger.info(f"Article deleted successfully: {response['_id']}")
            return True
            
        except NotFoundError:
            logger.warning(f"Cannot delete - article not found for URL: {url}")
            return False
        except Exception as e:
            logger.error(f"Failed to delete article: {e}")
            return False
    
    def search_articles(self, 
                       query: str = None,
                       author: str = None,
                       location: str = None,
                       start_date: str = None,
                       end_date: str = None,
                       min_word_count: int = None,
                       max_word_count: int = None,
                       size: int = None,
                       sort_by: str = "published_at",
                       sort_order: str = "desc") -> Dict[str, Any]:
        """
        Advanced search with multiple filters.
        
        Args:
            query: Text to search in headline and content
            author: Filter by author name  
            location: Filter by location
            start_date: Start date for published_at filter (YYYY-MM-DD)
            end_date: End date for published_at filter (YYYY-MM-DD)
            min_word_count: Minimum word count
            max_word_count: Maximum word count
            size: Number of results to return
            sort_by: Field to sort by
            sort_order: Sort order (asc/desc)
            
        Returns:
            dict: Search results with hits and metadata
        """
        if size is None:
            size = self.config.DEFAULT_SEARCH_SIZE
        
        try:
            # Build the search query
            search_body = {
                "size": size,
                "sort": [{sort_by: {"order": sort_order}}],
                "query": {"bool": {"must": [], "filter": []}},
                "highlight": {
                    "fields": {
                        "headline": {},
                        "content": {"fragment_size": 150, "number_of_fragments": 3}
                    }
                }
            }
            
            # Text search
            if query:
                search_body["query"]["bool"]["must"].append({
                    "multi_match": {
                        "query": query,
                        "fields": ["headline^2", "content"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                })
            else:
                search_body["query"]["bool"]["must"].append({"match_all": {}})
            
            # Author filter
            if author:
                search_body["query"]["bool"]["filter"].append({
                    "match": {"author": author}
                })
            
            # Location filter
            if location:
                search_body["query"]["bool"]["filter"].append({
                    "term": {"location": location}
                })
            
            # Date range filter
            if start_date or end_date:
                date_range = {}
                if start_date:
                    date_range["gte"] = start_date
                if end_date:
                    date_range["lte"] = end_date
                
                search_body["query"]["bool"]["filter"].append({
                    "range": {"published_at": date_range}
                })
            
            # Word count range filter
            if min_word_count or max_word_count:
                word_count_range = {}
                if min_word_count:
                    word_count_range["gte"] = min_word_count
                if max_word_count:
                    word_count_range["lte"] = max_word_count
                
                search_body["query"]["bool"]["filter"].append({
                    "range": {"word_count": word_count_range}
                })
            
            # Execute search
            response = self.es_client.search(
                index=self.config.ES_INDEX,
                body=search_body
            )
            
            # Process results
            results = {
                "total_hits": response["hits"]["total"]["value"],
                "max_score": response["hits"]["max_score"],
                "took": response["took"],
                "articles": []
            }
            
            for hit in response["hits"]["hits"]:
                article = hit["_source"]
                article["_id"] = hit["_id"]
                article["_score"] = hit["_score"]
                
                if "highlight" in hit:
                    article["highlight"] = hit["highlight"]
                
                results["articles"].append(article)
            
            logger.info(f"Search completed: {results['total_hits']} results found")
            return results
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {"total_hits": 0, "articles": [], "error": str(e)}
    
    def filter_articles_by_date_range(self, start_date: str, end_date: str, size: int = 50) -> List[Dict[str, Any]]:
        """
        Filter articles by date range.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            size: Maximum number of results
            
        Returns:
            list: List of articles within the date range
        """
        return self.search_articles(
            start_date=start_date,
            end_date=end_date,
            size=size
        )["articles"]
    
    def filter_articles_by_author(self, author: str, size: int = 50) -> List[Dict[str, Any]]:
        """
        Filter articles by author.
        
        Args:
            author: Author name to filter by
            size: Maximum number of results
            
        Returns:
            list: List of articles by the specified author
        """
        return self.search_articles(author=author, size=size)["articles"]
    
    def filter_articles_by_location(self, location: str, size: int = 50) -> List[Dict[str, Any]]:
        """
        Filter articles by location.
        
        Args:
            location: Location to filter by
            size: Maximum number of results
            
        Returns:
            list: List of articles from the specified location
        """
        return self.search_articles(location=location, size=size)["articles"]
    
    def get_recent_articles(self, days: int = 7, size: int = 20) -> List[Dict[str, Any]]:
        """
        Get articles from the last N days.
        
        Args:
            days: Number of days to look back
            size: Maximum number of results
            
        Returns:
            list: List of recent articles
        """
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        return self.filter_articles_by_date_range(start_date, end_date, size)
    
    def get_articles_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the articles in the index.
        
        Returns:
            dict: Statistics including counts, averages, and distributions
        """
        try:
            # Get total count
            count_response = self.es_client.count(index=self.config.ES_INDEX)
            total_articles = count_response['count']
            
            # Get aggregations for detailed stats
            agg_body = {
                "size": 0,
                "aggs": {
                    "authors": {
                        "terms": {
                            "field": "author.keyword",
                            "size": 10
                        }
                    },
                    "locations": {
                        "terms": {
                            "field": "location",
                            "size": 10
                        }
                    },
                    "word_count_stats": {
                        "stats": {
                            "field": "word_count"
                        }
                    },
                    "articles_by_date": {
                        "date_histogram": {
                            "field": "published_at",
                            "calendar_interval": "day"
                        }
                    }
                }
            }
            
            response = self.es_client.search(
                index=self.config.ES_INDEX,
                body=agg_body
            )
            
            aggs = response["aggregations"]
            
            stats = {
                "total_articles": total_articles,
                "top_authors": [
                    {"author": bucket["key"], "count": bucket["doc_count"]}
                    for bucket in aggs["authors"]["buckets"]
                ],
                "top_locations": [
                    {"location": bucket["key"], "count": bucket["doc_count"]}
                    for bucket in aggs["locations"]["buckets"]
                ],
                "word_count_stats": {
                    "average": round(aggs["word_count_stats"]["avg"], 2),
                    "min": aggs["word_count_stats"]["min"],
                    "max": aggs["word_count_stats"]["max"],
                    "total": aggs["word_count_stats"]["sum"]
                },
                "articles_per_day": len(aggs["articles_by_date"]["buckets"])
            }
            
            logger.info("Statistics retrieved successfully")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {"error": str(e)}
    
    def search_by_keyword_in_content(self, keyword: str, size: int = 20) -> List[Dict[str, Any]]:
        """
        Search for articles containing specific keywords in content.
        
        Args:
            keyword: Keyword to search for
            size: Maximum number of results
            
        Returns:
            list: List of matching articles
        """
        return self.search_articles(query=keyword, size=size)["articles"]
    
    def upsert_article(self, article_data: Dict[str, Any]) -> bool:
        """
        Insert or update an article (upsert operation).
        
        Args:
            article_data: Article data dictionary
            
        Returns:
            bool: True if operation successful
        """
        try:
            doc_id = quote(article_data['url'], safe='')
            
            # Add timestamps
            article_data['last_updated'] = datetime.now().isoformat()
            if 'scraped_at' not in article_data:
                article_data['scraped_at'] = datetime.now().isoformat()
            
            response = self.es_client.index(
                index=self.config.ES_INDEX,
                id=doc_id,
                body=article_data
            )
            
            action = "updated" if response["result"] == "updated" else "created"
            logger.info(f"Article {action}: {doc_id}")
            return True
            
        except Exception as e:
            logger.error(f"Upsert failed: {e}")
            return False
    
    # ========================
    # PIPELINE OPERATIONS
    # ========================
    
    def run_scraping_pipeline(self, max_pages: int = None) -> bool:
        """Runs the complete scraping and indexing pipeline."""
        if max_pages is None:
            max_pages = self.config.DEFAULT_MAX_PAGES
        
        logger.info(f"Starting scraping pipeline for {max_pages} pages...")
        
        if not self.connect_to_elasticsearch():
            return False
        
        if not self.create_index_if_not_exists():
            return False
        
        article_urls = self.get_article_urls_from_api(max_pages)
        if not article_urls:
            logger.error("No article URLs found")
            return False
        
        scraped_articles = []
        total_urls = len(article_urls)
        
        for i, url in enumerate(article_urls, 1):
            logger.info(f"Scraping article {i}/{total_urls}: {url}")
            
            article_data = self.scrape_single_article(url)
            if article_data:
                scraped_articles.append(article_data)
            
            if i < total_urls:
                time.sleep(self.config.REQUEST_DELAY)
        
        logger.info(f"Successfully scraped {len(scraped_articles)}/{total_urls} articles")
        
        if scraped_articles:
            return self.bulk_index_articles(scraped_articles)
        else:
            logger.warning("No articles were successfully scraped")
            return False

def main():
    """Main entry point with example usage of all features."""
    scraper = ProthomAloScraperEnhanced()
    
    # Connect to Elasticsearch
    if not scraper.connect_to_elasticsearch():
        return
    
    # Create index
    scraper.create_index_if_not_exists()
    
    # Example operations
    logger.info("="*60)
    logger.info("PROTHOM ALO ENHANCED SCRAPER - DEMO")
    logger.info("="*60)
    
    # 1. Run scraping pipeline
    logger.info("1. Running scraping pipeline...")
    scraper.run_scraping_pipeline(max_pages=1)  # Scrape 1 page for demo
    
    # 2. Search articles
    logger.info("\n2. Searching articles...")
    search_results = scraper.search_articles(query="রাজনীতি", size=5)
    logger.info(f"Found {search_results['total_hits']} articles")
    
    # 3. Filter by author
    logger.info("\n3. Getting articles by author...")
    author_articles = scraper.filter_articles_by_author("প্রতিবেদক", size=3)
    logger.info(f"Found {len(author_articles)} articles by author")
    
    # 4. Get recent articles
    logger.info("\n4. Getting recent articles...")
    recent_articles = scraper.get_recent_articles(days=30, size=5)
    logger.info(f"Found {len(recent_articles)} recent articles")
    
    # 5. Get statistics
    logger.info("\n5. Getting statistics...")
    stats = scraper.get_articles_statistics()
    logger.info(f"Total articles: {stats.get('total_articles', 0)}")
    
    # 6. Example update operation
    logger.info("\n6. Example update operation...")
    if search_results['articles']:
        first_article = search_results['articles'][0]
        url = first_article['url']
        updates = {"custom_tag": "demo_updated"}
        scraper.update_article(url, updates)
    
    logger.info("="*60)
    logger.info("DEMO COMPLETED")
    logger.info("="*60)

if __name__ == "__main__":
    main()