"""
Consolidated Prothom Alo News Scraper with Elasticsearch Integration

This script efficiently scrapes news articles from Prothom Alo's politics section
and indexes them into Elasticsearch with proper error handling and optimization.

Features:
- Automatic Elasticsearch index creation with proper mapping
- Efficient bulk indexing to minimize database operations
- Bengali date parsing and standardization
- Comprehensive error handling and logging
- Rate limiting to be respectful to the website
- Duplicate prevention using URL-based document IDs
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
import time
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import logging
from typing import Optional, Dict, List, Any

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

class ProthomAloScraper:
    """Main scraper class that handles all operations."""
    
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
        """
        Establishes connection to Elasticsearch with authentication.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            logger.info("Connecting to Elasticsearch...")
            self.es_client = Elasticsearch(
                hosts=[self.config.ES_HOST],
                basic_auth=(self.config.ES_USER, self.config.ES_PASSWORD),
                verify_certs=False  # For local Docker development
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
        """
        Creates the Elasticsearch index with proper mapping if it doesn't exist.
        
        Returns:
            bool: True if index exists or was created successfully
        """
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
                        "word_count": {"type": "integer"}
                    }
                }
            }
            
            self.es_client.indices.create(index=self.config.ES_INDEX, body=index_mapping)
            logger.info("Index created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create index: {e}")
            return False
    
    # def parse_bengali_date(self, date_str: str) -> Optional[str]:
    #     """
    #     Converts Bengali date string to ISO format (YYYY-MM-DD).
        
    #     Args:
    #         date_str: Bengali date string (e.g., "২২ জুন ২০২৫")
            
    #     Returns:
    #         str: ISO formatted date or None if parsing fails
    #     """
    #     if not date_str or "not found" in date_str.lower():
    #         return None
        
    #     try:
    #         # Convert Bengali digits to English
    #         date_str_en = date_str.translate(self.bengali_to_english_digits)
    #         parts = date_str_en.split()
            
    #         if len(parts) < 3:
    #             return None
            
    #         day, month_bn, year = parts[0], parts[1], parts[2]
    #         month = self.bengali_months.get(month_bn)
            
    #         if not month:
    #             return None
            
    #         # Validate and format the date
    #         dt_object = datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d')
    #         return dt_object.strftime('%Y-%m-%d')
            
    #     except (ValueError, IndexError) as e:
    #         logger.warning(f"Failed to parse date '{date_str}': {e}")
    #         return None
    

    def parse_bengali_date(self, date_str: str) -> Optional[str]:
        """
        Converts Bengali datetime string (e.g. '২২ জুন ২০২৫, ১৯:১৪')
        to ISO format with time (e.g. '2025-06-22 19:14')
        """
        if not date_str or "not found" in date_str.lower():
            return None

        try:
            # Split into date and time
            parts = date_str.strip().split(",")
            if len(parts) != 2:
                return None

            date_part_bn = parts[0].strip()
            time_part_bn = parts[1].strip().replace(" ", "")  # Remove any extra space in time

            # Translate Bengali digits
            date_en = date_part_bn.translate(self.bengali_to_english_digits)
            time_en = time_part_bn.translate(self.bengali_to_english_digits)

            # Split and map
            day, month_bn, year = date_en.split()
            month = self.bengali_months.get(month_bn)
            if not month:
                return None

            return f"{year}-{month}-{day} {time_en}"

        except Exception as e:
            logger.warning(f"Failed to parse datetime '{date_str}': {e}")
            return None


    
    def scrape_single_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrapes a single article from the given URL.
        
        Args:
            url: The article URL to scrape
            
        Returns:
            dict: Article data or None if scraping fails
        """
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Extract article data using CSS selectors
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

            
            # Extract content paragraphs
            content_paragraphs = soup.select("div.story-content p")
            content = "\n".join([p.get_text(strip=True) for p in content_paragraphs])
            
            # Calculate word count for analytics
            word_count = len(content.split()) if content else 0
            
            article_data = {
                "url": url,
                "headline": headline,
                "author": author,
                "location": location,
                "published_at":publication_date,
                "content": content,
                "scraped_at": datetime.now().isoformat(),
                "word_count": word_count
            }
            
            logger.info(f"Successfully scraped: {headline[:50]}...")
            return article_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error scraping {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error scraping {url}: {e}")
            return None
    
    def get_article_urls_from_api(self, max_pages: int) -> List[str]:
        """
        Fetches article URLs from the Prothom Alo API.
        
        Args:
            max_pages: Number of pages to fetch from the API
            
        Returns:
            list: List of article URLs
        """
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
                
                # Rate limiting
                time.sleep(self.config.REQUEST_DELAY)
                
            except Exception as e:
                logger.error(f"Error fetching API page {page_num + 1}: {e}")
                break
        
        logger.info(f"Found {len(article_urls)} article URLs")
        return article_urls
    
    def bulk_index_articles(self, articles: List[Dict[str, Any]]) -> bool:
        """
        Efficiently bulk index articles into Elasticsearch.
        
        Args:
            articles: List of article data dictionaries
            
        Returns:
            bool: True if indexing successful
        """
        if not articles:
            logger.warning("No articles to index")
            return False
        
        try:
            actions = []
            for article in articles:
                # Use URL as unique document ID to prevent duplicates
                doc_id = quote(article['url'], safe='')
                
                action = {
                    "_index": self.config.ES_INDEX,
                    "_id": doc_id,
                    "_source": article
                }
                actions.append(action)
            
            logger.info(f"Starting bulk indexing of {len(actions)} documents...")
            
            # Use bulk helper for efficient indexing
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
                for failure in failed[:5]:  # Log first 5 failures
                    logger.error(f"Index failure: {failure}")
            
            return True
            
        except Exception as e:
            logger.error(f"Bulk indexing failed: {e}")
            return False
    
    def run_scraping_pipeline(self, max_pages: int = None) -> bool:
        """
        Runs the complete scraping and indexing pipeline.
        
        Args:
            max_pages: Number of pages to scrape (defaults to config value)
            
        Returns:
            bool: True if pipeline completed successfully
        """
        if max_pages is None:
            max_pages = self.config.DEFAULT_MAX_PAGES
        
        logger.info(f"Starting scraping pipeline for {max_pages} pages...")
        
        # Step 1: Connect to Elasticsearch
        if not self.connect_to_elasticsearch():
            return False
        
        # Step 2: Create index if needed
        if not self.create_index_if_not_exists():
            return False
        
        # Step 3: Get article URLs from API
        article_urls = self.get_article_urls_from_api(max_pages)
        if not article_urls:
            logger.error("No article URLs found")
            return False
        
        # Step 4: Scrape articles
        scraped_articles = []
        total_urls = len(article_urls)
        
        for i, url in enumerate(article_urls, 1):
            logger.info(f"Scraping article {i}/{total_urls}: {url}")
            
            article_data = self.scrape_single_article(url)
            if article_data:
                scraped_articles.append(article_data)
            
            # Rate limiting between requests
            if i < total_urls:  # Don't sleep after the last request
                time.sleep(self.config.REQUEST_DELAY)
        
        logger.info(f"Successfully scraped {len(scraped_articles)}/{total_urls} articles")
        
        # Step 5: Bulk index to Elasticsearch
        if scraped_articles:
            return self.bulk_index_articles(scraped_articles)
        else:
            logger.warning("No articles were successfully scraped")
            return False

def main():
    """Main entry point for the scraper."""
    scraper = ProthomAloScraper()
    
    # You can customize the number of pages to scrape
    max_pages = 2  # Change this value as needed
    
    logger.info("="*60)
    logger.info("PROTHOM ALO NEWS SCRAPER STARTING")
    logger.info("="*60)
    
    success = scraper.run_scraping_pipeline(max_pages=max_pages)
    
    if success:
        logger.info("="*60)
        logger.info("SCRAPING PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*60)
    else:
        logger.error("="*60)
        logger.error("SCRAPING PIPELINE FAILED")
        logger.error("="*60)

if __name__ == "__main__":
    main()
