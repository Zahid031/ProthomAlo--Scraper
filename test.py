import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import random

# Set headers to mimic a browser visit
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

def scrape_article(url, max_retries=3, delay=1):
    """
    Scrapes a single news article from a given Prothom Alo URL with error handling and retries.

    Args:
        url (str): The URL of the news article.
        max_retries (int): Maximum number of retry attempts for failed requests.
        delay (int): Base delay between retries in seconds (will have random jitter).

    Returns:
        dict: A dictionary containing the scraped data (headline, author, publication_date, 
              content, etc.), or None if scraping fails after retries.
    """
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Add random delay to avoid hitting rate limits
            time.sleep(delay * (1 + random.random()))  # Add jitter
            
            # Send an HTTP GET request to the URL with headers
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            
            # Check if content is HTML
            if 'text/html' not in response.headers.get('Content-Type', ''):
                print(f"Unexpected content type for URL {url}")
                return None

            # Parse the HTML content
            soup = BeautifulSoup(response.content, 'html.parser')
            print(soup)

            # Extract Headline
            headline = extract_text(soup, "h1.IiRps", "Headline not found")
            
            # Extract Author Name
            author = extract_text(soup, "span.author-name-location-wrapper", "Author not found")
            
            # Extract and clean Publication Date
            date_tag = soup.select_one("div.time-social-share-wrapper span:first-child")
            publication_date = clean_date(date_tag)
            
            # Extract Article Content
            content_paragraphs = soup.select("div.story-content p")
            content = clean_content(content_paragraphs)
            
            # Extract additional metadata
            category = extract_text(soup, "div.breadcrumb-wrapper a:last-child", "Uncategorized")
            
            # Get current timestamp for when we scraped it
            scraped_at = datetime.utcnow().isoformat()

            return {
                "url": url,
                "headline": headline,
                "author": author,
                "publication_date": publication_date,
                "content": content,
                "category": category,
                "scraped_at": scraped_at,
                "source": "Prothom Alo"
            }

        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"Failed to fetch {url} after {max_retries} attempts. Error: {e}")
                return None
            print(f"Retry {retry_count} for {url} due to error: {e}")

    return None

def extract_text(soup, selector, default=""):
    """Helper function to safely extract text from a BeautifulSoup selector"""
    element = soup.select_one(selector)
    return element.get_text(strip=True) if element else default

def clean_date(date_tag):
    """Clean and format the publication date"""
    if not date_tag:
        return "Date not found"
    
    date_text = date_tag.get_text(strip=True)
    # Remove "Published:" prefix if present
    date_text = date_text.replace("প্রকাশিত:", "").strip()
    date_text = date_text.replace("Published:", "").strip()
    
    # TODO: Add date parsing logic if you need to convert to datetime object
    return date_text

def clean_content(paragraphs):
    """Clean and join the article content paragraphs"""
    if not paragraphs:
        return "Content not found"
    
    # Filter out empty paragraphs and join with newlines
    content = "\n".join(
        p.get_text(strip=True) 
        for p in paragraphs 
        if p.get_text(strip=True)
    )
    
    # Remove any excessive whitespace
    return ' '.join(content.split())

if __name__ == "__main__":
    # Example URLs to test
    test_urls = [
        "https://www.prothomalo.com/politics/5vgwrqbj1d",
        "https://www.prothomalo.com/bangladesh/g9jw3z4x1l",
        "https://www.prothomalo.com/sports/fifa-world-cup-2022-6q8q1q3z1l"
    ]

    for url in test_urls:
        print(f"\nScraping: {url}")
        scraped_data = scrape_article(url)
        
        if scraped_data:
            print("--- Article Data ---")
            print(f"URL: {scraped_data['url']}")
            print(f"Headline: {scraped_data['headline']}")
            print(f"Author: {scraped_data['author']}")
            print(f"Publication Date: {scraped_data['publication_date']}")
            print(f"Category: {scraped_data['category']}")
            print("\n--- Content Excerpt ---")
            print(scraped_data['content'][:200] + "...")  # Print first 200 chars
            print("\n--------------------")
        else:
            print(f"Failed to scrape article at {url}")