# # In file: scrape_category.py

# import requests
# from urllib.parse import urljoin
# import time

# from scrape_article import scrape_article

# # The base URL for the Prothom Alo website
# BASE_URL = "https://www.prothomalo.com/"
# # The API endpoint we will use to get article lists
# API_URL = "https://www.prothomalo.com/api/v1/collections/politics"

# def get_articles_from_api(max_pages=5):
#     """
#     Uses the Prothom Alo API to fetch article information from multiple pages.

#     Args:
#         max_pages (int): The maximum number of pages to scrape.

#     Returns:
#         list: A list of dictionaries, where each dictionary contains the
#               scraped data for one article.
#     """
#     all_articles_data = []
    
#     # The website loads 12 stories per page
#     stories_per_page = 12

#     # This loop will run for each page we want to scrape
#     for page_num in range(max_pages):
#         # Calculate the 'skip' value for the current page
#         skip = page_num * stories_per_page
        
#         # These parameters will be added to the API_URL (e.g., ?skip=12&limit=12)
#         params = {
#             'skip': skip,
#             'limit': stories_per_page
#         }

#         print(f"\n--- Fetching page {page_num + 1} from the API ---")

#         try:
#             # Make the request to the API
#             response = requests.get(API_URL, params=params, headers={'User-Agent': 'Mozilla/5.0'})
#             response.raise_for_status() # Check for errors like 404 or 500
            
#             # Convert the JSON response from the API into a Python dictionary
#             data = response.json()

#         except requests.exceptions.RequestException as e:
#             print(f"Error fetching data from API: {e}")
#             break # Stop the loop if there's a network error
#         except ValueError:
#             print("Error decoding JSON from API response. The page might not exist.")
#             break

#         # The story information is inside the 'items' key of the dictionary
#         stories = data.get('items', [])
        
#         # If the API returns no stories, we've reached the end
#         if not stories:
#             print("No more stories found. Stopping.")
#             break 

#         # Loop through each story we received from the API
#         for story in stories:
#             # The 'slug' is the unique end part of the article's URL
#             slug = story.get('story', {}).get('slug')
#             if slug:
#                 # Create the full URL (e.g., https://www.prothomalo.com/politics/xyz)
#                 absolute_url = urljoin(BASE_URL, slug)
                
#                 print(f"  -> Found article. Now scraping: {absolute_url}")
                
#                 # Now, use your function to scrape the full article page
#                 article_data = scrape_article(absolute_url)
                
#                 if article_data:
#                     all_articles_data.append(article_data)
                
#                 # Pause for 1 second to be respectful to the website's server
#                 time.sleep(1) 

#     return all_articles_data

# # This is the main part of the script that runs when you execute the file
# if __name__ == "__main__":
#     # You can change this number to scrape more or fewer pages
#     NUM_PAGES_TO_SCRAPE = 3
    
#     articles = get_articles_from_api(max_pages=NUM_PAGES_TO_SCRAPE)

#     if articles:
#         print(f"\n========================================================")
#         print(f"Finished! Successfully scraped a total of {len(articles)} articles.")
#         print(f"========================================================")
        
#         # Print the details of the first article as a sample
#         print("\n--- Sample of First Scraped Article ---")
#         print(f"Headline: {articles[0]['headline']}")
#         print(f"Author: {articles[0]['author']}")
#         print(f"URL: {articles[0]['url']}")
#         print("------------------------------------")
#     else:
#         print("\nNo articles were scraped.")




# In file: scrape_category.py (Final Version)
# In file: scrape_category.py (Final Version for Insertion)

#scrape_category.py

import requests
from urllib.parse import urljoin, quote
import time
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

# Make sure your 'scrape_single_article.py' file is in the same directory,
# as this script needs to import the 'scrape_article' function from it.
from scrape_article import scrape_article 

# --- Configuration ---
# All your project's settings are here for easy modification.
BASE_URL = "https://www.prothomalo.com/"
API_URL = "https://www.prothomalo.com/api/v1/collections/politics"
ES_HOST = "http://localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "JvQhvZYl"
ES_INDEX = "prothomalo_politics"

# --- Date Parsing Helper ---
def parse_bengali_date(date_str):
    """
    Converts a Prothom Alo date string (e.g., "২২ জুন ২০২৫") 
    into a standard 'YYYY-MM-DD' format that Elasticsearch can understand.
    """
    if not date_str or "not found" in date_str.lower():
        return None
        
    bengali_to_english_digits = str.maketrans('০১২৩৪৫৬৭৮৯', '0123456789')
    bengali_months = {
        'জানুয়ারি': '01', 'ফেব্রুয়ারি': '02', 'মার্চ': '03', 'এপ্রিল': '04',
        'মে': '05', 'জুন': '06', 'জুলাই': '07', 'আগস্ট': '08',
        'সেপ্টেম্বর': '09', 'অক্টোবর': '10', 'নভেম্বর': '11', 'ডিসেম্বর': '12'
    }
    
    # Translate digits to English first
    date_str_en = date_str.translate(bengali_to_english_digits)
    
    parts = date_str_en.split()
    if len(parts) < 3:
        return None # Not a valid format

    try:
        day, month_bn, year = parts[0], parts[1], parts[2]
        month = bengali_months.get(month_bn)
        if not month:
            return None
        
        # Create a datetime object to validate and format correctly
        dt_object = datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d')
        return dt_object.strftime('%Y-%m-%d')
    except (ValueError, IndexError):
        # Return None if parsing fails for any reason
        return None

# --- Main Scraping and Indexing Logic ---
def scrape_and_index(max_pages=2):
    """
    The main function that scrapes articles and indexes them into a secure Elasticsearch.
    
    Args:
        max_pages (int): The number of pages to scrape from the API.
    """
    try:
        print("Connecting to Elasticsearch with credentials...")
        es_client = Elasticsearch(
            hosts=[ES_HOST],
            basic_auth=(ES_USER, ES_PASSWORD),
            # This is needed for the default self-signed cert from Docker
            verify_certs=False  
        )
        if not es_client.ping():
            raise ConnectionError("Ping to Elasticsearch failed.")
        print("Successfully connected to Elasticsearch.")
    except Exception as e:
        print(f"Could not connect to Elasticsearch. Please check if it's running. Error: {e}")
        return

    all_articles = []
    stories_per_page = 12

    # Loop through the API pages to get article links
    for page_num in range(max_pages):
        skip = page_num * stories_per_page
        params = {'skip': skip, 'limit': stories_per_page}
        print(f"\n--- Fetching page {page_num + 1} from Prothom Alo API ---")
        
        try:
            response = requests.get(API_URL, params=params)
            response.raise_for_status() # Check for HTTP errors
            stories = response.json().get('items', [])
            if not stories:
                print("No more stories found on the API. Stopping.")
                break
            
            # For each story found, scrape the full article
            for story in stories:
                slug = story.get('story', {}).get('slug')
                if slug:
                    url = urljoin(BASE_URL, slug)
                    print(f"  -> Scraping: {url}")
                    # This calls the function from your other file
                    article_data = scrape_article(url) 
                    if article_data:
                        all_articles.append(article_data)
                    # Be a good web citizen: wait a little between requests
                    time.sleep(1)
        except Exception as e:
            print(f"An error occurred during scraping: {e}")
            break
            
    if not all_articles:
        print("\nScraping finished, but no articles were collected. Nothing to index.")
        return

    print(f"\nScraping complete. Total articles collected: {len(all_articles)}")
    print("Preparing data for bulk insertion into Elasticsearch...")
    
    actions = []
    for article in all_articles:
        # We must use the URL as a unique ID to prevent adding the same article twice.
        # The 'quote' function makes the URL safe to use as an ID.
        doc_id = quote(article['url'], safe='')
        
        action = {
            "_index": ES_INDEX,
            "_id": doc_id,
            "_source": {
                "url": article.get("url"),
                "headline": article.get("headline"),
                "author": article.get("author"),
                "location": article.get("location"),
                "publication_date": parse_bengali_date(article.get("publication_date")),
                "content": article.get("content")
            }
        }
        actions.append(action)
    
    print(f"Starting bulk indexing of {len(actions)} documents...")
    try:
        # 'helpers.bulk' is the most efficient way to insert many documents.
        success, failed = helpers.bulk(es_client, actions, raise_on_error=True)
        print(f"Successfully indexed {success} documents.")
        if failed:
            print(f"Failed to index {len(failed)} documents.")
    except Exception as e:
        print(f"An error occurred during bulk indexing: {e}")

if __name__ == "__main__":
    # You can change this number to scrape more or fewer pages.
    # Let's scrape 2 pages and insert the data into our database.
    scrape_and_index(max_pages=2)