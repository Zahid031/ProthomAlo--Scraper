import requests
from bs4 import BeautifulSoup

def scrape_article(url):
    """
    Scrapes a single news article from a given Prothom Alo URL.

    Args:
        url (str): The URL of the news article.

    Returns:
        dict: A dictionary containing the scraped data (headline, author,
              publication_date, content), or None if the request fails.
    """
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(url)
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the URL: {e}")
        return None

    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, "html.parser")

    # --- Extracting the data using the selectors we found ---

    # Extract Headline
    headline_tag = soup.select_one("h1.IiRps")
    headline = headline_tag.get_text(strip=True) if headline_tag else "Headline not found"

    # Extract Author Name
    author_tag = soup.select_one("span.contributor-name._8TSJC")
    author = author_tag.get_text(strip=True) if author_tag else "Author not found"

    location_tag= soup.select_one("span.author-location._8-umj")
    location = location_tag.get_text(strip=True) if location_tag else "Location not found"
    location = location.replace("Location: ", "").strip()

    # Extract Publication Date
    # The date is inside a span, we take the text and clean it up.
    date_tag = soup.select_one("div.time-social-share-wrapper span:first-child")
    # The text includes "Published: ", we can remove it.
    publication_date_raw = date_tag.get_text(strip=True) if date_tag else "Date not found"
    publication_date = publication_date_raw.replace("Published:", "").strip()


    # Extract Article Content
    # Find all 'p' tags within the div with class 'story-content'
    content_paragraphs = soup.select("div.story-content p")
    # Join the text from all paragraphs to form the full article
    content = "\n".join([p.get_text(strip=True) for p in content_paragraphs])

    # --- Storing the data in a dictionary ---
    article_data = {
        "url": url,
        "headline": headline,
        "author": author,
        "location": location,
        "publication_date": publication_date,
        "content": content
    }

    return article_data

if __name__ == "__main__":
    # The URL of the article we want to scrape
    target_url = "https://www.prothomalo.com/politics/5vgwrqbj1d"

    # Call the function and get the data
    scraped_data = scrape_article(target_url)

    # Print the scraped data
    if scraped_data:
        print("--- Article Data ---")
        print(f"URL: {scraped_data['url']}")
        print(f"Headline: {scraped_data['headline']}")
        print(f"Author: {scraped_data['author']}")
        print(f"Location: {scraped_data['location']}")
        print(f"Publication Date: {scraped_data['publication_date']}")
        print("\n--- Content ---")
        print(scraped_data['content'])
        print("\n--------------------")