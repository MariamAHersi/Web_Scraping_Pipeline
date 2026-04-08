## Ingestion - Web Scraping
# Each book has been extracted from https://books.toscrape.com

#%%time
# Imports
import requests
import time
import random
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

#%%time
# Get request - no fancy headers needed, books.toscrape.com is open
def send_get_request(url):
    session = requests.Session()
    response = session.get(url, timeout=15)
    response.raise_for_status()
    return response

#%%time
# Scrape a single book page
def get_book_information(url):
    try:
        response = send_get_request(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # --- Title ---
        title_el = soup.select_one('h1')
        book_title = title_el.get_text(strip=True) if title_el else None

        # --- Price ---
        price_el = soup.select_one('p.price_color')
        price = price_el.get_text(strip=True) if price_el else None

        # --- Star Rating ---
        # Rating is stored as a word in the class name e.g. "star-rating Three"
        rating_el = soup.select_one('p.star-rating')
        star_rating = rating_el['class'][1] if rating_el else None  # gets the word e.g. "Three"

        # --- Availability ---
        availability_el = soup.select_one('p.availability')
        availability = availability_el.get_text(strip=True) if availability_el else None

        # --- Product details table (UPC, Tax, Stock count etc.) ---
        upc = None
        product_type = None
        num_reviews = None

        rows = soup.select('table.table-striped tr')
        for row in rows:
            header = row.select_one('th')
            value = row.select_one('td')
            if header and value:
                if 'UPC' in header.text:
                    upc = value.text.strip()
                if 'Product Type' in header.text:
                    product_type = value.text.strip()
                if 'Number of reviews' in header.text:
                    num_reviews = value.text.strip()

        # --- Description ---
        description_el = soup.select_one('article.product_page > p')
        description = description_el.get_text(strip=True) if description_el else None

        # Small random delay to be polite to the server
        time.sleep(random.uniform(1.0, 2.5))

        return {
            "UPC": upc,
            "Title": book_title,
            "Product Type": product_type,
            "Price": price,
            "Star Rating": star_rating,
            "Availability": availability,
            "Number of Reviews": num_reviews,
            "Description": description,
            "URL": url
        }

    except Exception as e:
        print(f"[FAILED] {url}\n  Reason: {e}\n")
        return {"URL": url, "Error": str(e)}

#%%time
# Step 1: Scrape all book URLs from the catalogue pages
def get_all_book_urls(base_url="https://books.toscrape.com/catalogue/category/books/sequential-art_5/"):
    book_urls = []
    page = 1

    while True:
        # Each page follows this pattern
        if page == 1:
            url = base_url + "index.html"
        else:
            url = base_url + f"page-{page}.html"
        response = send_get_request(url)

        # If we get a bad response, we've gone past the last page
        if response.status_code != 200:
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        books = soup.select('article.product_pod h3 a')

        # Build the full URL for each book
        for book in books:
            href = book.get('href', '')
            full_url = "https://books.toscrape.com/catalogue/" + str(book['href']).replace('../', '')
            book_urls.append(full_url)

        print(f"[PAGE {page}] Found {len(books)} books")

        # Check if there's a next page
        next_btn = soup.select_one('li.next a')
        if not next_btn:
            break

        page += 1
        time.sleep(random.uniform(0.5, 1.5))

    return book_urls

#%%time
if __name__ == "__main__":

    # First collect all book URLs from the catalogue
    print("Collecting book URLs...")
    book_urls = get_all_book_urls()
    print(f"\nFound {len(book_urls)} books total. Starting scrape...\n")

    results = []

    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_book_information, url): url for url in book_urls}

        for future in as_completed(futures):
            url = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"[OK] {result.get('Title', url)}")
            except Exception as e:
                print(f"[ERROR] {url}: {e}")
                results.append({"URL": url, "Error": str(e)})

    df = pd.DataFrame(results)
    df.to_csv("../data/raw/raw_data_multiprocessing.csv", index=False)
    print(f"\nDone. {len(df)} rows saved.")