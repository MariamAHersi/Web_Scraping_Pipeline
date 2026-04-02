#!/usr/bin/env python
# coding: utf-8

# # Ingestion - Web Scraping
# Each book has been extracted from https://www.packtpub.com/en-gb/search?q=data%20engineering

# In[ ]:


# Import modules
import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ProcessPoolExecutor


# In[ ]:


# Function to send a GET request to a URL
# Parameters are
#   url: the book URL
#   user_agent_version: version of the browser user agent
#   browser: web browser being used (e.g. chrome)
# Output is the response showing whether the GET request worked (e.g. 200)
def send_get_request(url, user_agent_version, browser):

    # Define the User-Agent string based on browser and version
    if browser.lower() == 'chrome':
        user_agent = f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{user_agent_version}.0.0.0 Safari/537.36'
    elif browser.lower() == 'edge':
        user_agent = f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{user_agent_version}.0.0.0 Safari/537.36 Edg/{user_agent_version}.0.0.0'
    else:
        raise ValueError("Unsupported browser. Try using Chrome or Edge.")

    # Headers dictionary for information about the device
    headers = {
        'User-Agent': user_agent,
        'Accept-Language': 'en-US, en;q=0.5'
    }

    # Send the GET request with the user's headers
    request_response = requests.get(url, headers=headers)

    return request_response


# In[ ]:


# Function to extract the book information using web scraping
# Parameters are
#   url: the book URL
# Output is the book details as a dictionary
def get_book_information(url):

    # Send get request for the url using send_get_request() function
    # Use agent info from https://www.whatismybrowser.com/detect/what-is-my-user-agent/#google_vignette
    request_response = send_get_request(url, 132, 'Chrome')
    # Use HTML parser to extract and look through data from the HTML structure
    soup = BeautifulSoup(request_response.content, 'html.parser')

    # Get title
    title_element = soup.select_one('h1.product-title')
    book_title = title_element.get_text(strip=True) if title_element else None

    # Get author(s) name
    authors = soup.select('div.authors')
    # Extract and clean up
    author_names = [author.get_text(", ", strip=True) for author in authors]
     # Remove duplicates
    author_names = set(author_names)
    # Create string from author name(s)
    author_names = ", ".join(author_names)
   
    # Get year of publication
    # Find all publication details
    details = soup.select('div.product-details-section-content')
    # Extract the full publication date
    publication_date = None

    # Get ISBN and publication date
    isbn = None
    for detail in details:
      key = detail.select_one('span.product-details-section-key')
      value = detail.select_one('span.product-details-section-value')
      if key and value:
        # Extract publication date
        if 'Publication date' in key.text:
          # Format date string
          publication_date = value.text.strip()
          # Convert date string to HTML date format
          publication_date = pd.to_datetime(publication_date, format="%b %d, %Y").date()
        # Get ISBN
        if 'ISBN-13' in key.text:
          isbn = value.text.strip()

    # Get star rating
    rating = soup.select('span.star-rating-total-rating-medium')
    # Extract only the main rating
    star_rating = None
    for rating in rating:
        text = rating.get_text(strip=True)
        # Ensure we get only single-number ratings (avoid "4.6 (18)" format)
        if "(" not in text:
            star_rating = text
            # Stop at the first valid rating
            break

    # Get total number of star ratings
    num_of_ratings = soup.select('span.star-rating-total-count.size-medium')
    # Clean up output
    if num_of_ratings:
        num_of_ratings = list(set([rating.get_text(strip=True).strip('()').split()[0] for rating in num_of_ratings]))[0]
    else:
        # Set to None if not found
        num_of_ratings = None

    # Get format of book
    format_elements = soup.select('span.device-fc-black-1')
    # Extract and clean up
    format = list(set(element.get_text(strip=True) for element in format_elements))
    # Create string from format
    format = ", ".join(format)

    # Get price of paperback book
    prices = soup.select('span.product-details-price-tab-formattedSpecialPrice')
    # Extract text and clean up
    cleaned_prices = list(dict.fromkeys([price.get_text(strip=True) for price in prices]))  # Removes duplicates
    # Remove £ symbol and convert to float
    price = float(cleaned_prices[1].replace('£', '')) if len(cleaned_prices) > 1 else None

    # Return book details as a dictionary
    return {
        "ISBN": isbn,
        "Title": book_title,
        "Authors": author_names,
        "Publication Date": publication_date,
        "Star Rating": star_rating,
        "Number of Ratings": num_of_ratings,
        "Price": price,
        "Format": format,
        "URL": url
    }


# In[ ]:


# Define list of all book URLs to be scraped
book_urls = [
    # paperbooks
    "https://www.packtpub.com/en-gb/product/data-engineering-with-databricks-cookbook-9781837633357",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-aws-9781804614426",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-dbt-9781803246284",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-python-9781839214189",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-google-cloud-platform-9781835080115",
    "https://www.packtpub.com/en-gb/product/cracking-the-data-engineering-interview-9781837630776",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-apache-spark-delta-lake-and-lakehouse-9781801077743",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-aws-cookbook-9781805127284",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-scala-and-spark-9781804612583",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-aws-9781800560413",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-alteryx-9781803236483",
    "https://www.packtpub.com/en-gb/product/data-observability-for-data-engineering-9781804616024",
    "https://www.packtpub.com/en-gb/product/azure-data-engineering-cookbook-9781803246789",
    "https://www.packtpub.com/en-gb/product/azure-data-engineering-cookbook-9781800206557",
    "https://www.packtpub.com/en-gb/product/simplifying-data-engineering-and-analytics-with-delta-9781801814867",
    "https://www.packtpub.com/en-gb/product/big-data-on-kubernetes-9781835462140",
    "https://www.packtpub.com/en-gb/product/managing-data-as-a-product-9781835468531",
    "https://www.packtpub.com/en-gb/product/comptia-casp-cas-004-certification-guide-9781801816779",
    "https://www.packtpub.com/en-gb/product/automated-machine-learning-on-aws-9781801811828",
    "https://www.packtpub.com/en-gb/product/go-recipes-for-developers-9781835464397",
    "https://www.packtpub.com/en-gb/product/data-contracts-in-practice-9781836209157",
    "https://www.packtpub.com/en-gb/product/hands-on-simulation-modeling-with-python-9781838985097",
    "https://www.packtpub.com/en-gb/product/the-machine-learning-solutions-architect-handbook-9781801072168",
    "https://www.packtpub.com/en-gb/product/hands-on-penetration-testing-with-python-9781788990820",
    "https://www.packtpub.com/en-gb/product/mastering-kali-linux-for-advanced-penetration-testing-9781789340563",
    "https://www.packtpub.com/en-gb/product/automated-machine-learning-9781800567689",
    "https://www.packtpub.com/en-gb/product/machine-learning-with-lightgbm-and-python-9781800564749",
    "https://www.packtpub.com/en-gb/product/dynamodb-cookbook-9781784393755",
    "https://www.packtpub.com/en-gb/product/kibana-8x-a-quick-start-guide-to-data-analysis-9781803232164",
    "https://www.packtpub.com/en-gb/product/the-self-taught-cloud-computing-engineer-9781805123705",

    # ebooks
    "https://www.packtpub.com/en-gb/product/data-engineering-with-databricks-cookbook-9781837632060",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-aws-9781804613139",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-dbt-9781803241883",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-python-9781839212307",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-google-cloud-platform-9781835085363",
    "https://www.packtpub.com/en-gb/product/cracking-the-data-engineering-interview-9781837631070",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-apache-spark-delta-lake-and-lakehouse-9781801074322",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-aws-cookbook-9781805126850",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-scala-and-spark-9781804614327",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-aws-9781800569041",
    "https://www.packtpub.com/en-gb/product/data-engineering-with-alteryx-9781803231983",
    "https://www.packtpub.com/en-gb/product/data-observability-for-data-engineering-9781804612095",
    "https://www.packtpub.com/en-gb/product/azure-data-engineering-cookbook-9781803235004",
    "https://www.packtpub.com/en-gb/product/azure-data-engineering-cookbook-9781800201545",
    "https://www.packtpub.com/en-gb/product/simplifying-data-engineering-and-analytics-with-delta-9781801810715",
    "https://www.packtpub.com/en-gb/product/big-data-on-kubernetes-9781835468999",
    "https://www.packtpub.com/en-gb/product/managing-data-as-a-product-9781835469378",
    "https://www.packtpub.com/en-gb/product/comptia-casp-cas-004-certification-guide-9781801814485",
    "https://www.packtpub.com/en-gb/product/automated-machine-learning-on-aws-9781801814522",
    "https://www.packtpub.com/en-gb/product/go-recipes-for-developers-9781835464786",
    "https://www.packtpub.com/en-gb/product/data-contracts-in-practice-9781836209140",
    "https://www.packtpub.com/en-gb/product/hands-on-simulation-modeling-with-python-9781838988654",
    "https://www.packtpub.com/en-gb/product/the-machine-learning-solutions-architect-handbook-9781801070416",
    "https://www.packtpub.com/en-gb/product/hands-on-penetration-testing-with-python-9781788999465",
    "https://www.packtpub.com/en-gb/product/mastering-kali-linux-for-advanced-penetration-testing-9781789340617",
    "https://www.packtpub.com/en-gb/product/automated-machine-learning-9781800565524",
    "https://www.packtpub.com/en-gb/product/dynamodb-cookbook-9781784391096",
    "https://www.packtpub.com/en-gb/product/kibana-8x-a-quick-start-guide-to-data-analysis-9781803244051",
    "https://www.packtpub.com/en-gb/product/the-self-taught-cloud-computing-engineer-9781805128687"
    ]


# In[ ]:


# Empty list to store book dictionaries
book_info_list = []

# Use ProcessPoolExecutor to speed up scraping using multiprocessing
# Instead of fetching book details one at a time, a pool of worker processes is created
# Each process runs independently and executes the get_book_information() function on different book URLs in parallel
# Unlike multithreading, each process runs in its own memory space
# The parameter max_workers=10 means that up to 10 separate processes can be created to allow for parallel execution
with ProcessPoolExecutor(max_workers=10) as executor:
    # executor.map(get_book_information, book_urls) distributes the URLs among the threads and collects the results
    book_info_list = list(executor.map(get_book_information, book_urls))

# Convert list of dictionaries to pandas DataFrame
df = pd.DataFrame(book_info_list)

# Convert to csv file
df.to_csv("engineering_books_from_Packt.csv", index=False)

