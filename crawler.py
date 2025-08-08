from bs4 import BeautifulSoup
from requests import RequestException
from dotenv import load_dotenv
from pymongo import MongoClient
from lxml import html
import requests
import logging
import json
import os
import asyncio
import aiohttp


class BookCrawler:
    baseUrl = "https://books.toscrape.com/"
    categories_set = set()

    collected_books = []
    parsed_books = []


    # Logging config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    def __init__(self):
        self.checkUrl()  # Check site availability
        self.crawlerBookCategories()  # Fetch categories

        load_dotenv() # Load Env
        self.mongo_user = os.getenv("MONGO_USERNAME")
        self.mongo_pass = os.getenv("MONGO_PASSWORD")
        self.mongo_host = os.getenv("MONGO_HOST")
        self.mongo_port = os.getenv("MONGO_PORT")

        self.conn_string = f"mongodb://{self.mongo_user}:{self.mongo_pass}@{self.mongo_host}:{self.mongo_port}/"

        self.client = None
        self.db = None

    def checkUrl(self):
        # Test connection to the website
        try:
            response = requests.get(self.baseUrl, timeout=2)
            response.raise_for_status()
            logging.info(f"[OK] {self.baseUrl} [{response.status_code}]")
        except RequestException:
            logging.warning(f"[NOT 200] {self.baseUrl}")

    def crawlerBookCategories(self):
        # Get category links from homepage
        response = requests.get(self.baseUrl, timeout=2)
        if not response.text.strip():
            raise Exception("Website data is empty!")

        bsSoup = BeautifulSoup(response.text, 'html.parser')
        booksCategories = bsSoup.find("ul", attrs={
            'class': 'nav nav-list'
        }).find('li').find_all("a")

        if not booksCategories:
            logging.warning("[ERROR] Categories not found!")
            raise Exception("No categories collected!")

        for categories in booksCategories:
            categoriesHref = categories.get('href')
            categoriesName = categoriesHref.split('/')[3].split('_')[0].capitalize()

            if not categoriesHref in "catalogue/category/books_1/index.html":
                self.categories_set.add(
                    (categoriesName, f"{self.baseUrl + categoriesHref}"))

        #(For Debug)print(self.categories_set)
        asyncio.run(self.crawlerBooks(self.categories_set))
   

    # Collect All Books In Categories
    async def crawlerBooks(self, categories):
        bookData = []

        try:
            async with aiohttp.ClientSession() as session:
                tasks = [self.fetchBookUrl(session, categoryUrl) for _, categoryUrl in categories]
                results = await asyncio.gather(*tasks)

                for i, html in enumerate(results):
                    categoryName = list(categories)[i][0]

                    if not html:
                        logging.warning(f"[WARN] Empty response for category {categoryName}")
                        continue

                    soup = BeautifulSoup(html, 'html.parser')
                    bookList = soup.find('ol', attrs={'class': 'row'}).find_all('li')

                    temp_list = []
                    for book in bookList:
                        link = book.find('h3').find('a').get('href').replace('../../../', f'{self.baseUrl}catalogue/')
                        temp_list.append(link)

                    bookData.append({
                        "category_name": categoryName,
                        "books": temp_list
                    })
                # Contiune With Book Parser
                self.parseBooks(bookData)

        except Exception as exError:
            logging.error("[ERROR] Books data is not extracted!")

    def parseBooks(self,booksList):
        try:
            for bookCategory in booksList:
                categoryName = bookCategory['category_name']
                books = bookCategory['books']

                for bookUrl in books:
                    response = requests.get(bookUrl, timeout=1)
                    print(f"Fetching book: {bookUrl}")  # For Debug
                    if response.status_code != 200:
                        logging.error(f"[ERROR] Failed to fetch {bookUrl} - Status: {response.status_code}")
                        continue

                    bookHtml = html.fromstring(response.content)
                    bookTitle = bookHtml.xpath('//article//h1/text()')
                    bookDescription = bookHtml.xpath('//*[@id="content_inner"]/article/p/text()')
                    bookPrice = bookHtml.xpath('//*[@id="content_inner"]/article/div[1]/div[2]/p[1]/text()')
                    bookAvailable = bookHtml.xpath('//th[text()="Availability"]/following-sibling::td/text()')
                    bookPicture = bookHtml.xpath('//div[@id="product_gallery"]//img/@src')
                    bookUPCID = bookHtml.xpath('//th[text()="UPC"]/following-sibling::td/text()')


                    self.collected_books.append({
                        "title": bookTitle,
                        "price": bookPrice,
                        "description": bookDescription,
                        "upc": bookUPCID if bookUPCID else "null",
                        "availability": bookAvailable,
                        "picture": f"{self.baseUrl}{bookPicture[0]}" if bookPicture else "null",
                        "url": bookUrl,
                        "category": categoryName,
                    })
            logging.info(f"[SUCCESS] Parsed {len(self.collected_books)} books.")
            print(self.collected_books)  # For Debug
        except Exception as e:
            logging.error(f"[ERROR] Parsing books failed: {str(e)}")

    
    async def fetchBookUrl(self, session, url):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    logging.info(f"*********[SUCCESS] Book Fetched: {url} *********")
                    return html
                else:
                    logging.error(f"[ERROR] {url} responded with status {response.status}")
                    return None
        except Exception as e:
            logging.error(f"[ERROR] Fetch failed for {url} - {str(e)}")
            return None


if __name__ == "__main__":
    app = BookCrawler()