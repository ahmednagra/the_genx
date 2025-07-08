import os
import random
import re
import json
from datetime import datetime
from urllib.parse import quote

import requests
from scrapy import Spider, Request

"""
LuxuryClosetSpider

This spider is designed to scrape product details from The Luxury Closet website, an online luxury fashion marketplace. It extracts information such as product titles, prices, descriptions, sizes, and availability across multiple categories like women, men, and accessories.

The spider starts by making a request to the category API to get a list of available categories and sub-categories. For each category, the spider requests a list of products and then extracts the detailed information for each product by making individual requests.

Key Features:
- Scrapes product details such as title, brand, price, description, images, and sizes.
- Supports pagination to navigate through multiple pages of product listings.
- Handles product details extraction, including nested lists for sizes and other attributes.
- Uses Scrapy's retry mechanism for robust scraping, retrying failed requests up to 5 times.
- Logs the start time, scraping statistics, and any errors encountered during the scraping process.
- Outputs scraped data in JSON format with specific fields including product ID, URL, title, and more.
- Playwright integration is used to extract pricing information from dynamic product pages.

Custom Settings:
- Limits concurrent requests to 3 to avoid overloading the server.
- Retries failed requests with specified HTTP error codes.
- Output is saved as JSON with proper indentation and UTF-8 encoding, with timestamped filenames.

To run this spider, simply invoke it using Scrapy's `crawl` command. The spider will generate logs for tracking and store the scraped product details in the specified output directory.

"""

class LuxuryClosetSpider(Spider):
    name = "luxuryCloset"
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    # Scrapy Custom Settings
    custom_settings = {
        # "CONCURRENT_REQUESTS": 3,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'DOWNLOAD_TIMEOUT': 70,
        'FEEDS': {
            f'output/{name} Products Details {current_dt}.json': {
                'format': 'json',
                'encoding': 'utf8',
                'indent': 4,
                'fields': [
                    'source_id', 'product_url', 'brand', 'product_title', 'product_id', 'category', 'price', 'discount',
                    'currency', 'description', 'main_image_url', 'other_image_urls', 'colors', 'variations',
                    'sizes', 'other_details', 'availability', 'number_of_items_in_stock', 'last_update', 'creation_date'
                ]
            }
        },
    }

    us_headers = {
    'accept': '*/*',
    'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
    'cache-control': 'no-cache',
    'content-country': 'US',
    'content-language': 'en',
    'content-type': 'application/json',
    'platform': 'desktop',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://theluxurycloset.com/us-en/women/chanel-pink-gold-strips-pearl-cc-resin-bangle-s-p5430',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
}

    json_headers = {
        'accept': '*/*',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'no-cache',
        'content-country': 'AE',
        'content-language': 'en',
        'content-type': 'application/json',
        'platform': 'desktop',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    }
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'priority': 'u=0, i',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    }


    def __init__(self):
        super().__init__()
        self.items_found = 0
        self.items_scraped = 0

        # Create directories for logs and output
        os.makedirs("output", exist_ok=True)
        os.makedirs("logs", exist_ok=True)

        # Logs
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

    def start_requests(self):
        """Start the spider by requesting category API."""
        # API Endpoints
        cat_api_url = "https://theluxurycloset.com/wapi/api/web/v3/menus?platform=D&type=top&dynamic_header=1&language=en&country=AE"
        yield Request(url=cat_api_url, headers=self.json_headers, callback=self.parse)

    def parse(self, response, **kwargs):
        """Parse categories and initiate product listing requests."""
        try:
            data = json.loads(response.text)
            menu_items = data.get("data", {}).get("menu", [])
            categories_info = set()  # Use a set to ensure uniqueness

            for category in menu_items:
                title = category.get('name', '').strip()
                skip_categories = ['Authenticity']
                if any(text in title for text in skip_categories):
                    continue

                sub_categories = category.get('children', {})
                if sub_categories:
                    for sub_category in sub_categories:
                        sub_title = sub_category.get('name', '').strip()
                        sub_sub_categories = sub_category.get('children', {})
                        if sub_sub_categories:
                            for sub_sub_category in sub_sub_categories:
                                sub_sub_title = sub_sub_category.get('name', '').strip()
                                sub_sub_url = sub_sub_category.get('url', '')

                                # Reset title for each level of category
                                title_combined = f'{title}, {sub_title}, {sub_sub_title}'
                                categories_info.add((title_combined, sub_sub_url))

            for category_title, category_url in categories_info:
                api_url = self.get_api_url(category_url, page_no=1)
                self.json_headers['referer'] = api_url
                yield Request(url=api_url, headers=self.json_headers, callback=self.parse_product_list, dont_filter=True,
                              meta={'page_num': 1, 'category_url': category_url, 'category_title':category_title})
        except Exception as e:
            self.write_logs(f"[ERROR] Failed to parse categories: {e}")

    def parse_product_list(self, response):
        """Parse product listing and extract product URLs."""
        try:
            data = json.loads(response.text)
        except Exception as e:
            self.write_logs(f"[ERROR] Failed to parse product list: {e}")
            return

        products = data.get("data", {}).get("items", [])

        total_products = data.get('data', {}).get('_meta', {}).get('totalCount', '')
        category_name = response.meta.get('category_title', '')
        self.items_found += int(total_products)
        self.write_logs(f'Category: {category_name} Has Total records: {total_products}')

        for product in products:
            product_url = product.get('web_link', '')
            api_url = self.construct_api_url(product_url)
            if api_url:
                yield Request(url=api_url, headers=self.us_headers, callback=self.parse_product_details, meta={'product_url':product_url})


        # Handle pagination and request the next page if available
        total_pages = data.get('data', {}).get('_meta', {}).get('pageCount', 0)
        category_url = response.meta.get('category_url', '')

        for page_no in range(2, total_pages+1):
            api_url = self.get_api_url(category_url, page_no=page_no)
            self.json_headers['referer'] = api_url
            yield Request(url=api_url, headers=self.json_headers, callback=self.pagination,
                          meta={'page_num': page_no, 'category_url': category_url})

    def pagination(self, response):
        try:
            data = json.loads(response.text)
        except Exception as e:
            self.write_logs(f"[ERROR] Failed to parse product list: {e}")
            return

        products = data.get("data", {}).get("items", [])
        for product in products:
            product_url = product.get('web_link', '')
            api_url = self.construct_api_url(product_url)
            if api_url:
                yield Request(url=api_url, headers=self.us_headers, callback=self.parse_product_details, meta={'product_url':product_url})


    def parse_product_details(self, response):
        """Extract product details."""
        product_url = response.meta.get('product_url', '')
        try:
            data = json.loads(response.text)
            product_data = data.get("data", {})
            product_info = product_data.get("product_info", {})
            images = product_data.get("images", [])
            category = ", ".join(filter(None, [product_data.get("category_level_one_name"),
                                               product_data.get("category_level_two_name"),
                                               product_data.get("category_level_three_name")]))

            size_attr = product_info.get("size_attr", [])
            sizes = {"us": []}  # Initialize with 'us' as the main key

            for s_item in size_attr:
                if isinstance(s_item, list):  # Handling nested lists
                    for sub_item in s_item:
                        key = sub_item["name"].lower().replace(" ", "_")
                        sizes["us"].append({key: sub_item["value"]})  # Append dictionary to 'us' list
                elif isinstance(s_item, dict):
                    key = s_item["name"].lower().replace(" ", "_")
                    sizes["us"].append({key: s_item["value"]})  # Append dictionary to 'us' list

            if not sizes.get('us', []):
                sizes = {}

            other_data = product_info.get("other_attr", [])
            other_attr = {}
            for item in other_data:
                if isinstance(item, list):  # Handling nested lists
                    for sub_item in item:
                        key = sub_item["name"].lower().replace(" ", "_")
                        other_attr[key] = sub_item["value"]
                elif isinstance(item, dict):
                    key = item["name"].lower().replace(" ", "_")
                    other_attr[key] = item["value"]

            p_url = product_data.get("web_link", "").strip()
            price = product_data.get('price_tlc_multi_country', 0.0)
            price = float(round(price)) if price is not None else 0.0
            discounted_amount = product_data.get('promoted_vouchers', {})
            if discounted_amount:
                discounted_amount = discounted_amount.get('amount', 0)
            else:
                discounted_amount = 0

            # Extracting required fields
            item = {
                'source_id': self.name,
                "product_id": str(product_data.get("id", "")),
                "product_url": p_url,
                "product_title": product_data.get("name", ""),
                "brand": product_data.get("brand_name", ""),
                "category": category,
                "currency": "$",
                "price": price,
                "discount": float(discounted_amount),
                "description": product_info.get("description", ""),
                "main_image_url": images[0]["url"] if images else "",
                "other_image_urls": [img["url"] for img in images[1:]] if len(images) > 1 else [],
                "colors": [product_data.get("colour_name", "")] if product_data.get("colour_name") else [],
                "variations": {},
                "sizes": sizes,
                "other_details": other_attr,
                "availability": "in_stock" if product_data.get("current_status",
                                                               "").lower() == "available" else "out_of_stock",
                "number_of_items_in_stock": None,  # No direct stock quantity provided
                "last_update": "",
                "creation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

            self.items_scraped += 1
            yield item
        except Exception as e:
            self.write_logs(f"[ERROR] Failed to parse product details: {e} - URL:{product_url}")

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def get_api_url(self, url, page_no):
        split_url = url.strip('/').split('/')
        c_l_one = ''.join(split_url[0:1]).strip() #c == Category, l== level
        c_l_two = ''.join(split_url[1:2]).strip()
        c_l_three = ''.join(split_url[2:3]).strip()
        api_url = f'https://theluxurycloset.com/wapi/api/web/v3/products?category_level_one_ids={c_l_one}&category_level_three_ids={c_l_three}&category_level_two_ids={c_l_two}&initial_params=category_level_one_ids%3D{c_l_one}%23category_level_three_ids%3D{c_l_three}%23category_level_two_ids%3D{c_l_two}&page={str(page_no)}&per-page=60&is_seo=1&countryCode=AE&ctr_ranking=0&collection_ranking=1&language=en&country=AE'
        return api_url

    @staticmethod
    def construct_api_url(product_url):
        """Helper method to construct API URL from a given product URL."""
        if not product_url:
            return None

        # Extract product ID (last numeric sequence)
        match = re.search(r'(\d+)$', product_url)
        if not match:
            return None  # Return None if no product ID is found

        product_id = match.group(1)

        # Extract alias (path after domain)
        alias = product_url.split('.com/')[-1]

        # Encode alias for API format
        encoded_alias = quote(alias, safe='')

        # Construct and return API URL
        # return f"https://theluxurycloset.com/wapi/api/web/v3/products/{product_id}?&countryCode=AE&alias={encoded_alias}&language=en&country=AE"
        return f"https://theluxurycloset.com/wapi/api/web/v3/products/{product_id}?&countryCode=US&alias={encoded_alias}&country=US"

    def close(spider, reason):
        # Log overall scraping statistics
        spider.write_logs(f"\n--- Scraping Summary ---")
        spider.write_logs(f"Total Products Available on Website: {spider.items_found}")
        spider.write_logs(f"Total Products Successfully Scraped: {spider.items_scraped}")

        # Log script execution times
        spider.write_logs(f"\n--- Script Execution Times ---")
        spider.write_logs(f"Script Start Time: {spider.script_starting_datetime}")
        spider.write_logs(f"Script End Time: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        spider.write_logs(f"Reason for Closure: {reason}")
