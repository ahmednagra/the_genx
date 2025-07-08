import os
import json
import time
from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict

import requests
from scrapy import Spider, Request

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


class HmSpider(Spider):
    name = "H&M"
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
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
        'URLLENGTH_LIMIT': 10000,  # Increase the limit beyond 2083'
        "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        'DOWNLOADER_MIDDLEWARES': {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
            "scrapy_poet.InjectionMiddleware": 543,
        },
        'REQUEST_FINGERPRINTER_CLASS': "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        'ZYTE_API_KEY': "f693db95c418475380b0e70954ed0911",
        "ZYTE_API_TRANSPARENT_MODE": True,
    }

    cat_headers = {
        'accept': '*/*',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'content-type': 'application/json',
        'magento-store-code': 'hm_ksa_store',
        'magento-store-view-code': 'sau_en',
        'magento-website-code': 'sau',
        'mesh_context': 'live',
        'mesh_locale': 'en',
        'mesh_market': 'sa',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://sa.hm.com/en/',
        'store': 'sau_en',
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

        # Set up Selenium WebDriver
        # chrome_options = Options()
        # chrome_options.add_argument("--headless")  # Run in headless mode
        # chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        # chrome_options.add_argument("--no-sandbox")
        # chrome_options.add_argument("--disable-dev-shm-usage")
        #
        # self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

        CHROMIUM_PATH = "/usr/bin/chromium-browser"
        CHROMEDRIVER_PATH = "/usr/bin/chromedriver"

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        service = Service(CHROMEDRIVER_PATH)
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def start_requests(self):
        """Start the spider by requesting category API."""
        cat_api_url = 'https://sa.hm.com/graphql'
        params = self.get_categories_data()
        resp = requests.get(cat_api_url, params=params, headers=self.cat_headers)
        if resp.status_code==200:
            data = resp.json()
            yield Request(url=cat_api_url, headers=self.cat_headers,
                callback=self.parse_categories, meta={"api_data": data})
        else:
            a=1

    def parse_categories(self, response):
        """Parse the response from the category API."""
        try:
            data = response.meta.get("api_data", {}).get('data', {}).get('commerce_categories', {}).get('items', [])[0]
        except json.JSONDecodeError:
            data = {}
            self.write_logs(f"Invalid JSON received")

        menu_items= data.get('children', [])
        categories_info = set()  # Use a set to ensure uniqueness

        for category in menu_items:
            title = category.get('name', '').strip()
            sub_categories =category.get('children', [])

            if sub_categories:
                for sub_category in sub_categories:
                    sub_title = sub_category.get('name', '').strip()
                    sub_sub_categories = sub_category.get('children', [])
                    if sub_sub_categories:
                        for sub_sub_category in sub_sub_categories:
                            sub_sub_title = sub_sub_category.get('name', '').strip()
                            sub_sub_url = sub_sub_category.get('url_path', '')

                            # Reset title for each level of category
                            title_combined = f'{title}, {sub_title}, {sub_sub_title}'
                            categories_info.add((title_combined, sub_sub_url))

        for category_title, category_url in categories_info:
            self.cat_headers['x-algolia-api-key']= 'a2fdc9d456e5e714d8b654dfe1d8aed8'
            self.cat_headers['x-algolia-application-id']= 'HGR051I5XN'
            self.cat_headers['Referer'] = f'https://sa.hm.com/en/{category_url}'
            self.cat_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            url = f'https://sa.hm.com/en/{category_url}'
            yield Request(url=url, callback=self.parse_category_list,
                          meta={'cat_url': url, 'cat_title': category_title})
                          # meta={'cat_url': url, 'cat_title': category_title, "zyte_api": {
                                  # "browserHtml": True  # Ensures JavaScript execution
                              # }})

    def parse_category_list(self, response):
        """Parse product listing and extract product URLs."""
        # product_urls = response.css('.product-item-link ::attr(href)').getall()
        #
        # for product in product_urls:
        #     url = urljoin(response.url, product)
        #     response.meta['handle_httpstatus_all'] = True
        #     yield Request(url=url, callback=self.parse_product_detail, headers=self.cat_headers, meta=response.meta)
        #
        # try:
        #     # Handle pagination and request the next page if available
        #     total_products = response.css('#count-of-found-items-on-main::text').get('').replace('items', '').strip()
        #     self.items_found += int(total_products) if total_products else 0
        #     category_title = response.meta.get('cat_title', '')
        #     self.write_logs(f'Category:{category_title} Found Records: {total_products}')
        # except Exception as e:
        #     self.write_logs(f'Error:{e} in the URL:{response.url}')
        self.driver.get(response.url)
        time.sleep(12)  # Allow an initial page load

        total = 0  # Initialize total to prevent reference issues

        while True:
            try:
                # Extract progress count
                progress_element = self.driver.find_element(By.ID, "progress-count")
                progress_text = progress_element.text if progress_element else ""

                if progress_text:
                    showing, total = map(int, progress_text.replace("showing ", "").replace(" items", "").split(" of "))
                else:
                    self.logger.error("Failed to extract progress count.")
                    break

                # If all products are loaded, break
                if showing >= total:
                    self.logger.info(f"All products loaded: {showing} of {total}")
                    break

                # Click the "Load more" button
                load_more_button = self.driver.find_element(By.CLASS_NAME, "pager-button")
                if load_more_button.is_displayed():
                    self.driver.execute_script("arguments[0].click();", load_more_button)
                    time.sleep(5)  # Delay for data to load
                else:
                    self.logger.info("Load more button not found or not visible.")
                    break

            except Exception as e:
                self.logger.error(f"Error while loading products: {e}")
                break

        # Extract product URLs
        product_urls = [elem.get_attribute("href") for elem in
                        self.driver.find_elements(By.CSS_SELECTOR, ".product-item-link")]

        # Ensure correct count
        self.logger.info(f"Extracted {len(product_urls)} product URLs (Expected: {total})")

        for product in product_urls:
            yield Request(url=product, callback=self.parse_product_detail, headers=self.cat_headers,
                          meta={"zyte_api": {"browserHtml": True}})

        try:
            # Handle pagination and request the next page if available
            # total_products = response.css('#count-of-found-items-on-main::text').get('').replace('items', '').strip()
            total_products_element = self.driver.find_element(By.ID, "count-of-found-items-on-main")
            total_products = total_products_element.text.replace("items", "").strip()

            self.items_found += int(total_products) if total_products else 0
            category_title = response.meta.get('cat_title', '')
            self.write_logs(f'Category:{category_title} Found Records: {total_products}')
        except Exception as e:
            self.write_logs(f'Error:{e} in the URL:{response.url}')

    def pagination(self, response):
        """Response for Next pages then requested detail Page of Product"""
        product_urls = response.css('.product-item-link ::attr(href)').getall()

        for product in product_urls:
            url = urljoin(response.url, product)
            # response.meta['handle_httpstatus_all'] = True
            yield Request(url=url, callback=self.parse_product_detail, headers=self.cat_headers, meta=response.meta)

    def parse_product_detail(self, response):
        try:
            item = OrderedDict()

            # Extract JSON-LD Data
            json_ld_script = response.xpath(
                '//script[@type="application/ld+json" and @data-name="product"]/text()').get()
            if json_ld_script:
                try:
                    product_data = json.loads(json_ld_script)
                    item['source_id'] = 'H&M KSA'
                    item['product_url'] = response.url
                    item['brand'] = product_data.get('brand', {}).get('name', '')
                    item['product_title'] = product_data.get('name', '')
                    item['product_id'] = product_data.get('sku', '')
                    item['description'] = product_data.get('description', '')
                    images = product_data.get('image', [])
                    item['main_image_url'] = images[0] if images else ''
                    item['other_image_urls'] = images[1:] if images else []

                    # Extract price and currency from offers
                    offer = product_data.get('offers', [{}])[0]
                    item['price'] = offer.get('price', '')
                    item['currency'] = offer.get('priceCurrency', '')
                    item['availability'] = 'in_stock' if 'InStock' in offer.get('availability', '') else 'out_of_stock'
                except json.JSONDecodeError:
                    self.logger.error("Error parsing JSON-LD data")

            # Extract Category (if available in breadcrumbs or other metadata)
            categories = response.css('nav.breadcrumbs li a span::text').getall()
            item['category'] = categories[-1] if categories else ''

            # Extract Color Options
            item['colors'] = response.css('div.pdp-swatches-refs a::attr(data-swatch-title)').getall()

            # Extract Available Sizes
            item['sizes'] = {"uk": response.css('div.pdp-swatches__options input::attr(value)').getall()}

            # Extract Discount if any
            discount_text = response.css('span.discount::text').get()
            item['discount'] = discount_text.strip() if discount_text else '0.0'

            # Set Static Fields
            item['other_details'] = {}
            item['number_of_items_in_stock'] = None
            item['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            item['creation_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            self.items_scraped += 1
            yield item
        except Exception as e:
            self.write_logs(f'Error parsing item:{e} URL:{response.url}')

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def get_categories_data(self):
        params = {
            'query': 'query ($categoryId: String) { commerce_categories( filters: { ids: { eq: $categoryId } } include_in_menu: 1 ) { items { error { code message } id name level url_path image promo_id banners prev_prea_enabled preview_sd preview_ed preview_category_text preview_pdp_text preview_tier_type preview_timer preaccess_sd preaccess_ed preaccess_category_text preaccess_pdp_text preaccess_tier_type include_in_menu display_view_all gtm_name breadcrumbs { category_level category_name category_uid category_url_key category_url_path } children { id name level image url_path promo_id banners include_in_menu display_view_all gtm_name children { id name level image url_path promo_id banners include_in_menu display_view_all gtm_name children { id name level image url_path promo_id banners include_in_menu display_view_all gtm_name children { id name level image url_path promo_id banners include_in_menu display_view_all gtm_name } } } } } } }',
            'variables': '{"categoryId":"2"}',
        }
        return params

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

        """Close WebDriver when the spider finishes."""
        spider.driver.quit()

