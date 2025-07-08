import os
import time
import json
from math import ceil
from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict

import requests
from scrapy import Spider, Request

"""
Scrapy spider to scrape product details from the Ted Baker Saudi Arabia website.

This spider extracts product data, including titles, prices, stock availability, descriptions, images, sizes, and variations.  
It navigates through categories, handles pagination, and retrieves detailed product information using API requests.  
The scraped data is stored in a structured JSON file.

Key Features:
- Extracts product details from category pages and API responses.
- Handles pagination and dynamically constructs API requests.
- Captures product metadata, including images, sizes, and stock status.
- Logs scraping statistics and execution details.
"""


class TedbakerSpider(Spider):
    name = "TedBaker"
    current_dt = datetime.now().strftime("%d%m%Y%H%M")
    start_urls = ['https://tedbaker.sa']

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

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'priority': 'u=0, i',
        'referer': 'https://tedbaker.sa',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    json_headers = {
                'accept': '*/*',
                'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
                'pragma': 'no-cache',
                'referer': 'https://tedbaker.sa/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            }

    def __init__(self):
        super().__init__()
        self.item_found = 0
        self.item_scraped = 0

        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

    def parse(self, response, **kwargs):
        menu_items = response.css('.c-header-navigation__sublink.c-header-navigation__sublink--level--3')
        for menu in menu_items:
            url = menu.css('a::attr(href)').get('')
            url = urljoin(response.url, url)
            sub_cat = menu.css('a::text').get('')
            yield Request(url, callback=self.parse_category, meta={'sub_cat':sub_cat})

    def parse_category(self, response):
        category = response.meta.get('sub_cat', '')
        try:
            total_products = response.css('section .js-boost-items-count ::text').re_first(r'\d[\d,]*')
            self.write_logs(f"Category: {category.title()} | Total Items Found: {total_products}")
            self.item_found += int(total_products)
        except Exception as e:
            total_products = 0
            self.write_logs(f'Error in Parse Function:{e} URL:{response.url}')

        total_pages = ceil(int(total_products)/24)
        for page_no in range(2, total_pages + 1):
            collection_scope = response.css('#__st ::text').get('').split('"collection","rid":')[1].replace('};', '')
            # data = self.form_data(page_no, collection_scope)

            params = self.get_params(page_no, collection_scope)
            res = requests.get('https://services.mybcapps.com/bc-sf-filter/filter', params=params, headers=self.headers)
            if res.status_code==200:
                response_text = res.text
                j_string = response_text.split('BoostPFSFilterCallback(')[1].replace(');', '')
                data_dict = json.loads(j_string)
                products = data_dict.get('products', [])
                for product in products:
                    url = product.get('handle', '')
                    if url:
                        p_url = f'https://tedbaker.sa/products/{url}'
                        response.meta['product'] = product
                        yield Request(url=p_url, callback=self.parse_product_details,
                                      headers=self.headers, meta=response.meta)

        yield from self.parse_pagination(response)

    def parse_pagination(self, response):
        try:
            products_tag = response.css('#web-pixels-manager-setup ::text').get('').split('collection_viewed", ')[1].replace('\\', '')
            pro = json.loads(products_tag.split(');},')[0])
            products = pro.get('collection', {}).get('productVariants', [])
        except json.JSONDecodeError as e:
            products= []
            self.write_logs(f'Error in Product Dictionary:{e}  URL{response.url}')

        for product in products:
            url = product.get('product', {}).get('url', '')
            p_url = f'https://tedbaker.sa{url}'
            response.meta['product']= product
            yield Request(p_url, callback=self.parse_product_details, headers=self.headers, meta=response.meta)

    def parse_product_details(self, response):
        product = response.meta.get('product', {})
        cat= response.meta.get('sub_cat', '')
        cat = cat.title() if cat else ''

        try:
            data_dict = response.css('script[type="application/ld+json"]:contains(": \\"Product")::text').get('')
            data_dict = data_dict.replace('\t', '').replace('" / ', '')
            data_dict = json.loads(data_dict)
        except:
            data_dict = {}
            return

        item = OrderedDict()
        item['source_id'] = 'TedBaker'
        item['brand'] = product.get('product', {}).get('vendor', '')
        item['product_title'] = product.get('product', {}).get('title', '')
        item['category'] = cat
        item['description'] = data_dict.get('description', '')
        item['main_image_url'] = response.css('meta[property="og:image:secure_url"] ::attr(content)').get('')
        item['other_image_urls'] = self.get_image(response)
        item['number_of_items_in_stock'] = 0
        item['last_update'] = ''
        item['creation_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        variations = data_dict.get('offers', [])
        for variation in variations:
            try:
                availability = variation.get('availability', '')
                previous_price = response.css('.o-product-form-1__price--old ::text').re_first(r'\d[\d,]*')
                previous_price = previous_price.replace(',', '') if previous_price else '0'
                current_price = variation.get('price', '')
                discount = int(previous_price) - float(current_price) if int(previous_price) > float(
                    current_price) else 0

                item['product_id'] = variation.get('sku', '')
                item['price'] = float(current_price) if current_price else 0.0
                item['discount'] = float(discount) if discount else 0.0
                item['currency'] = variation.get('priceCurrency', '')
                item['colors'] = [variation.get('color', '')] or []
                item['variations'] = {}
                item['sizes'] = self.get_sizes(variation)
                item['other_details'] = {}
                item['availability'] = 'in_stock' if 'InStock' in availability else 'out_of_stock'
                item['product_url'] = variation.get('url', '')
                self.item_scraped += 1
                yield item
                # self.current_records.append(item)
            except Exception as e:
                # print(f'Error parsing item:{e} URL:{response.url}')
                self.item_scraped += 1
                yield item

    def price_format(self, price):
        try:
            price = str(int(price))
        except:
            price = str(price)

        return price

    def get_image(self, response):
        unique_images = set()

        # Extract all images from meta tags with 'og:image'
        og_images = response.css('meta[property="og:image"]::attr(content)').getall()
        if og_images:
            v_id = next((''.join(img.split('v=')[1:2]) for img in og_images if 'v=' in img), None)
            unique_images.update(og_images)

        # Extract lazy-loaded images
        lazy_images = response.css('img.lazyload::attr(data-src)').getall()
        if lazy_images:
            filtered_lazy_images = [img for img in lazy_images if v_id and v_id in img]
            base_url = "https:"
            for img in filtered_lazy_images:
                if img.startswith("//"):
                    full_image_url = f"{base_url}{img}"
                else:
                    full_image_url = img
                unique_images.add(full_image_url)

        return list(unique_images)

    def get_sizes(self,variation):
        value = variation.get('size', '').lower()
        keys = ['uk', 'eu', 'us']
        if value:
            info = {key: value for key in keys if key in value} or {'us': value}
            return info
        else:
            return {}

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def get_params(self, page_no, collection_scope):
        timestamp_seconds = int(time.time())
        timestamp_milliseconds = timestamp_seconds * 1000
        params = {
            't': timestamp_milliseconds,
            '_': 'pf',
            'shop': 'ted-baker-ksa.myshopify.com',
            'page': str(page_no),
            'limit': '24',
            'sort': 'created-descending',
            'display': 'grid',
            'collection_scope': str(collection_scope),
            'tag': '',
            'product_available': 'true',
            'variant_available': 'true',
            'build_filter_tree': 'true',
            'check_cache': 'true',
            'sort_first': 'available',
            'locale': 'en',
            'callback': 'BoostPFSFilterCallback',
            'event_type': 'init',
        }
        return params

    def close(spider, reason):
        # Log overall scraping statistics
        spider.write_logs(f"\n--- Scraping Summary ---")
        spider.write_logs(f"Total Products Available on Website: {spider.item_found}")
        spider.write_logs(f"Total Products Successfully Scraped: {spider.item_scraped}")

        # Log script execution times
        spider.write_logs(f"\n--- Script Execution Times ---")
        spider.write_logs(f"Script Start Time: {spider.script_starting_datetime}")
        spider.write_logs(f"Script End Time: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        spider.write_logs(f"Reason for Closure: {reason}")
