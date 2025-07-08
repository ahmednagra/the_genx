import os
import re
import json
from math import ceil
from datetime import datetime
from collections import OrderedDict

from scrapy import Spider, Request, Selector

"""
Bloomingdale's UAE Scraper

This Scrapy spider extracts product details from the Bloomingdale's UAE website.
It scrapes product listings by category, retrieves detailed product data,
and paginates through available products efficiently.

### Key Features:
- Extracts product details (title, price, brand, sizes, colors, availability, etc.).
- Uses structured API requests and GraphQL queries to fetch accurate data.
- Implements robust error handling and logging for debugging.
- Supports pagination to ensure full product coverage.
- Avoids duplicate scraping by tracking previously scraped items.

### Data Extracted:
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Images, and More.
"""

class BloomingdalesSpider(Spider):
    name = "BloomingDales"
    start_urls = ["https://bloomingdales.ae"]
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
        }
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'priority': 'u=1, i',
        'referer': 'https://bloomingdales.ae/women.html',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }

    def __init__(self):
        super().__init__()
        self.items_found = 0
        self.items_scraped_count = 0
        self.scraped_items = []

        # Initialize Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

    def parse(self, response, **kwargs):
        """Extracts main product categories and initiates scraping per category."""
        try:
            menu_items = response.css('.b-menu__nav-link')
            for menu_item in menu_items[:1]:
                category = menu_item.css('a::text').get('').strip()
                url = menu_item.css('a::attr(href)').get('')
                menu_url = response.urljoin(url)
                key = url.replace('.html', '').rstrip('/').split('/')[-1]
                yield Request(url=menu_url, callback=self.parse_next, meta={'category':category, 'cat_key':key})
        except Exception as e:
            self.write_logs(f"Error in parse: {e}")

    def parse_next(self, response):
        """Extracts total product count and paginates through products."""
        try:
            category = response.meta.get('category', '')
            category_key = response.meta.get('cat_key', '')

            # Extract total product count and handle empty or invalid cases
            total_products = response.css('.b-product-count ::text').re_first(r'(?<=/)\d[\d,]*')
            total_products = int(total_products.replace(',', '')) if total_products else 0
            self.items_found += total_products
            self.write_logs(f"Category: '{category}' contains {total_products} records.")

            products = response.css('.js-product-brand')
            for product in products:
                product_url = product.css("::attr(href)").get('')
                if product_url and product_url not in self.scraped_items:
                    yield Request(url=response.urljoin(product_url), callback=self.parse_product)
        except Exception as e:
            self.write_logs(f"Error in parse_next: {e}")

        try:
            # Calculate total pages for pagination (getting 12 products per page after the first 48)
            remaining_products = total_products - 48
            total_pages = max(0, ceil(remaining_products / 12))

            # Handle pagination and yield requests for each page
            for page_no in range(1, total_pages + 1):
                page_url = self.get_url(page_no, category_key.lower())
                if page_url:
                    self.headers['referer'] = response.url  # Set referer header to current URL
                    yield Request(url=page_url, callback=self.pagination, headers=self.headers, dont_filter=True, meta=response.meta)

        except Exception as e:
          self.write_logs(f"Error in Pagination at parse_next: {e}")

    def pagination(self, response):
        """Handles pagination and retrieves product links from each page."""
        try:
            product_links = response.css('.blm-producttile__name.js-product-name::attr(href)').getall() or []
            for product in product_links:
                    product_url = response.urljoin(product)
                    if product_url in self.scraped_items:
                        # print('Item Already Scraped so skipped')
                        continue
                    yield Request(url=product_url, callback=self.parse_product, meta=response.meta)
        except Exception as e:
            self.write_logs(f"Error in pagination: {e}")

    def parse_product(self, response):
        """Extracts product details from the product page."""
        current_time = datetime.now()

        try:
            json_data = response.css('script[type="application/ld+json"]::text').get()
            product_data = json.loads(json_data) if json_data else {}

            script_data = response.xpath('//script[contains(text(), "window.dataLayer.push")]/text()')[1].get()
            json_data = re.search(r'\{.*\}', script_data, re.DOTALL)
            data_layer = json.loads(json_data.group(0))
            category = ", ".join(
                data_layer.get('ecommerce', {}).get('detail', {}).get('products', [{}])[0].get('category', "").split(
                    "/"))
        except json.JSONDecodeError as e:
            self.write_logs(f'error in Dictionary :{response.url}')
            return

        try:
            price = response.css('.js-product-prices .blm-price__value::attr(content)').get('')
            discount_price = response.css('.js-product-prices .blm-price__standard .blm-price__value::attr(content)').get('')
            discount = float(discount_price) - float(price) if discount_price and not None else 0.0

            colors = (response.css('.colors a::attr(title)').getall() or
                      response.css('.js-color-label-value::text').getall() or [])
            other_details = {
                "brand": product_data.get("brand", {}).get("name", ""),
                "mpn": product_data.get("mpn", ""),
            }
            availability = product_data.get("offers", {}).get("availability", "")

            sizes = [self.safe_strip(value) for value in response.css('.blm-attribute__value-name::text').getall()] or []
            size_dict = self.extract_sizes(sizes)

            item = OrderedDict()
            url = product_data.get("offers", {}).get("url", "")
            item["source_id"] = "bloomingdales"
            item["product_url"] = url
            item["brand"] = product_data.get("brand", {}).get("name", "")
            item["product_title"] = product_data.get("name", "")
            item["product_id"] = product_data.get("sku", "")
            item["category"] = category
            item["price"] = float(price) if price else 0.0
            item["discount"] = discount if discount else 0.0
            item["currency"] = product_data.get("offers", {}).get("priceCurrency", "SAR")
            item["description"] = self.get_description(product_data)
            item["main_image_url"] =  product_data.get("image", [])[0] if product_data.get("image") else ""
            item["other_image_urls"] = product_data.get("image", [])[1:] if product_data.get("image") else []
            item["colors"] = colors if colors else []
            item["variations"] = {}
            item["sizes"] = size_dict if size_dict else {}
            item["other_details"] = other_details
            item["availability"] = 'in_stock' if 'InStock' in availability else 'out_of_stock'
            item["number_of_items_in_stock"] = 0
            item["last_update"] = ""
            item["creation_date"] = current_time.strftime("%Y-%m-%d %H:%M:%S")

            if not size_dict:
                size_url = response.urljoin(response.css('.js-sizeguide-new::attr(data-href)').get(''))
                yield Request(url=size_url, callback=self.get_sizes, meta={'item': item})
            else:
                self.items_scraped_count += 1
                self.scraped_items.append(product_data.get("offers", {}).get("url", ""))
                yield item
        except Exception as e:
            self.write_logs(f"Error in parse_product: {e} | URL: {response.url}")

    def get_sizes(self, response):
        item = response.meta['item']
        sizes_list = ['xxs', 'xs', 's', 'm', 'l', 'xl', 'xxl', 'xxxl', 'xxxxl']
        json_response = response.json()
        try:
            content = json_response.get('content', '')
            selector = Selector(text=content)

            sizes = {}
            size_ids = selector.css(
                "table.blm-sizeguide__measurement-table thead td[data-cell-value] .js-measurement-radio-title::text").getall()
            size_ids = [size.lower().strip() for size in size_ids if size.strip()]

            matching_sizes = [size for size in sizes_list if size in size_ids]
            if matching_sizes:
                sizes['us'] = matching_sizes
            else:
                for row in selector.css("table.blm-sizeguide__measurement-table tbody tr.blm-sizeguide__measurement-table-row"):
                    label = row.css("td:first-child span::text").get().strip().lower()
                    measurements = row.css("td.blm-sizeguide__measurement-table-cell .js-measurement-inches::text").getall()
                    measurements = [m.strip() for m in measurements if m.strip() and "undefined" not in m]

                    for size_id, measurement in zip(size_ids, measurements):
                    #     try:
                    #         region = size_id.split()[0].lower()
                    #         if region not in sizes:
                    #             sizes[region] = {}
                    #
                    #         # Check if the size_value exists (if it's a region-based size, like 'eu 10')
                    #         if len(size_id.split()) > 1:
                    #             size_value = size_id.split()[1]
                    #             if size_value not in sizes[region]:
                    #                 sizes[region][size_value] = []
                    #
                    #             # Append the measurement under the correct size within the region
                    #             sizes[region][size_value].append({label: measurement})
                    #         else:
                    #             # If there is no size_value (for example, only 'us' or 'eu' without a specific number), append the measurement directly
                    #             sizes[region].append({label: measurement})
                    #     except Exception as e:
                    #         print(f"Error processing size_id: {size_id}, Error: {e}")
                        if size_id not in sizes:
                            sizes[size_id] = []
                        sizes[size_id].append({label: measurement})
        except:
            a=1
            sizes={}
            # json_response = {}

        if sizes:
            item['sizes'] = sizes
        else:
            item['sizes'] = self.extract_size_fallback(json_response)

        url = item.get('product_url')
        self.scraped_items.append(url)
        self.items_scraped_count += 1
        yield item

    def extract_sizes(self, sizes):
        size_dict = {}
        if any("EU" in size for size in sizes):
            size_dict["eu"] = [size.split(" ")[1] for size in sizes if "EU" in size]
        if any("US" in size for size in sizes):
            size_dict["us"] = [size.split(" ")[1] for size in sizes if "US" in size]
        if any("UK" in size for size in sizes):
            size_dict["uk"] = [size.split(" ")[1] for size in sizes if "UK" in size]
        return size_dict

    def extract_size_fallback(self, json_response):
        content = json_response.get('content', '')
        selector = Selector(text=content)
        rows = selector.css("tr.blm-sizeguide__measurement-table-row")

        sizes_ = {}
        height_values = []

        for row in rows:
            key_column = row.css("td.blm-sizeguide__measurement-table-cell:first-child span::text").get().strip()
            values = row.css("td.js-measurement-table-cell .js-measurement-inches::text").getall()
            values = [value.strip() for value in values if value.strip()]
            if key_column == "HEIGHT":
                height_values = values
                break

        for row in rows:
            key_column = row.css("td.blm-sizeguide__measurement-table-cell:first-child span::text").get().strip()
            values = row.css("td.js-measurement-table-cell .js-measurement-inches::text").getall()
            values = [value.strip() for value in values if value.strip()]
            if key_column == "AGE" and height_values:
                for age, height in zip(values, height_values):
                    sizes_[self.to_snake_case(age)] = {"height": height}

        return sizes_

    def get_description(self, product_data):
        try:
            desc_tag =  product_data.get("description", "")
            html = Selector(text=desc_tag)
            text = ' '.join(html.css(' ::text').getall())
        except:
            text = ''
        return text

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def get_url(self, page_no, category_key):
        sz = 12
        start = page_no * sz + 48
        productCount = int(start) + 2
        url = (
            f'https://bloomingdales.ae/on/demandware.store/Sites-BloomingDales_AE-Site/en_AE/Search-UpdateGrid?cgid={category_key}'
            f'&pmin=6.00&start={start}&sz={str(sz)}&icgid={category_key}&selectedUrl=https://bloomingdales.ae/on/demandware.store/Sites-BloomingDales_AE-Site/en_AE/Search-UpdateGrid'
            f'?cgid={category_key}&pmin=6.00&start={start}&sz={str(sz)}&icgid={category_key}&productCount={productCount}')

        return url

    def close(spider, reason):
        # Log overall scraping statistics
        spider.write_logs(f"\n--- Scraping Summary ---")
        spider.write_logs(f"Total Products Available on Website: {spider.items_found}")
        spider.write_logs(f"Total Products Successfully Scraped: {spider.items_scraped_count}")

        # Log script execution times
        spider.write_logs(f"\n--- Script Execution Times ---")
        spider.write_logs(f"Script Start Time: {spider.script_starting_datetime}")
        spider.write_logs(f"Script End Time: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        spider.write_logs(f"Reason for Closure: {reason}")

    @staticmethod
    def safe_strip(value):
        return value.strip() if value else ""

    @staticmethod
    def to_snake_case(s):
        s = s.strip().lower().replace("-", "_").replace(" ", "_")
        return re.sub(r"[^a-z0-9_]", "", s)




