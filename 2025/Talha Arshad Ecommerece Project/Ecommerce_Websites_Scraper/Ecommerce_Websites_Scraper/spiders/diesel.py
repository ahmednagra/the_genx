import os
import re
import json
from datetime import datetime
from collections import OrderedDict

from scrapy import Spider, Request, Selector

"""
Scrapy Spider for extracting product details from Diesel's website.
This spider navigates through categories, extracts product URLs, and scrapes product details.
"""

class DieselSpider(Spider):
    name = "Diesel"
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        "CONCURRENT_REQUESTS": 2,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 429, 403, 404, 408],
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
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    def __init__(self):
        super().__init__()
        self.items_found = 0
        self.items_scraped_count = 0

        # Initialize Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

    def start_requests(self):
        """Initiates the scraping process by requesting the homepage."""
        yield Request(url='https://ae.diesel.com/', headers=self.headers, callback=self.parse)

    def parse(self, response, **kwargs):
        """Extracts main categories and iterates over subcategories."""
        main_cats = ['SALE', "MAN", "WOMAN", "1DR", "KIDS"]
        categories = response.css('[data-action="navigation"] li')

        if not categories:
            self.write_logs("No categories found on the homepage.")
            return

        for i, cat in enumerate(categories[:5]):
            main_cat_name = main_cats[i]
            sub_cat_urls = cat.css('.second-level .nav-anchor::attr(href)').getall()
            sub_cat_names = cat.css('.second-level .nav-anchor::attr(title)').getall()

            for url, sub_cat_name in zip(set(sub_cat_urls), set(sub_cat_names)):
                self.write_logs(f"Scraping category: {main_cat_name} -> {sub_cat_name}")
                yield Request(url=url, callback=self.listing_page, headers=self.headers,
                                     meta={'main_cat_name': main_cat_name, 'sub_cat_name': sub_cat_name})

    def listing_page(self, response):
        """Extracts product URLs and follows pagination."""
        main_cat_name = response.meta.get('main_cat_name', '')
        sub_cat_name = response.meta.get('sub_cat_name', '')

        product_urls = response.css('.product-item-link::attr(href)').getall()

        if not response.meta.get('pagination', ''):
            if not product_urls:
                self.write_logs(f"No products found in {main_cat_name} -> {sub_cat_name}")
            else:
                total_products = response.css('#toolbar-amount span::text').get('')
                self.items_found += int(total_products)
                self.write_logs(f"Category: {main_cat_name} -> {sub_cat_name} found total {total_products} Products")

        for product_url in product_urls:
            yield Request(url=product_url, callback=self.product_page, headers=self.headers, meta={'main_cat_name': main_cat_name,
                                                                                                 'sub_cat_name': sub_cat_name})
        next_page_url = response.css('[title="Next"]::attr(href)').get('')
        if next_page_url:
            yield Request(url=next_page_url, callback=self.listing_page, headers=self.headers,
                                 meta={'main_cat_name': main_cat_name,
                                       'sub_cat_name': sub_cat_name,
                                       'pagination':True})

    def product_page(self, response):
        """Extracts product details and handles missing fields gracefully."""
        main_cat_name = response.meta.get('main_cat_name')
        sub_cat_name = response.meta.get('sub_cat_name')
        old_price = response.css('[data-price-type="oldPrice"] span::text').get('')

        try:
            json_data = json.loads(response.css('[type="application/ld+json"]::text').getall()[-1])
        except json.JSONDecodeError as e:
            self.write_logs(f"Error parsing JSON data: {e} - URL: {response.url}")
            return

        try:
            product_title = json_data.get('name', '')
            product_price = json_data.get('offers', {})[0].get('price', '')
            discount = 0.0
            if old_price:
                old_price = old_price.replace(',', '')
                match = re.search(r'\d+', old_price)
                if match:
                    old_price = float(match.group())
                    discount = old_price - float(product_price)

            currency = json_data.get('offers', {})[0].get('priceCurrency', '')
            availability = json_data.get('offers', {})[0].get('availability', '')
            brand = json_data.get('brand', {}).get('name', '')
            sku = json_data.get('sku', '')
            product_images = response.css('.image-gallery .image-gallery__item::attr(src)').getall()
            description = response.css('.description .value::text').get('')
            colors = response.css('.micro_colour::text').getall()
            current_time = datetime.now()

            sizes, variations = self.extract_sizes(response)
            other_details = self.extract_other_details(response)

            item = OrderedDict()
            item["source_id"] = "Diesel"
            item["product_url"] = response.url
            item["brand"] = brand
            item["product_title"] = product_title
            item["product_id"] = sku
            item["category"] = (main_cat_name + ', ' + sub_cat_name) if main_cat_name and sub_cat_name else main_cat_name
            item["price"] = float(product_price) if product_price else 0.0
            item["discount"] = discount if discount else 0.0
            item["currency"] = currency
            item["description"] = description
            item["main_image_url"] = product_images[0] if product_images and len(product_images) >= 1 else ''
            item["other_image_urls"] = product_images[1:] if product_images and len(product_images) > 1 else []
            item["colors"] = colors if colors else []
            item["variations"] = variations if variations else {}
            item["sizes"] = sizes if sizes else {}
            item["other_details"] = other_details
            item["availability"] = 'in_stock' if 'InStock' in availability else 'out_of_stock'
            item["number_of_items_in_stock"] = 0
            item["last_update"] = ""
            item["creation_date"] = current_time.strftime("%Y-%m-%d %H:%M:%S")

            self.items_scraped_count += 1
            yield item
        except Exception as e:
            self.write_logs(f"Error parsing JSON data: {e} - URL: {response.url}")

    def extract_sizes(self, response):
        """Extracts size details from JSON config."""
        colors = response.css('.micro_colour::text').getall()
        description = response.css('.description .value::text').get('')
        addtitional_desc = response.css('.additional-description span::text').getall()
        if addtitional_desc and description:
            addtitional_desc = '\n'.join(addtitional_desc)
            description = description + '\n' + addtitional_desc
        if addtitional_desc and description is None:
            description = addtitional_desc

        # Adjusted regex to capture the entire "jsonSwatchConfig" object
        pattern = r'"jsonSwatchConfig": (\{.*?\}\})'
        match = re.search(pattern, response.text, re.DOTALL)  # Enable multi-line matching
        sizes = {}
        variation = {}
        labels = []
        length = []
        if match:
            json_string = match.group(1)  # Get the full JSON string

            try:
                size_data = json.loads(json_string)  # Parse into Python dict
                # print("Parsed JSON Object:")
                labels = [
                    value['label']
                    for key, value in size_data.get('192', {}).items()
                    if isinstance(value, dict) and 'label' in value
                ]
                length = [
                    value['label']
                    for key, value in size_data.get('200', {}).items()
                    if isinstance(value, dict) and 'label' in value
                ]
                if colors is None:
                    colors = [
                        value['label'].strip()
                        for key, value in size_data.get('209', {}).items()  # Use .get() to safely access '209'
                        if isinstance(value, dict) and 'label' in value
                    ]

                # Output the result
            except json.JSONDecodeError as e:
                a=1
                # print(f"Error decoding JSON: {e}")
        else:
            a=1
            # print("JSON object not found")

        sizes['us'] = labels

        if length:
            variation['length'] = length

        return sizes, variation

    def extract_other_details(self, response):
        """Extracts additional product details like care instructions and size & fit."""
        care_instructions = response.css('.care-instructions-content li::text').getall()
        size_and_fit = response.css('.size-fit-content li span::text').get('')

        other_details = {
            "care_instructions": [line.strip() for line in care_instructions if
                                  line.strip()] if care_instructions else [],
            "size_and_fit": size_and_fit.strip() if size_and_fit else ''
        }
        return other_details if other_details else {}

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

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
