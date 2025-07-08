import json
import scrapy
import re
from collections import OrderedDict
from datetime import datetime

"""
SandroSpider - Scrapy spider for extracting product details from Sandro's Saudi Arabia website.

This spider is designed to scrape product information such as:
- Product Title, Price, Discount, Stock Status, Brand, Category
- Description, Images, Colors, Sizes, Variations, and Additional Details

The spider navigates through the main categories (e.g., Woman, Man), fetches the product listings, and extracts detailed information about each product. It also handles pagination to scrape multiple pages and supports retries for failed requests.

### Key Features:
- Scrapes product data such as title, price, discount, description, images, colors, sizes, and more.
- Handles pagination to fetch data across multiple pages.
- Uses regular expressions to extract product and pagination data from JSON responses.
- Supports retries for failed requests with a retry limit and custom HTTP codes.
- Uses Scrapy's `OrderedDict` for structured output, saved in a timestamped JSON file.
- Collects detailed product data including product variants, availability, and additional product details like delivery, returns, and payment methods.
- Captures metadata in a structured JSON format suitable for further processing or analysis.

### Data Extracted:
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Colors, Variations, and more.

This spider provides a robust solution for scraping Sandro's product listings from their Saudi Arabia website.
"""

class SandroSpider(scrapy.Spider):
    name = "Sandro"
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

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
        }
    }

    def start_requests(self):
        url = 'https://sandro.sa'
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        maincats = ['Woman', 'Man']
        for i, maincat in enumerate(maincats):
            subcats_urls = response.css(f'#MegaMenu-Content-{i + 1} ul.mega-menu__list-title li a::attr(href)').getall()
            for subcat_url in subcats_urls:
                subcat_url = response.urljoin(subcat_url)
                yield scrapy.Request(subcat_url, callback=self.listing_page, meta={'maincat': maincat})

    def listing_page(self, response):
        maincat = response.meta.get('maincat', '')

        # regex to match full JSON
        pattern = r'products:\s*(\[\{.*?\}\])(?=\s*,\s*collection:)'

        # Find the match
        match = re.search(pattern, response.text, re.DOTALL)

        if match:
            # Extract the JSON string
            products_json = match.group(1)
            # Load the JSON string into a Python dictionary
            products = json.loads(products_json)
            for product in products:

                try:
                    product_url = "https://sandro.sa/products/" + product.get('handle')
                    price = product.get('price')
                    if price:
                        price = float(price)
                        price = price / 100
                    compare_at_price = product.get('compare_at_price')
                    if compare_at_price:
                        compare_at_price = float(compare_at_price)
                        compare_at_price = compare_at_price / 100
                    discount = 0.0
                    if price and compare_at_price:
                        discount = compare_at_price - price
                    updated_variants = []
                    variants = product.get('variants')
                    for variant in variants:
                        updated_variants.append({
                            'id': str(variant.get('id')),
                            'title': variant.get('title'),
                            'options': variant.get('options'),
                            'price': int(variant.get('price')) / 100 if variant.get('price') else None,
                            'featured_image': variant.get('featured_image'),
                        })
                    current_time = datetime.now()
                    main_img = product.get('featured_image')  # Check if main_img exists and does not start with 'https':
                    if main_img and not main_img.startswith('https:'):
                        main_img = f'https:{main_img}'

                    others_img = product.get('images', [])  # Get an image list, default to an empty list if None
                    others_img = [f'https:{img}' if img and not img.startswith('https:') else img for img in others_img]

                    item = OrderedDict({
                        "source_id": "Sandro",
                        "product_url": product_url,
                        "brand": product.get('vendor'),
                        "product_title": product.get('title'),
                        "product_id": str(product.get('id')),
                        "category": f"{maincat}, {product.get('type')}",
                        "price": price if price else compare_at_price,
                        "discount": discount if discount else 0.0,
                        "currency": "SAR",
                        "description": product.get('content'),
                        "main_image_url": main_img,
                        "other_image_urls": others_img,
                        "colors": [],
                        "variations": updated_variants if updated_variants else [],
                        "sizes": [],
                        "other_details": {},
                        "availability": 'in_stock' if product.get('available') else 'out_of_stock',
                        "number_of_items_in_stock": 0,
                        "last_update": product.get('created_at'),
                        "creation_date": current_time.strftime("%Y-%m-%d %H:%M:%S"),

                    })
                    yield scrapy.Request(product_url, callback=self.detail_page, meta={'item': item})
                except json.JSONDecodeError as e:
                    # Handle JSON decoding errors
                    print("Error yield item:", e)
        else:
            a=1
            # print("No match found")

        # Regex to capture the JSON after `pagination`:
        pattern = r'pagination:\s*(\{.*?\})(?=\s*,\s*moneyFormatWithCurrency:)'

        # Find the match
        match = re.search(pattern, response.text, re.DOTALL)

        if match:
            # Extract the JSON string
            pagination_json = match.group(1)
            pagination_dict = json.loads(pagination_json)
            # print("Parsed JSON:", pagination_dict)
            next_page = pagination_dict.get('next', {}).get('url')
            if next_page:
                next_page = response.urljoin(next_page)
                yield scrapy.Request(next_page, callback=self.listing_page, meta={'maincat': maincat})

    def detail_page(self, response):
        item = response.meta.get('item')
        color = response.css('.color-label::text').getall()
        size_json = response.css('#variant-data::text').get()
        if size_json:
            size_data = json.loads(size_json)
            item['sizes'] = size_data if size_data else []
        other_detail = {
        }
        delivery_and_returns = response.css('#drw-8 .page-width div li::text').getall()
        if isinstance(delivery_and_returns, list) and delivery_and_returns:
            other_detail['delivery_and_returns'] = [line.replace(' ', '') for line in delivery_and_returns]
        our_commitments = response.css('.product-description-hr .description-dds::text').getall()[-1] if response.css(
            '.product-description-hr .description-dds::text') else ''
        other_detail['our_commitments'] = (our_commitments or [])[-1].strip() if our_commitments else ''
        secure_payment_methods = response.css('.description-ccp.page-width')[-1] if response.css(
            '.description-ccp.page-width') else None
        if secure_payment_methods:
            secure_payment_methods = secure_payment_methods.css('*::text').getall()
            other_detail['secure_payment_methods'] = [line for line in secure_payment_methods if line.strip()]

        item['colors'] = [colr.strip() for colr in (color or []) if colr] if color else []
        data = response.css('[type="application/ld+json"]::text').getall()[-1]
        if data:
            data = json.loads(data)
            item['description'] = data.get('description')
        item['other_details'] = other_detail if other_detail else {}
        yield item
