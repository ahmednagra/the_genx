import json
import scrapy
import re
from collections import OrderedDict
from datetime import datetime
from bs4 import BeautifulSoup

"""
MajeSpider - A Scrapy spider for scraping product details from Maje (Saudi Arabia) website.

This spider is designed to collect product information such as:
- Product ID, title, brand, category
- Pricing (with and without discount), currency
- Description, images, colors, and sizes
- Additional product details like material and care instructions, delivery and collection information

The spider starts by scraping the main menu and navigation categories from the homepage, then proceeds to the product listing pages. For each product, it scrapes the details from the product page including pricing, variants, description, images, and more.
The spider uses BeautifulSoup for parsing product description tables and handling HTML formatting for material, care instructions, and other information.
This spider is optimized for crawling Maje's product pages and gathering rich product data for further processing or analysis.
"""

class MajeSpider(scrapy.Spider):
    name = "Maje"
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
        url = 'https://maje.sa/'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        menu_cats = response.css('.inner-wrapper a::attr(href)').getall()
        nav_cats = response.css('.header__nav-link::attr(href)').getall()
        cats = menu_cats + nav_cats
        for cat in cats:
            yield scrapy.Request(url=response.urljoin(cat), callback=self.listing_page)

    def listing_page(self, response):

        # Regex pattern to match the productVariants array
        pattern = r'"productVariants":\s*(\[\{.*?\}\])'

        # Search for the pattern in the string
        match = re.search(pattern, response.text)

        # If the pattern is found, load the matched JSON into a Python object
        if match:
            product_variants_json = match.group(1)
            product_variants = json.loads(product_variants_json)  # Converts to Python object
            for product in product_variants:
                product = product.get('product')
                data = {
                    'id': product.get('id'),
                    'title': product.get('title'),
                    'brand': product.get('vendor'),
                    'cat': product.get('type'),
                    'url': product.get('url'),
                }
                yield scrapy.Request(url=response.urljoin(data['url']), callback=self.detail_page, meta={'data': data})
        else:
            a=1
            # print("No match found")

        next_page = response.css('[rel="next"]::attr(href)').get()
        if next_page:
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.listing_page)

    def detail_page(self, response):
        current_time = datetime.now()
        data = response.meta.get('data')
        discount = 0.0
        currency = response.css('[property="og:price:currency"]::attr(content)').get()
        price_without_discount = response.css('.price__compare::text').get()
        if price_without_discount:
            price_without_discount = int(price_without_discount.replace(",", "").split()[0])
        price_with_dicount = response.css('[property="og:price:amount"]::attr(content)').get()
        if price_with_dicount:
            price_with_dicount = int(price_with_dicount.replace(",", ""))

        if price_with_dicount and price_without_discount:
            discount = int(price_without_discount) - int(price_with_dicount)
        description = response.css('.product__descr li::text').getall()
        colors = response.css('.product_color_swatch::attr(aria-label)').getall()
        colors = [c.split(':')[1].strip() for c in colors] if colors else []
        sizes = response.css('.size__guide-mobi+.select__wrapper li::attr(data-handle)').getall()
        uk = []
        fr = []
        for size in sizes:
            if '/' in size:
                uk_size, fr_size = size.split('/')
                uk.append(uk_size)
                fr.append(fr_size)
            else:
                uk.append(size)
                fr.append(size)
        sizes = {'uk': uk, 'fr': fr}
        main_image = response.css('.featured__images  img::attr(src)').get()
        if main_image:
            main_image = response.urljoin(main_image)
        other_images = response.css('.product__images img::attr(src), .product__images video::attr(src)').getall()
        if other_images:
            other_images = [response.urljoin(img) for img in other_images]
        delivery_and_collection = response.css(
            '.product__description-wrapper+.product__tabs .tabs__content li::text').getall() or []
        delivery_and_collection = [d.strip().replace(' ', ' ') for d in delivery_and_collection] if delivery_and_collection else []
        material_and_care = ''
        more_description = ''
        html = response.css('.prodaccordion__content tbody').getall()[-1] if len(response.css('.prodaccordion__content tbody').getall()) > 1 else ''
        if html:
            soup = BeautifulSoup(html, "html.parser")

            # Find table rows
            rows = soup.find_all("tr")

            for row in rows:
                cells = row.find_all("td")

                # Extract Material
                material_text = cells[0].get_text(separator=" | ", strip=True) if cells[0].get_text(
                    strip=True) else "No material info"

                # Extract Care Instructions
                care_text = cells[1].get_text(separator=" | ", strip=True) if len(cells) > 1 else "No care instructions"

                # Combine both into one variable
                material_and_care = f"Material: {material_text} || Care Instructions: {care_text}"

        html2 = response.css('.prodaccordion__content tbody').getall()[0] if response.css('.prodaccordion__content tbody').getall() else ''
        if html2:
            soup = BeautifulSoup(html2, "html.parser")

            # Find the first <td> containing description
            td = soup.find("td")

            if td:
                # Extract text, handling <p>, <br>, and inline text correctly
                more_description = td.get_text(separator=" ", strip=True)
            else:
                more_description = ""

        other_detail = {
            'delivery_and_collection': delivery_and_collection,
            'material_and_care': [material_and_care if material_and_care else ''],
            'more_description': more_description if more_description else ''
        }
        item = OrderedDict(
            {
                "source_id": "Maje",
                "product_url": response.url,
                "brand": data.get('brand', ''),
                "product_title": data.get('title', ''),
                "product_id": data.get('id', ''),
                "category": data.get('cat', '') if data.get('cat', '') else '',
                "price": float(price_with_dicount) if price_with_dicount else None,
                "discount": float(discount) if discount else 0.0,
                "currency": currency,
                "description": '\n'.join(description),
                "main_image_url": main_image if main_image else '',
                "other_image_urls": other_images if other_images else [],
                "colors": colors if colors else [],
                "variations": {},
                "sizes": sizes if sizes else {},
                "other_details": other_detail if other_detail else {},
                "availability": '',
                "number_of_items_in_stock": 0,
                "last_update": "",
                "creation_date": current_time.strftime("%Y-%m-%d %H:%M:%S"),

            }
        )
        yield item
