import os
import re
import json
from datetime import datetime
from collections import OrderedDict

import requests
from scrapy import Spider, Request, Selector

"""
Bath & Body Works Scraper

This Scrapy spider is designed to scrape product details from the Bath & Body Works Saudi Arabia website.  
It extracts product information such as name, price, availability, category, and promotions using API requests  
to Algolia's search service and the website's GraphQL API.

### Key Features:
- Scrapes multiple categories including Candles, Body Care, Hand Soaps & Sanitizers, and Fresheners.
- Uses Algolia API to fetch product listings.
- Fetches detailed product data via GraphQL requests.
- Implements robust error handling and logging for debugging.
- Supports proxy usage via Zyte for reliable scraping.

### Data Extracted:
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Images, and More.

- This spider ensures efficient and structured data collection while maintaining anonymity and avoiding detection  
by using Zyte's proxy service.
"""


class BathBodySpider(Spider):
    name = "BathandBody"
    start_urls = ["https://www.bathandbodyworks.com.sa/en/"]
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
        "ZYTE_API_TRANSPARENT_MODE": True
    }

    json_headers = {
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'no-cache',
                    'content-type': 'application/json',
                    'pragma': 'no-cache',
                    'priority': 'u=1, i',
                    'referer': 'https://www.bathandbodyworks.com.sa/en/buy-night-3-wick-candle-8.html',
                    'store': 'sau_en',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
                    'x-requested-with': 'XMLHttpRequest',
                }

    def __init__(self):
        super().__init__()
        self.items_found = 0
        self.items_scraped_count = 0
        self.categories = ['Candles', 'Body Care', 'Hand Soaps & Sanitizers', 'Fresheners']
        self.proxies = {scheme: "http://f693db95c418475380b0e70954ed0911:@api.zyte.com:8011" for scheme in ("http", "https")}

        # Initialize Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

    def parse(self, response, **kwargs):
        """Parses the main response and extracts API keys."""
        try:
            data_dict = json.loads(response.css('script[type="application/json"] ::text').get(''))
        except json.JSONDecodeError as e:
            self.write_logs(f'[ERROR] JSON parsing error in parse(): {e}')
            data_dict = {}

        # Extract API credentials with default values
        app_id = data_dict.get('algoliaSearch', {}).get('application_id', '') or '6TOQSJY0O6'
        app_key = data_dict.get('algoliaSearch', {}).get('api_key', '') or 'c7af7bb2c02f6f150cf0e92eeaef1476'
        url = f'https://{app_id.lower()}-1.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia for JavaScript (3.35.1); Browser (lite)&x-algolia-application-id={app_id}&x-algolia-api-key={app_key}'

        for category in self.categories:
            data = self.form_data(category=category)
            data_str = json.dumps(data) if 'Hand' not in category else data
            yield Request(url, method='POST', body=data_str,
                callback=self.parse_categories, meta={'handle_httpstatus_all': True, 'category':category})

    def parse_categories(self, response):
        """Parses category response and extracts product URLs."""
        category = response.meta.get('category', '')
        try:
            data_dict = response.json()
            data_dict = data_dict.get('results', {})[0]
        except json.JSONDecodeError as e:
            self.write_logs(f'[ERROR] JSON parsing error in parse_categories() for {category}: {e}')
            return

        total_records = data_dict.get('nbHits', 0)
        if total_records:
            self.write_logs(f'Category:{category} - Total Records: {total_records}')
            self.items_found  += int(total_records)

        for product in data_dict.get('hits', []):
            try:
                url = product.get('url', {}).get('en', '')
                if url:
                    url_key = url.replace('.html', '').split('/')[-1]
                    url = f'https://www.bathandbodyworks.com.sa{url}'
                    self.json_headers['referer'] = url
                    data = self.form_data(url_key=url_key)
                    resp = requests.get('https://www.bathandbodyworks.com.sa/graphql', params=data, headers=self.json_headers, proxies=self.proxies, verify=False)
                    if resp.status_code == 200:
                        yield from self.parse_product_detail(response=resp, product=product)
            except Exception as e:
                self.write_logs(f'[ERROR] Failed to process product URL in parse_categories(): {e}')

    def parse_product_detail(self,response, product):
        """Parses product details and extracts necessary fields."""
        current_time = datetime.now()
        try:
            p_dict = response.json().get('data', {}).get('products', {}).get('items', [])[0]
        except json.JSONDecodeError as e:
            self.write_logs(f'[ERROR] JSON parsing error in parse_product_detail(): {e}')
            return

        try:
            price_dict= p_dict.get('price_range', {}).get('maximum_price', {})
            price = price_dict.get('final_price', {}).get('value', 0.0)
            discount = price_dict.get('discount', {}).get('percent_off', 0.0)
            stock_status = p_dict.get('stock_status', '')
            updated_at= product.get('attr_updated_at', {}).get('en', '')

            item = OrderedDict()
            url = p_dict.get('url_key', '')
            category = p_dict.get('gtm_attributes', {}).get('category', '')
            item["source_id"] = "bathandbody"
            item["product_url"] = f'https://www.bathandbodyworks.com.sa/en/{url}' if url else ''
            item["brand"] = ''.join(product.get('attr_brand', {}).get('en', ''))
            item["product_title"] = p_dict.get('name', '') or product.get('title', {}).get('en')
            item["product_id"] = p_dict.get('sku', '')
            item["category"] = category.replace('/', ', ') if category else ''
            item["price"] = float(price) if price else 0.0
            item["discount"] = float(discount) if discount else 0.0
            item["currency"] = "SAR"
            item["description"] = self.get_description(p_dict)
            item["main_image_url"] =  p_dict.get('swatch_image_url', '')
            item["other_image_urls"] = []
            item["colors"] = []
            item["variations"] = {}
            item["sizes"] = self.get_sizes(p_dict, product)
            item["other_details"] = self.get_other_details(p_dict, product)
            item["availability"] = 'in_stock' if 'IN_STOCK' in stock_status else 'out_of_stock'
            item["number_of_items_in_stock"] = p_dict.get('stock_data', {}).get('qty', 0)
            item["last_update"] = updated_at if updated_at else ''
            item["creation_date"] = current_time.strftime("%Y-%m-%d %H:%M:%S")

            self.items_scraped_count += 1
            yield item
        except Exception as e:
            self.write_logs(f'[ERROR] Failed to extract product details: {e}')

    def get_description(self, product_data):
        try:
            desc_tag =  product_data.get('description', {}).get('html', '')
            html = Selector(text=desc_tag)
            text = ' '.join(html.css(' ::text').getall())
        except:
            text = ''
        return text

    def product_formdata(self, url_key):
        params = {
            'query': 'query($url:String){products(filter:{url_key:{eq:$url}}){total_count items{sku id type_id name description{html}short_description{html}url_key is_buyable stock_status express_delivery same_day_delivery ship_to_store is_returnable reserve_and_collect swatch_image swatch_image_url free_gift_promotion{rule_id rule_type rule_web_url rule_name rule_description auto_add max_gift coupon_code total_items gifts{id sku name}}price_range{maximum_price{regular_price{value}final_price{value}discount{percent_off}}}brand_logo_data{url alt title}brand_logo media_gallery{url label styles ... on ProductVideo{video_content{media_type video_provider video_url video_title video_description video_metadata}}}gtm_attributes{id name variant price brand category dimension2 dimension3 dimension4}meta_title meta_description meta_keyword og_meta_title og_meta_description stock_data{max_sale_qty qty}promotions{context url label type}... on ConfigurableProduct{maximum_discounted_price{final_price regular_price discount}configurable_options{attribute_uid label position attribute_code values{value_index store_label}}variants{product{id sku meta_title stock_status express_delivery same_day_delivery ship_to_store is_returnable reserve_and_collect attribute_set_id swatch_data{swatch_type}free_gift_promotion{rule_id rule_type rule_web_url rule_name rule_description auto_add max_gift coupon_code total_items gifts{id sku name}}price_range{maximum_price{regular_price{value}final_price{value}discount{percent_off}}}stock_data{qty max_sale_qty}media_gallery{url label styles ... on ProductImage{url label}... on ProductVideo{video_content{media_type video_provider video_url video_title video_description video_metadata}}}swatch_image swatch_image_url promotions{context url label type}size}attributes{label code value_index}}}category_ids_in_admin breadcrumb_category_id categories{id name level url_path include_in_menu breadcrumbs{category_name category_id category_level category_url_key category_url_path category_gtm_name}write_review_form_fields gtm_name}fragrance_name fragrance_description descriptive_name form more_info usage brand size}}}',
            'variables': f'{{"url":"{url_key}"}}',
        }

        return params

    def write_logs(self, log_msg):
        """Writes logs to a file and prints them."""
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def form_data(self, category=None, url_key=None):
        if category and 'Hand Soaps' in category :
            data = '{"requests":[{"indexName":"bbw_prod_sa_product_list","params":"clickAnalytics=true&facets=%5B%22*%22%5D&filters=(stock%20%3E%200)%20AND%20(field_category_name.en.lvl0%3A%20%22Hand%20Soaps%20%26%20Sanitizers%22)&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=1000&optionalFilters=null&page=0&ruleContexts=%5B%22hand_soaps_sanitizers%22%2C%22web__hand_soaps_sanitizers%22%5D&userToken=anonymous-e5028086-d4d2-4f28-8c49-e55b7a85d9ed&analytics=true"}]}'
            return data

        if category:
            category = category.replace('"', '\\"')  # escaping any quotes in category, just in case
            lower_category = category.replace(' ', '_').lower()

            params = {
                "requests": [
                    {
                        "indexName": "bbw_prod_sa_product_list",
                        "params": f"clickAnalytics=true&facets=[\"*\"]&filters=(stock > 0) AND (field_category_name.en.lvl0: \"{category}\")&highlightPostTag=<ais-highlight-0000000000>&highlightPreTag=<ais-highlight-0000000000>&hitsPerPage=1000&optionalFilters=null&page=0&ruleContexts=[\"{lower_category}\",\"web__{lower_category}\"]&userToken=anonymous-f0fbd000-3342-4094-af2c-f02870e99f21&analytics=true"
                    }
                ]
            }

        elif url_key:
            params = {
                'query': 'query($url:String){products(filter:{url_key:{eq:$url}}){total_count items{sku id type_id name description{html}short_description{html}url_key is_buyable stock_status express_delivery same_day_delivery ship_to_store is_returnable reserve_and_collect swatch_image swatch_image_url free_gift_promotion{rule_id rule_type rule_web_url rule_name rule_description auto_add max_gift coupon_code total_items gifts{id sku name}}price_range{maximum_price{regular_price{value}final_price{value}discount{percent_off}}}brand_logo_data{url alt title}brand_logo media_gallery{url label styles ... on ProductVideo{video_content{media_type video_provider video_url video_title video_description video_metadata}}}gtm_attributes{id name variant price brand category dimension2 dimension3 dimension4}meta_title meta_description meta_keyword og_meta_title og_meta_description stock_data{max_sale_qty qty}promotions{context url label type}... on ConfigurableProduct{maximum_discounted_price{final_price regular_price discount}configurable_options{attribute_uid label position attribute_code values{value_index store_label}}variants{product{id sku meta_title stock_status express_delivery same_day_delivery ship_to_store is_returnable reserve_and_collect attribute_set_id swatch_data{swatch_type}free_gift_promotion{rule_id rule_type rule_web_url rule_name rule_description auto_add max_gift coupon_code total_items gifts{id sku name}}price_range{maximum_price{regular_price{value}final_price{value}discount{percent_off}}}stock_data{qty max_sale_qty}media_gallery{url label styles ... on ProductImage{url label}... on ProductVideo{video_content{media_type video_provider video_url video_title video_description video_metadata}}}swatch_image swatch_image_url promotions{context url label type}size}attributes{label code value_index}}}category_ids_in_admin breadcrumb_category_id categories{id name level url_path include_in_menu breadcrumbs{category_name category_id category_level category_url_key category_url_path category_gtm_name}write_review_form_fields gtm_name}fragrance_name fragrance_description descriptive_name form more_info usage brand size}}}',
                'variables': f'{{"url":"{url_key}"}}',
            }

        return params

    def get_sizes(self, p_dict, product):
        """Extracts size/volume details of the product."""
        try:
            s_value= ''.join(product.get('attr_size', {}).get('en', ''))
            key = 'us'
            if s_value:
                dict = {'size/volume': s_value}
                sizes = {key:dict}
            else:
                sizes = {}

            return sizes
        except Exception as e:
            self.write_logs(f'[ERROR] Failed to extract sizes: {e}')
            return {}

    def get_other_details(self, p_dict, product):
        """Extracts promotional details of the product."""
        promotions = p_dict.get('promotions', [])
        promotion_details = {}
        if promotions:
            for promotion in promotions:
                label = promotion.get('label', '')
                url = promotion.get('url', '')
                # You can append these to a list or process them as needed
                if label and url:  # Only add valid promotions with both label and url
                    promotion_details[label] = url

        return  promotion_details


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