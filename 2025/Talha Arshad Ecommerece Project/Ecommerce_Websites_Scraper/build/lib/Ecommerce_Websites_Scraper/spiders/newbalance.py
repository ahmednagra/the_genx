import json
import os
from datetime import datetime
from collections import OrderedDict

import requests
from scrapy import Spider, Request, Selector

"""
Scrapy spider to scrape product details from the New Balance Saudi Arabia website.

This spider extracts product data using Scrapy, Algolia API, and GraphQL requests. It fetches product details, including pricing, stock availability, descriptions, and images. The scraper handles pagination, extracts category-wise data, and formats results into a structured JSON file.

Key Features:
- Extracts Algolia API credentials dynamically.
- Scrapes paginated product listings and detailed product info.
- Captures product title, price, stock status, images, variations, and descriptions.
- Stores structured data in a JSON file for further processing.
"""

class NewbalanceSpider(Spider):
    name = "NewBalance"
    start_urls = ["https://www.newbalance.com.sa/en/"]
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
        "ZYTE_API_TRANSPARENT_MODE": True,
    }

    json_headers = {
        'Accept-Language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'Connection': 'keep-alive',
        'Origin': 'https://www.newbalance.com.sa',
        'Referer': 'https://www.newbalance.com.sa/en/shop-women/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'accept': 'application/json',
        'content-type': 'application/x-www-form-urlencoded',
    }

    graph_headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        'store': 'sau_en',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
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

    def parse(self, response, **kwargs):
        """Extract API credentials and initiate category scraping."""
        try:
            # data_dict = json.loads(response.css('script:contains("algoliaSearch")::text').get(''))
            script_content = response.css('script:contains("algoliaSearch")::text').get()
            data_dict = json.loads(script_content) if script_content else {}

            application_id = data_dict.get('algoliaSearch', {}).get('application_id', '')
            api_key = data_dict.get('algoliaSearch', {}).get('api_key', '')

            if not application_id or not api_key:
                self.write_logs("Missing Algolia API credentials. Aborting scrape.")
                return

            categories = ['men', 'women', 'kids']
            for category in categories:
                data = self.get_formdata(name=category, page=0)  # 0 page mean page no 1
                api_url = f'https://{application_id.lower()}-3.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia for JavaScript (3.35.1); Browser (lite)&x-algolia-application-id={application_id}&x-algolia-api-key={api_key}'
                meta = {'category': category, 'app_id': application_id, 'api_key': api_key}
                yield Request(url=api_url, callback=self.pagination, headers=self.json_headers,
                              method='POST', body=data, meta=meta)
        except Exception as e:
            self.write_logs(f"Error in parse method: {e}")


    def pagination(self, response):
        """Handles pagination by iterating through product pages."""
        category = response.meta.get('category', '')
        try:
            data_dict = response.json().get('results', [])[0]
            # print('Current Page No, get response', data_dict.get('page', 0))
            total_pages = data_dict.get('nbPages', 0)
            total_records = data_dict.get('nbHits', 0)
            yield from self.products_details(response)
        except:
            data_dict = {}
            return

        # # print('Current Page No, get response', data_dict.get('page', 0))
        # total_pages = data_dict.get('nbPages', 0)
        # total_records = data_dict.get('nbHits', 0)
        #
        # yield from self.products_details(response)

        if not response.meta.get('pagination'):
            print(f"Category: '{category}' contains {total_records} records spread across {total_pages} pages.")
            self.items_found += int(total_records)

            for pg_no in range(0, total_pages):
                response.meta['pagination'] = True
                data = self.get_formdata(name=category, page=pg_no)
                # api_url = f'https://{application_id.lower()}-3.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia for JavaScript (3.35.1); Browser (lite)&x-algolia-application-id={application_id}&x-algolia-api-key={api_key}'
                yield Request(url=response.url, callback=self.pagination, headers=self.json_headers,
                              method='POST', body=data, dont_filter=True, meta=response.meta)

    def products_details(self, response):
        """Extracts product details from API response."""
        category = response.meta.get('category', '')
        try:
            products = response.json().get('results', [])[0].get('hits', '')
            for product in products:
                try:
                    url = product.get('url', {}).get('en', '')
                    url = url.split('/')[-1] if url and '/' in url else url
                    url = ''.join(url.split('.')[0:1])
                    data = self.graphql_formdata(url)
                    response.meta['product'] = product
                    proxies = {scheme: "http://f693db95c418475380b0e70954ed0911:@api.zyte.com:8011" for scheme in
                               ("http", "https")}

                    resp = requests.get('https://www.newbalance.com.sa/graphql', params=data, headers=self.graph_headers, proxies=proxies, verify=False)
                    if resp.status_code == 200:
                        yield from self.parse_product_detail(response=resp, product=product)
                except:
                    continue

        except Exception as e:
            self.write_logs(f"Error in products_details method: {e}")

    def parse_product_detail(self, response, product):
        """Parses detailed product information."""
        try:
            p_dict = response.json().get('data', {}).get('products', {}).get('items', [])[0]
        except Exception as e:
            p_dict = {}
            a=1

        try:
            item = OrderedDict()
            url = product.get('url', {}).get('en', '')
            url = f'https://www.newbalance.com.sa{url}' if url else ''
            current_price = product.get('gtm', {}).get('gtm-price', '')
            old_price = product.get('gtm', {}).get('gtm-old-price', '')
            discount = product.get('discount', {}).get('en', 0.0)
            category = product.get('gtm', {}).get('gtm-category', '')
            stock_quantity = product.get('stock_quantity', 0)
            date_str = product.get('attr_updated_at', {}).get('en', '')

            item['source_id'] = 'new_balance'
            # item['product_url'] = urljoin(self.base_url, url)
            item['product_url'] = url
            item['brand'] = product.get('attr_brand', {}).get('en', '')
            item['product_title'] = p_dict.get('name', '') or product.get('title', {}).get('en', '')
            item['product_id'] = p_dict.get('sku', '') or product.get('gtm', {}).get('gtm-product-sku', '')
            item['category'] = category.replace('/', ', ') if category else ''
            item['price'] = float(current_price)
            item['discount'] = float(discount) if discount else 0.0
            item['currency'] = 'SAR'
            item['description'] = self.get_des(p_dict)
            item['main_image_url'] = ''.join([img.get('url', '').replace('product_listing', 'product_zoom_medium_606x504') for img in product.get('media', [])][0:1])
            item['other_image_urls'] = [img.get('url', '').replace('product_listing', 'product_zoom_medium_606x504') for img in product.get('media', [])][1:]
            item['colors'] = product.get('attr_color', {}).get('en', [])
            item['variations'] = {}
            item['sizes'] = self.get_sizes(response, product, p_dict)
            item['other_details'] = self.get_other_details(response, product, p_dict)
            item['availability'] = p_dict.get('stock_status', '').lower().replace(' ', '_') or product.get('gtm', {}).get('gtm-stock', '').title()
            item['number_of_items_in_stock'] = stock_quantity if stock_quantity != 0 else ''
            item['last_update'] = datetime.fromisoformat(date_str).strftime('%Y-%m-%d %H:%M:%S') if date_str else ''
            item['creation_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            self.items_scraped_count += 1
            yield item
        except Exception as e:
            print(f'error in item yield :{e}')

    def get_formdata(self, name, page):
        name_title = f'{name.title()}'
        data = f'{{"requests":[{{"indexName":"nb_prod_sa_product_list","params":"clickAnalytics=true&facets=%5B%22*%22%5D&filters=(stock%20%3E%200)%20AND%20(field_category_name.en.lvl0%3A%20%22{name_title}%22)&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=36&optionalFilters=null&page={page}&ruleContexts=%5B%22{name}%22%2C%22web__{name}%22%5D&analytics=true"}}]}}'
        return data

    def get_sizes(self, response, product, p_dict):
        info = {}
        try:
            size_list = [s.get('attributes', []) for s in p_dict.get('variants', [])]
            sizes = list(sorted(set([item['label'] for sublist in size_list for item in sublist if item['code'] == 'size'])))
            if sizes:
                info['us'] = sizes
        except:
            sizes = []

        return info

    def get_des(self, p_dict):
        try:
            html = p_dict.get('description', {}).get('html', '')
            resp = Selector(text=html)
            text = ', '.join(resp.css('::text').getall())
        except:
            text = ''

        return text


    def get_other_details(self, response, product, p_id):
        info = {}
        try:
            html = p_id.get('feature_bullets', '')
            resp = Selector(text=html)
            text = ', '.join(resp.css('::text').getall())
            info['features'] = text  # Correct syntax for assigning to a dictionary key
        except:
            text = ''

        article_no = [p.get('product', {}).get('article_number', '') for p in p_id.get('variants', [])]
        if article_no:
            article_no = ', '.join(set(article_no))
        if article_no:
            info['style'] = article_no

        return info

    def graphql_formdata(self, url):
        params = {
            'query': 'query($url:String){products(filter:{url_key:{eq:$url}}){total_count items{sku id type_id name description{html}short_description{html}url_key is_buyable stock_status express_delivery same_day_delivery ship_to_store is_returnable reserve_and_collect swatch_image swatch_image_url free_gift_promotion{rule_id rule_type rule_web_url rule_name rule_description auto_add max_gift coupon_code total_items gifts{id sku name}}price_range{maximum_price{regular_price{value}final_price{value}discount{percent_off}}minimum_price{regular_price{value}final_price{value}discount{percent_off}}}brand_logo_data{url alt title}brand_logo media_gallery{url label styles ... on ProductVideo{video_content{media_type video_provider video_url video_title video_description video_metadata}}}gtm_attributes{id name variant price brand category dimension2 dimension3 dimension4}meta_title meta_description meta_keyword og_meta_title og_meta_description stock_data{max_sale_qty qty}promotions{context url label type}... on ConfigurableProduct{maximum_discounted_price{final_price regular_price discount}configurable_options{attribute_uid label position attribute_code values{value_index store_label}}variants{product{id sku meta_title stock_status express_delivery same_day_delivery ship_to_store is_returnable reserve_and_collect attribute_set_id swatch_data{swatch_type}free_gift_promotion{rule_id rule_type rule_web_url rule_name rule_description auto_add max_gift coupon_code total_items gifts{id sku name}}price_range{maximum_price{regular_price{value}final_price{value}discount{percent_off}}minimum_price{regular_price{value}final_price{value}discount{percent_off}}}stock_data{qty max_sale_qty}media_gallery{url label styles ... on ProductImage{url label}... on ProductVideo{video_content{media_type video_provider video_url video_title video_description video_metadata}}}swatch_image swatch_image_url promotions{context url label type}width_description weight weight_text uk_size european_size article_number color size}attributes{label code value_index}}}category_ids_in_admin breadcrumb_category_id categories{id name level url_path include_in_menu breadcrumbs{category_name category_id category_level category_url_key category_url_path category_gtm_name}gtm_name}gender nb_product_type primary_material technologies feature_bullets green_leaf_notice green_leaf}}}',
            'variables': f'{{"url":"{url}"}}',
        }

        return params

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
