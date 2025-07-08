import re, os, json, csv, html, unicodedata
from collections import OrderedDict
from datetime import datetime
from scrapy import Spider, Request
from scrapy.utils.log import logger


class ZazzleSpider(Spider):
    name = "Zazzle"
    allowed_domains = ["www.zazzle.com"]
    start_urls = ["https://www.zazzle.com/"]
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        'OFFSITE_ENABLED': False,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 408],
        'CONCURRENT_REQUESTS': 2,
        'FEED_EXPORTERS': {
            'xlsx': 'scrapy_xlsx.XlsxItemExporter',
        },
        'FEEDS': {
            f'outputs/Zazzle Records_{current_dt}.xlsx': {
                'format': 'xlsx',
                'fields': ['Search URL', 'Product Title', 'Design Description', 'Breadcrumb Category',
                           'Product ID', 'Created Date', 'Tags / Keywords', 'Price', 'Image URL', 'URL']
            }
        },

        'ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED': True,

        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_zyte_api.ScrapyZyteAPIDownloadHandler',
            'https': 'scrapy_zyte_api.ScrapyZyteAPIDownloadHandler',
        },

        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware': 1000,
            'scrapy_poet.InjectionMiddleware': 543,
        },

        'REQUEST_FINGERPRINTER_CLASS': 'scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter',
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'ZYTE_API_TRANSPARENT_MODE': True,
        'ZYTE_API_KEY': '920b89d3ebd540638629cc657ae3ff3f',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scrape_urls_count = 0
        self.failed_scrape_count = 0
        self.search_urls = [line.strip() for line in open('input/Cleaned_Zazzle_Product_Links.csv', 'r', encoding='utf-8') if line.strip()]
        self.logger.info(f'[INFO] Total project URLs found in CSV: {len(self.search_urls)}')

    def start_requests(self):
        for url in self.search_urls:
            yield Request(url, meta={'search_url': url})

    def parse(self, response, **kwargs):
        print(f"Search URl: {response.meta.get('search_url', '')}")
        products_urls = list(set(response.css(
            '.ProfileProductsPublic_results .SearchResultsGridCell2_realviewContainer a::attr(href)').getall()))

        if products_urls:
            logger.info(f"Listing page detected: Found {len(products_urls)} product URLs")
            for p_url in products_urls:
                yield Request( url=response.urljoin(p_url), callback=self.parse, meta=response.meta)
            return

        item = self.extract_item_info(response)
        if item:
            self.scrape_urls_count += 1
            yield item
            return
        else:
            logger.warning(f"No product URLs or item found — likely an irrelevant or broken page: {response.url}")
            self.failed_scrape_count += 1
            yield {
                'Search URL': response.meta.get('search_url', ''),
                'Product Title': '',
                'Design Description': '',
                'Breadcrumb Category': '',
                'Product ID': '',
                'Created Date': '',
                'Tags / Keywords': '',
                'Price': '',
                'Image URL': '',
                'URL': response.url
            }

    def normalize_text(self, text):
        if not isinstance(text, str):
            return text

        text = unicodedata.normalize('NFKC', text).strip().encode().decode('unicode_escape')
        text = html.unescape(text).replace('\xa0', ' ')
        text = text.replace('\\"', '"').replace("\\'", "'").replace('\\\\', '\\').replace('\\n', '\n').replace('","', '').strip()
        return text

    def extract_item_info(self, response):
        search_url = response.meta.get('search_url', '')

        try:
            p_dict = json.loads(
                response.css('script:contains("content_type: \'product\',")').get('').split('contents: [')[1].split(
                    "],")[0].replace("'", '"'))
        except Exception as e:
            logger.warning(f"Failed to parse product dictionary: {e}")
            p_dict = {}

        try:
            raw_data = response.css('script:contains("ZData = JSON.parse")::text').re_first(
                r"ZData = JSON\.parse\('(.+?)'\);")
        except Exception as e:
            logger.warning(f"Failed to extract raw ZData from script: {e}")
            raw_data = ''

        try:
            p_title = response.css('[itemProp="name"] ::text').get('').strip()
            price = response.css('.Pricing_mainPrice ::text').get('')
            img_url = response.css('.ProductView-image::attr(src)').get('')
            breadcrumb_category = p_dict.get('category', '').replace('/', ' > ')
            design_description = self.get_desc(p_dict, response)
            p_id = re.search(r'-([0-9]+)$', response.url).group(1) if re.search(r'-([0-9]+)$', response.url) else ''
            tags_keywords = self.get_tags_keywords(raw_data)
            created_date = self.get_create_date(response)

            product_dict =  {
                'Search URL': search_url,
                'Product Title': p_title,
                'Design Description': design_description,
                'Breadcrumb Category': breadcrumb_category,
                'Product ID': p_id,
                'Created Date': created_date,
                'Tags / Keywords': tags_keywords,
                'Price': price,
                'Image URL': img_url,
                'URL': response.url
            }

            return product_dict
        except Exception as e:
            logger.error(f"Failed to extract item info from {response.url}: {e}")
            return ''

    def get_tags_keywords(self, raw_data):
        try:
            key_dict = json.loads(raw_data.split('"All Products","tags":')[1].split('}]},"promos"')[0])
        except:
            key_dict = {}
        keywords = ', '.join(tag['text'] for tag in key_dict if 'text' in tag)

        tag_key = keywords if keywords else ''
        return tag_key

    def read_input_csv_file(self, input_file):
        data = []
        try:
            with open(input_file, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            data = [line.strip() for line in lines if line.strip()]
            return data
        except FileNotFoundError:
            print(f"File '{input_file}' not found.")
            return data
        except UnicodeDecodeError as e:
            print(f"Unicode decode error: {e}")
            return data
        except Exception as e:
            print(f"An error occurred while reading the file: {str(e)}")
            return data

    def get_desc(self, p_dict, response):
        title = p_dict.get('name', '')
        raw_des = response.css('script:contains("ZData = JSON.parse") ::text').re_first(r'"descriptionInOriginalLanguage":"(.*)').split('designerId')[0]
        if raw_des:
            desc = f'{title} \n {self.normalize_text(raw_des)}'
            return desc
        return ''

    def get_create_date(self, response):
        try:
            time_dict = json.loads(response.css('script:contains("ZENV = JSON.parse")::text').re_first(r"ZENV = JSON\.parse\('(.*?)'\);"))
            server_time = time_dict.get('serverStartTimestamp', '')
            if server_time:
                dt = datetime.fromtimestamp(server_time / 1000)
                return dt.strftime("%m/%d/%Y, %I:%M %p")
        except json.JSONDecodeError:
            return ''

    def close(Spider, reason):
        """Runs when the spider finishes scraping."""
        Spider.logger.info("=" * 60)
        Spider.logger.info("\n📊 Scraping Completed - Here's a Quick Summary:")
        Spider.logger.info(f"🔍 Total search URLs provided: {len(Spider.search_urls)}")
        Spider.logger.info(f"✅ Successfully scraped URLs:  {Spider.scrape_urls_count}")
        Spider.logger.info(f"❌ Failed/Empty scraped URLs:  {Spider.failed_scrape_count}")
        Spider.logger.info(f"⏩ Skipped or Not Processed URLs : {len(Spider.search_urls) - (Spider.scrape_urls_count + Spider.failed_scrape_count)}")

        Spider.logger.info("\n⏱️ Script Timing Details:")
        Spider.logger.info(f"📌 Spider Name: '{Spider.name}'")
        Spider.logger.info(f"📅 Started at:   {Spider.current_dt}")
        Spider.logger.info(f"🏁 Finished at:  {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        Spider.logger.info(f"📝 Reason for closing: {reason}")
        Spider.logger.info("=" * 60)
