import csv, os, logging
from datetime import datetime
from collections import OrderedDict

from scrapy import Spider, Request

os.makedirs('output', exist_ok=True)

class SpiderSpider(Spider):
    name = "The_lawyers_of_distinction"
    allowed_domains = ["www.thelawyersofdistinction.com"]
    start_urls = ["https://www.thelawyersofdistinction.com"]
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        'OFFSITE_ENABLED': False,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'CONCURRENT_REQUESTS': 4,

        'FEED_EXPORTERS': {
            'xlsx': 'scrapy_xlsx.XlsxItemExporter',
        },
        'FEEDS': {
            f'output/{name} Lawyers Information_{current_dt}.xlsx': {
                'format': 'xlsx',
                'fields': ['Name', 'Website', 'Address', 'Profile URL'],
            }
        }
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scrape_urls_count = 0
        self.successful_profiles = 0

    def start_requests(self):
        urls = [row['Profile URL'].strip() for row in
                csv.DictReader(open('input/clean_lod_profile_urls (1).csv', 'r', encoding='utf-8')) if
                row['Profile URL'].strip()]

        self.scrape_urls_count += len(urls)
        logging.info(f'{len(urls)} URLs found in the input CSV file.')

        for url in urls:
            yield Request(url, headers=self.headers)

    def parse(self, response, **kwargs):
        try:
            item = OrderedDict()
            item['Name'] = response.css('.shade-box .user-name::text').get('').strip()
            item['Website'] = response.css('.shade-box .website-link::attr(href), .shade-box + .para a::attr(href)').get('').strip()
            item['Address'] = response.css('.address-main addresses::text').get('').strip()
            item['Profile URL'] = response.url

            if item['Name']:
                self.successful_profiles += 1
                logging.info(f"[{self.successful_profiles}] Extracted profile: {item['Name']}")
            else:
                logging.warning(f"Profile skipped (no name found): {response.url}")
                return

            yield item

        except Exception as e:
            logging.error(f"Error parsing {response.url}: {e}")

    def close(self, reason):
        logging.info("Spider finished.")
        logging.info(f"Total input Search profiles URLs: {self.scrape_urls_count}")
        logging.info(f"Profiles scraped successfully: {self.successful_profiles}")
        logging.info(f"Profiles failed/skipped: {self.scrape_urls_count - self.successful_profiles}")