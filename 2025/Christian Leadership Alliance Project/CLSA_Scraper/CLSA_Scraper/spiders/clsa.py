import os
from collections import OrderedDict
from datetime import datetime

from scrapy import Request, Spider, Selector


class ClsaSpider(Spider):
    name = "clsa"
    allowed_domains = ["christianleadershipalliance.org"]
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        # 'CONCURRENT_REQUESTS': 4,
        'FEED_EXPORTERS': {
            'xlsx': 'scrapy_xlsx.XlsxItemExporter',
        },
        'FEEDS': {
            f'output/CLSA Articles {datetime.now().strftime("%d%m%Y%H%M")}.xlsx': {
                'format': 'xlsx',
                'fields': [
                        'item_title', 'publication_date', 'item_image', 'text_summary', 'h2_title',
                        'Author', 'Publisher', 'type', 'text', 'url'
                    ]
            }
        },

    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'referer': 'https://christianleadershipalliance.org/blog',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }

    def __init__(self):
        super().__init__()
        self.item_scraped = 0
        self.item_found = 0

        # Create directories for output and logs if they don't exist
        os.makedirs('output', exist_ok=True)
        self.output_file_path = f'output/{self.name} Products Details.json'

        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        # self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

    def start_requests(self):
        url = "https://christianleadershipalliance.org/blog"
        yield Request(url, headers=self.headers, callback=self.parse,  meta={"handle_httpstatus_all": True})

    def parse(self, response, **kwargs):
        articles_selectors = response.css('.pp-content-post.pp-content-grid-post')

        for article in articles_selectors[:1]:
            item = OrderedDict()
            item['item_title'] = article.css('[itemprop="mainEntityOfPage"] ::attr("content")').get('')
            item['publication_date'] = article.css('[itemprop="dateModified"] ::attr("content")').get('')
            item['item_image'] = article.css('[itemprop="url"] ::attr("content")').get('')
            item['text_summary'] = article.css('.pp-content-grid-content p::text').get('')
            h2_title = article.css('.term-personal-leadership.parent-term::text').get('') or ''.join([t for t in article.css('.pp-content-category-list.pp-post-meta ::text').getall() if t.strip()])
            if not h2_title:
                a=1

            item['h2_title'] = h2_title
            item['Author'] = 'Christian Leadership'
            item['Publisher'] = 'Christian Leadership'
            item['type'] = 'Article'
            item['url'] = article.css('.pp-post-link::attr(href)').get('')
            url = article.css('.pp-post-link::attr(href)').get('')

            response.meta['item']=item
            yield Request(url, headers=self.headers, callback=self.parse_article, meta=response.meta)

        # Pagination
        next_page = response.css('.next.page-numbers::attr(href)').get('').strip()
        if next_page:
            yield Request(url=next_page, headers=self.headers, callback=self.parse)

    def parse_article(self, response):
        try:
            text = []

            # Select all h4 tags inside the '.fl-module.fl-module-fl-post-content' element
            headings = response.css('.fl-module.fl-module-fl-post-content h4.wp-block-heading')

            # Iterate over each h4 heading
            for heading in headings:
                # Extract the text from the h4 tag (get the strong text inside it)
                heading_text = heading.css('strong::text').get()
                heading_text = heading_text.strip() if heading_text else ''  # Ensure not None

                # Find the preceding siblings of the h4 tag (elements before this h4 in the same parent)
                preceding_siblings = heading.xpath('preceding-sibling::*')

                # Extract text from each preceding sibling and join them
                sibling_texts = ''.join([sibling.css('::text').get() or '' for sibling in preceding_siblings]).strip()

                # Combine the heading and preceding sibling texts into one item
                combined_text = heading_text + '\n' + sibling_texts

                # Append the combined text to the list
                text.append(combined_text)

            # Check if text was found, otherwise use an empty string
            text = '\n\n'.join(text) if text else ''
            if not text:
                text = ' '.join([t for t in response.css('.fl-module.fl-module-fl-post-content ::text').getall() if t.strip()])

            if not text:
                a=1

            # Get the item passed via meta
            item = response.meta.get('item', {})
            item['text'] = text  # Add the extracted text to the item

        except Exception as e:
            item = response.meta.get('item', {})
            item['text'] = ''  # In case of error, set text as empty
            print(f"Error occurred: {e}")  # Log the error for debugging

            # Yield the complete item with all the fields
        yield item

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def close(spider, reason):
        """Shutdown logic when the spider closes."""

        # Detailed log messages for better clarity
        spider.write_logs("=" * 50)
        spider.write_logs(f"🛑 Spider '{spider.name}' has finished running.")
        spider.write_logs(f"📅 Start Time: {spider.script_starting_datetime}")
        spider.write_logs(f"✅ New Records Found: {spider.item_found}")
        spider.write_logs(f"✅ Records Successfully Scraped: {spider.item_scraped}")
        spider.write_logs(f"🔚 Closing Reason: {reason}")
        spider.write_logs("=" * 50)