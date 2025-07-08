from collections import OrderedDict
from datetime import datetime
from typing import Iterable
from urllib.parse import urljoin

from scrapy import Spider, Request, Selector
from scrapy import Request


class BorculoSpider(Spider):
    name = "Borculo"
    allowed_domains = ["onlineveilingborculo.nl"]

    current_dt = datetime.now().strftime("%d%m%Y%H%M")
    custom_settings = {
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        # 'CONCURRENT_REQUESTS': 4,
        'FEED_EXPORTERS': {
            'xlsx': 'scrapy_xlsx.XlsxItemExporter',
        },
        'FEEDS': {
             f'output/{name} Horses Details_{current_dt}.xlsx': {
                'format': 'xlsx',
                'fields': ['Name', 'horse profile', 'pedigrees',
                           'images', 'videos', 'prices', 'auction', 'listings', 'results', 'URL']
            }
        },
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        urls = ['https://www.onlineveilingborculo.nl/en/collection/22',
            'https://www.onlineveilingborculo.nl/en/live-auction/23']
        #
        for url in urls:
            yield Request(url, headers=self.headers, callback=self.parse)
        # yield Request('https://www.onlineveilingborculo.nl/en/live-auction/23', headers=self.headers, callback=self.parse)

    def parse(self, response, **kwargs):
        horses_divs = response.css('#ended-auctions .hover-effect') or response.css('#auctions-ended .hover-effect')

        if not horses_divs:
            a=1

        for horse in horses_divs:
            h_name = horse.css('.horsename a::text').get('').strip()
            # h_pedigree = ''.join([text.strip() for text in horse.css('.horsepedigree ::text').getall()])
            h_pedigree = ''.join([text.strip() for text in horse.css('.horsepedigree').getall()])  #html
            # h_price= horse.css('span[id^="auction-price-"]::text').get('')
            h_price= horse.css('span[id^="auction-price-"]').get('') #html
            url = horse.css('.stretched-link ::attr(href)').get('')

            horse_info = {
                'h_name':h_name,
                'h_pedigree': h_pedigree,
                'h_price':h_price
            }

            yield Request(url=urljoin(response.url, url), callback=self.parse_horse, meta={'horse_info':horse_info}, headers=self.headers)

    def parse_horse(self, response):
        try:
            horse_info = response.meta.get('horse_info', {})
            item = OrderedDict()
            # name =  response.css('h1.main-title ::text').get('')
            name =  response.css('h1.main-title').get('') #html
            # video_link= response.css('[data-fancybox="horse"] ::attr(href)').get('')
            video_link= response.css('[data-fancybox="horse"]').get('')
            # images = [urljoin(response.url, href) for href in response.css('[data-fancybox="photos"] ::attr(href)').getall()]
            images = response.css('[data-fancybox="photos"]').getall()

            item['auction'] = ''
            item['listings'] = ''
            item['results'] = ''
            # item['horse profile'] = self.get_profile(response)
            item['horse profile'] = response.css('.auction-detail-info').get('')
            item['pedigrees'] = horse_info.get('h_pedigree', '')
            item['images'] = images
            item['videos'] = video_link
            item['prices'] = horse_info.get('h_price', '')
            item['URL'] = response.url
            item['Name'] = name
            yield item
        except Exception as e:
            a=2

    def get_profile(self, response):
        profile_data = {}
        table_rows = response.css('.auction-detail-info tr')

        for row in table_rows:
            label = row.css('th b::text').get('')
            value = row.css('td::text').get('')
            if label and value:
                profile_data[label.strip()] = value.strip()

        return ' | '.join(f'{k}: {v}' for k, v in profile_data.items())







