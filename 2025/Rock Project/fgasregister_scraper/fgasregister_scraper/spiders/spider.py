import json, unicodedata
from math import ceil
from datetime import datetime
from collections import OrderedDict

from scrapy import Spider, Request


class FgasregisterSpider(Spider):
    name = "Fgasregister"
    allowed_domains = ["api.shocklogic.com"]
    current_dt = datetime.now().strftime("%d%m%Y%H%M")
    base_url = 'https://api.shocklogic.com/v1.1/SL-API-62a09bfec2a6e/MemberDirectory/Live/Limit/5/Page/{}/FilterBy/Is_Contact_For_Group/1/Activity/401'

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 400, 403],

        'FEEDS': {
            f'output/{name} Companies Details_{current_dt}.csv': {
                'format': 'csv',
                'encoding': 'utf8',
                'indent': 4,
                'fields': ['Company Name', 'Address', 'Zip Code', 'Phone Number', 'Website URL']
            }
        }
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'origin': 'https://sites.shocklogic.com',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://sites.shocklogic.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        yield Request(url=self.base_url.format(1), headers=self.headers)

    def parse(self, response, **kwargs):
        try:
            data_dict = response.json()
        except json.JSONDecodeError as e:
            data_dict = {}
            print(f'Func Parse: Json Error :{e}')
            return

        total_records = data_dict.get('total_nr_records', 0)
        total_pages = ceil(total_records / 5)

        for page in range(1, total_pages + 1):
            url = self.base_url.format(page)
            yield Request(url, headers=self.headers, callback=self.parse_page_listings, dont_filter=True)

    def parse_page_listings(self, response):
        try:
            data_dict = response.json()
        except json.JSONDecodeError as e:
            data_dict = {}

        records = [data_dict[key] for key in data_dict if key.isdigit()]

        for record in records:
            item = OrderedDict()
            phone_value = record.get('Telephone', '')
            item['Company Name'] = self.remove_accents(record.get('Company', ''))
            item['Address'] = self.remove_accents(self.full_address(record))
            item['Zip Code'] = self.remove_accents(record.get('Zip_Code', ''))
            item['Phone Number'] = '=' + '"' + self.remove_accents(str(phone_value).strip()) + '"'
            item['Website URL'] = self.remove_accents(record.get('URL', ''))

            yield item

    def full_address(self, record):
        address_parts = [
            record.get('Address_1', '').strip(),
            record.get('Address_2', '').strip(),
            record.get('City', '').strip(),
            record.get('Address_3', '').strip(),
            record.get('Country_Name', '').strip(),
            record.get('Zip_Code', '').strip()
        ]

        full_address = ', '.join(part for part in address_parts if part)
        return full_address

    def remove_accents(self, string):
        normalized = unicodedata.normalize('NFD', string)
        return ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')