import re, csv
from datetime import datetime
from urllib.parse import quote, urljoin

from scrapy import Spider, Request


class PeopleSpider(Spider):
    name = "True_People"
    start_urls = ["https://www.truepeoplesearch.com/"]
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        'DOWNLOADER_CLIENT_TLS_VERIFY': False,
        'OFFSITE_ENABLED': False,
        'RETRY_TIMES': 10,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408, 429],
        'CONCURRENT_REQUESTS': 1,
        'FEED_EXPORTERS': {
            'xlsx': 'scrapy_xlsx.XlsxItemExporter',
        },
        'FEEDS': {
            f'outputs/Truepeoplesearch_{datetime.now().strftime("%d%m%Y%H%M")}.xlsx': {
                'format': 'xlsx',
                'fields': ['Input_First_Name', 'Input_Last_Name', 'Input_Property_Address', 'Input_Property_City',
                           'Input_Property_State', 'Input_Property_Zip', 'Mailing_Zip', 'Mailing_State', 'Mailing_City',
                           'Mailing_Address', 'Phone1_Number', 'Phone2_Number', 'Phone3_Number', 'Phone4_Number',
                           'Phone5_Number', 'Phone6_Number', 'Phone7_Number', 'Phone8_Number', 'Phone9_Number',
                           'Phone10_Number',
                           'Email1_Number', 'Email2_Number', 'Email3_Number', 'Email4_Number', 'Email5_Number',
                           'Email6_Number', 'Email7_Number', 'Email8_Number', 'Email9_Number', 'Email10_Number',
                           'Email11_Number', 'Email12_Number', 'Email13_Number', 'Email14_Number', 'Email15_Number',
                           'APN_PRESERVED', 'County_PRESERVED', 'County Name_PRESERVED',
                           'Owner Name(s) Formatted_PRESERVED', 'Last Transaction Sale Date_PRESERVED',
                           'Last Transaction Sale Price_PRESERVED', 'Lot Size SF / Acre_PRESERVED',
                           'Building / Living Area SF_PRESERVED', 'Detailed Property Type_PRESERVED',
                           'Year Built_PRESERVED', 'Property Type_PRESERVED']
            }
        }
    }

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.proxy = 'http://scraperapi.ultra_premium=true.country_code=us.render=true:ade849058d13d7e7a8e0fb3a303af656@proxy-server.scraperapi.com:8001' #new key

    def start_requests(self):
        try:
            rows = list(csv.DictReader(open('input/Multi Tenant First Last Names (1).csv', 'r', encoding='utf-8')))
            self.logger.info(f'Total records found in CSV file: {len(rows)}')

            for row in rows:
                address = row.get('Mailing_Address', '').strip()
                city = row.get('Mailing_City', '').strip()
                state = row.get('Mailing_State', '').strip()
                zip_code = row.get('Mailing_Zip', '').strip()
                if address and zip_code:
                    city_state_zip = quote(f" {state} {zip_code}")
                    url = f'https://www.truepeoplesearch.com/resultaddress?streetaddress={quote(address)}&citystatezip={quote(city)},{city_state_zip}'
                    yield Request(url, headers=self.headers, meta={'search_row': row, 'proxy': self.proxy})
        except Exception as e:
            self.logger.error(f"Error reading input CSV: {e}")

    def parse(self, response, **kwargs):
        search_row = response.meta.get('search_row', {})
        search_f_name = search_row.get('Input_First_Name', '').strip().lower()
        search_l_name = search_row.get('Input_Last_Name', '').strip().lower()
        search_full_name = f"{search_f_name} {search_l_name}"

        try:
            results = response.css('.card-summary')
            found_full = False
            found_partial = False

            for result in results:
                result_name = result.css('.content-header ::text').get(default='').strip().lower()
                detail_page_url = result.css('.detail-link::attr(href), .card-summary::attr(data-detail-link)').get('')

                if search_full_name in result_name:
                    url = urljoin(response.url, detail_page_url)
                    self.logger.info(f"✅ Full match: {search_full_name} → {result_name}")
                    yield Request(url, callback=self.detail_page, meta={'search_row': search_row, 'proxy': self.proxy})
                    found_full = True
                    return

            # If neither full nor partial match found
            if not found_full and not found_partial:
                self.logger.warning(f"❌ No match found for: {search_full_name}")
                yield search_row

        except Exception as e:
            self.logger.error(f"Error parsing results page: {str(e)}")
            yield search_row

    def detail_page(self, response):
        updated_row = dict(self.update_phone_numbers(response)) # added new phone no in the search Row
        emails = [email.strip() for email in response.css('.row.pl-md-1:contains("Email Addresses") .pl-sm-2 .col div::text').getall() if email.strip()]
        for index, email in enumerate(emails[:15], start=1):
            updated_row[f"Email{index}_Number"] = email

        yield  updated_row

    def update_phone_numbers(self, response):
        try:
            row = response.meta.get('search_row', {})
            existing_phone_no = {re.sub(r'\D', '', row.get(f'Phone{i}_Number', '')) for i in range(1, 11)} - {''}

            extracted_phones = response.css('[data-link-to-more="phone"] [itemprop="telephone"]::text').getall()
            extracted_phone_no = [re.sub(r'\D', '', phone) for phone in extracted_phones if re.sub(r'\D', '', phone)]
            new_phones = [phone for phone in extracted_phone_no if phone not in existing_phone_no]

            for i in range(1, 11):
                key = f'Phone{i}_Number'
                if not row.get(key) and new_phones:
                    row[key] = new_phones.pop(0)
            return row
        except Exception as e:
            self.logger.error(f"Error updating phone numbers: {str(e)}")
            return response.meta.get('search_row', {})

    def get_form_data(self, url):
        json_data = {
            'apiKey': 'ade849058d13d7e7a8e0fb3a303af656',
            'urls': [
                url,
            ],
        }

        return json_data