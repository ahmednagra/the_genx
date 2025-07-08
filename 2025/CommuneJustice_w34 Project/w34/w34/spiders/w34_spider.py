import openpyxl
import unicodedata
import csv, os, re, json, glob
from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict, defaultdict
from playwright.sync_api import sync_playwright

from scrapy import Spider, Request


class W34Spider(Spider):
    name = "w34_spider"
    allowed_domains = ["commissaire-justice.fr", "location.cncj.org"]
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {

        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'DOWNLOAD_TIMEOUT': 70,
        'CONCURRENT_REQUESTS': 8,

        'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter',},
        'FEEDS': {
            f'output/Commune Justice France records_{current_dt}.xlsx': {
                'format': 'xlsx',
                'fields':['Code_commune_INSEE', 'Code_postal', 'Libellé_d_acheminement', 'Nom_de_la_commune', 'Ligne_5', 'Office', 'City Address',
                          'Adresse', 'Téléphone', 'E-mail', 'Langue(s) maîtrisée(s)', 'Fax', 'Site Internet', 'Numéro d urgence',
                          'Activités principales', 'Activités accessoires', 'URL', 'Comments']
            },
        },
        }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'referer': 'https://commissaire-justice.fr/annuaire/',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    }
    api_headers = {
        'accept': '*/*',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'apikey': 'f8D2Vk1rYqvHat8fW67azn5E1Hj49MG13gj',
        'cache-control': 'no-cache',
        'origin': 'https://commissaire-justice.fr',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://commissaire-justice.fr/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scraped = 0

        # Logs
        os.makedirs('logs', exist_ok=True)
        os.makedirs('output', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_logs {self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')
        # self.previous_records= self.read_excel_file()
        self.search_records = self.read_csv_file()
        a=1

    def start_requests(self):
        for search_row in self.search_records:
            commune = search_row.get('Nom_de_la_commune_clean', '').strip()
            search_code = search_row.get('#Code_commune_INSEE', '').strip()
            search_postal_code = search_row.get('Code_postal', '').strip()
            search_key = f'{search_code}_{search_postal_code}'
            # if search_key in self.previous_records:
            #     self.write_logs(f'[SKIP] Found in previous records: {search_key}')
            #     for prev_record in self.previous_records[search_key]:
            #         yield prev_record  # Assuming each record is a Request-like object or a dictionary for further processing
            # else:
            url = f'https://location.cncj.org/from-address?term={commune}%20({search_postal_code})'
            yield Request(url, headers=self.api_headers, meta={'search_row':search_row}, dont_filter=True)

    def parse(self, response, **kwargs):
        search_row = response.meta.get('search_row', {})
        search_zipcode = search_row.get('Code_postal', '')
        try:
            json_dict = response.json()
        except json.JSONDecodeError as e:
            self.write_logs(f'Json error :{response.url}')
            return

        commune_clean = search_row.get('Nom_de_la_commune_clean')
        acheminement_clean = search_row.get('Libellé_d_acheminement_clean')

        matched_item = None
        try:
            for dict_row in json_dict:
                label_clean = self.clean_text(dict_row.get('label', ''))
                item_zipcode = dict_row.get('zipcode', '')
                if label_clean == commune_clean or label_clean == acheminement_clean or label_clean.replace(' ', '') == commune_clean.replace(' ', '')  or label_clean.replace(' ', '') == acheminement_clean.replace(' ', '') and str(search_zipcode) == str(item_zipcode):
                    matched_item = dict_row
                    break
        except:
            a=1

        if matched_item:
            address = matched_item.get('address','')
            zipcode = matched_item.get('zipcode', '')
            lat = matched_item.get('latitude', '')
            lon = matched_item.get('longitude', '')
            detail_page_url = f"https://commissaire-justice.fr/annuaire-resultats/?term={commune_clean.upper()}&city-gmap={address} ({zipcode})&lat={lat}&lon={lon}"
            yield Request(url=detail_page_url, headers=self.headers, callback=self.parse_listings, meta=response.meta, dont_filter=True)
        else:
            item = self.office_info(response, comments='Name Search Results Not Found')
            yield item

    def parse_listings(self, response):
        offices_urls = response.css('.search-results-office a.search-results-button:contains("Détails") ::attr(href)').getall()
        if  offices_urls:
            for office_url in offices_urls:
                url = urljoin(response.url, office_url)
                yield Request(url, headers=self.headers, callback=self.parse_detail_page, meta=response.meta, dont_filter=True)
        else:
            office_name = response.css('.search-results-office-name ::text').get('')

            if office_name:
                item = self.office_info(response, comments='')
                yield item
            else:
                item = self.office_info(response, comments='No office Exist For this record')
                yield item

    def parse_detail_page(self, response):
        item = self.office_info(response, comments='')
        yield item

    def read_csv_file(self):
        try:
            file_name = glob.glob('input/019HexaSmal (1).csv')[0]
            with open(file_name, mode='r', encoding='ISO-8859-1') as csv_file:
                reader = csv.DictReader(csv_file, delimiter=';')
                rows = []
                for row in reader:
                    row['Nom_de_la_commune_clean'] = self.clean_text(row.get('Nom_de_la_commune', ''))
                    row['Libellé_d_acheminement_clean'] = self.clean_text(row.get('Libellé_d_acheminement', ''))
                    rows.append(row)
                return rows
        except Exception as e:
            print(f"Error reading CSV: {e}")
            return []

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def clean_text(self, text: str) -> str:
        if not text:
            return ''
        text = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("utf-8")
        text = re.sub(r"[^\w\s]", "", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip().lower()

    def office_info(self, response, comments=None):
        search_row = response.meta.get('search_row', {})
        item = OrderedDict()
        item['Code_commune_INSEE'] = search_row.get('#Code_commune_INSEE', '')
        item['Code_postal'] = search_row.get('Code_postal', '')
        item['Libellé_d_acheminement'] = search_row.get('Libellé_d_acheminement', '')
        item['Nom_de_la_commune'] = search_row.get('Nom_de_la_commune', '')
        item['Ligne_5'] = search_row.get('Ligne_5', '')
        item['Comments'] = comments

        try:
            item['Office'] = response.css('.search-results-office-name.study-name ::text').get('')
            item['City Address'] = response.css('.search-results-city.study-address ::text').get('')
            item['Adresse'] = ', '.join(
                [t.strip() for t in response.css('.study-informations-title:contains("Adresse") + div ::text').getall() if
                 t.strip()])
            item['Téléphone'] = response.css('.study-informations-title:contains("Téléphone") + div a::text').get('')
            item['E-mail'] = response.css('.study-informations-title:contains("E-mail") + div a::text').get('')
            item['Langue(s) maîtrisée(s)'] = ', '.join([t.strip() for t in response.css(
                '.study-informations-title:contains("Langue(s) maîtrisée(s)") + div ::text').getall()])
            item['Fax'] = response.css('.study-informations-title:contains("Fax") + div ::text').get('').strip()
            item['Site Internet'] = response.css('.study-informations-title:contains("Site Internet") + div a::text').get(
                '').strip()
            item['Numéro d urgence'] = response.css('.study-informations-title:contains("Numéro d\'urgence") + div a::text').get('').strip()
            item['Activités principales'] = ', '.join([t.strip() for t in response.css('.study-informations-title:contains("Activités principales :") + div ::text').getall() if t.strip()])
            item['Activités accessoires'] = ', '.join([t.strip() for t in response.css('.study-informations-title:contains("Activités accessoires :") + div ::text').getall() if t.strip()])
            item['URL'] = response.url

            self.scraped +=1
            print('items Scraped', self.scraped)
            return item
        except Exception as e:
            self.scraped += 1
            print('items Scraped', self.scraped)
            return item

    def read_excel_file(self):
        filename = glob.glob('output/Commune Justice France records_090520252332.xlsx')[0]

        workbook = openpyxl.load_workbook(filename)
        sheet = workbook.worksheets[0]
        column_names = [cell.value for cell in sheet[1]]

        data = defaultdict(list)
        for row in sheet.iter_rows(values_only=True, min_row=2):
            row_dict = {column_names[i]: row[i] for i in range(len(column_names))}
            code = row_dict.get('Code_commune_INSEE', '').strip()
            code_postal = row_dict.get('Code_postal', '').strip()
            if code:
                key = f'{code}_{code_postal}'
                data[key].append(row_dict)

        return dict(data)
