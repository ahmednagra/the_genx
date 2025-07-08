import csv, glob, os, re
from time import sleep
from datetime import datetime
from collections import OrderedDict

from scrapy import Spider, Request, Selector, signals

from selenium.webdriver import Keys
from undetected_chromedriver import Chrome
from selenium.webdriver.common.by import By


class RedfinLeadsSpider(Spider):
    name = "Redfin"
    allowed_domains = ["www.redfin.com"]

    # Get the directory containing the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.abspath(os.path.join(script_dir, '..'))

    current_dt = datetime.now().strftime("%d%m%Y%H%M")
    custom_settings = {
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'DOWNLOAD_TIMEOUT': 70,
        'FEEDS': {
            f'output/{name} Properties Details_{current_dt}.csv': {
                'format': 'csv',
                'encoding': 'utf8',
                'fields': ['Address', 'City', 'State','Zip','Home Address','CRMLS', 'GPSMLS', 'Agent Name1', 'DRE #1', 'Company Name1', 'Tel Number1', 'Email1',
                           'Agent Name2', 'DRE #2', 'Company Name2', 'Tel Number2', 'Email2', 'URL']
            }
        },
    }

    url_headers = {
                    'accept': '*/*',
                    'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
                    'pragma': 'no-cache',
                    'priority': 'u=1, i',
                    'referer': 'https://www.redfin.com/',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
                   }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.items_skipped = 0

        # Initialize Logs
        os.makedirs('logs', exist_ok=True)
        os.makedirs('output', exist_ok=True)

        self.filepath = f'output/{self.name} Properties Details.csv'
        self.fields = ['Address', 'City', 'State','Zip','Home Address','CRMLS', 'GPSMLS', 'Agent Name1', 'DRE #1',
                       'Company Name1', 'Tel Number1', 'Email1', 'Agent Name2', 'DRE #2', 'Company Name2', 'Tel Number2', 'Email2', 'URL']

        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

        self.search_addresses = self.read_input_file()
        self.scraped_search_addresses = self.read_previous_scraped()
        self.total_search_addresses = len(self.search_addresses)
        self.total_scrape_addresses = 0

        self.driver = Chrome()

    def parse(self, response, **kwargs):
        address_row = response.meta.get('current_record', {})

        try:
            address_response, url = self.search_addres(address_row)
            if address_response:
                yield from self.parse_property_detail(response=address_response, url=url, search_address = address_row)
            else:
                self.logger.warning(f"⚠️ Skipping {address_row} due to invalid response")
                self.items_skipped += 1
                return
        except Exception as e:
            self.logger.warning(f"⚠️ Error in parse() for {address_row}: {e}")
            self.items_skipped += 1

            # Always go to the next record
        self.spider_idle()

    def parse_property_detail(self, response, url, search_address):
        try:
            full_address = ''.join(response.css('.full-address ::text').getall()).strip()
            if not full_address:
                self.items_skipped += 1
                self.write_logs(f'Home address Not found: {url}')
                raise ValueError("Full address not found")
                # return

            agent_selectors = response.css('.agent-info-container')
            gpsmls = ''
            crmls= ''.join(text.strip() for text in response.css('.ListingSource--dataSourceName:contains("CRMLS") + span ::text').getall()if text.strip() != '#')
            if not crmls:
                gpsmls= ''.join(text.strip() for text in response.css('.ListingSource--mlsId ::text').getall()if text.strip() != '#')

            item = OrderedDict()
            item['Address'] = search_address.get('Address', '')
            item['City'] = search_address.get('City', '')
            item['State'] = search_address.get('State', '')
            item['Zip'] = search_address.get('Zip', '')
            item['Home Address'] = full_address
            item['CRMLS'] = crmls
            item['GPSMLS'] = gpsmls if gpsmls else ''
            item['URL'] = url

            if not agent_selectors:
                self.total_scrape_addresses += 1
                print('Items Scraped', self.total_scrape_addresses)
                self.write_csv(item)
                yield item
                return

            # Agents found — add agent details with suffixes
            for idx, agent in enumerate(agent_selectors, start=1):
                agent_name = agent.css('.agent-basic-details--heading span::text').get('')  or ''

                try:
                    company_name = ''.join(
                        [text.strip() for text in agent.css('.agent-basic-details--broker ::text').getall() if
                         text.strip().replace('•', '')][0:1])
                except:
                    company_name = ''

                tel_number = agent.css('[data-rf-test-id="agentInfoItem-agentPhoneNumber"] ::text').get('') or ''
                if not tel_number:
                    tel_text = ''.join(response.css(f'div:contains("{agent_name}") + div.listingContactSection::text').getall())
                    tel_match = re.search(r'\d{3}[-\s]?\d{3}[-\s]?\d{4}', tel_text)
                    tel_number = tel_match.group(0) if tel_match else ''

                email = agent.css('.email-addresses span + span::text').get('') or ''

                try:
                    agent_info = agent.css('.agentLicenseDisplay ::text').getall()
                    agent_dre = re.search(r'DRE\s*#\s*(\d+)', ''.join(agent_info)).group(1)
                except:
                    agent_dre = ''

                item[f'Agent Name{idx}'] = agent_name
                item[f'DRE #{idx}'] = agent_dre
                item[f'Company Name{idx}'] = company_name
                item[f'Tel Number{idx}'] = tel_number
                item[f'Email{idx}'] = email

            self.total_scrape_addresses += 1
            print('Items Scraped', self.total_scrape_addresses)
            self.write_csv(item)
            yield item

        except Exception as e:
            self.items_skipped += 1
            self.write_logs(f"❌ Error in parse_property_detail for {search_address}: {e}")

    def read_input_file(self):
        # Define the path to the input folder
        input_folder = os.path.join(self.project_dir, 'input')

        # Find the CSV file (handles dynamic file paths)
        csv_files = glob.glob(os.path.join(input_folder, 'Redfin *.csv'))
        file_path = csv_files[0] if csv_files else None

        if not file_path:
            print("No CSV file found in the 'input' folder.")
            return []

        result = []  # List to store dictionaries for each state

        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as csv_file:
                csv_reader = csv.DictReader(csv_file)

                # Collect data from CSV
                for row in csv_reader:
                    result.append(row)

            return result

        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return []
        except csv.Error:
            print(f"Error reading CSV file: {file_path}")
            return []
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return []

    def read_previous_scraped(self):
        # Initialize an empty dictionary for player data
        records = {}

        try:
            with open(self.filepath, mode='r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    key= f"{row.get('Address', '')}, {row.get('City', '')}, {row.get('State', '')}, {row.get('Zip', '')}"
                    records[key] = row

            return records

        except FileNotFoundError:
            print(f"File not found: {self.filepath}")
            return {}
        except Exception as e:
            print(f"Error reading file: {e}")
            return {}

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def write_csv(self, item):
        """
        Efficiently writes a single item to the CSV file.
        Writes headers if the file does not already exist.
        Includes error handling for reliable file operations.
        """
        try:
            file_exists = os.path.isfile(self.filepath) and os.path.getsize(self.filepath) > 0

            with open(self.filepath, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fields)

                if not file_exists:
                    writer.writeheader()

                writer.writerows([item])
                csvfile.flush()

            self.write_logs(f"✅ Successfully wrote item to CSV.")

        except (FileNotFoundError, PermissionError, IOError) as e:
            self.write_logs(f"❌ Error occurred while writing to CSV: {e}")
        except Exception as e:
            self.write_logs(f"❌ Unexpected error in 'write_csv': {e}")

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def search_addres(self,address_row):
        zip_code = address_row.get('Zip', '')
        state = address_row.get('State', '')
        search_format = f"{address_row.get('Address', '')}, {address_row.get('City', '')}, {state}, {zip_code}"

        try:
            self.driver.get("https://www.redfin.com/")
            sleep(5)

            input_id = 'search-box-input'
            search_input = self.driver.find_element(By.ID, input_id)

            # Clear and enter search text
            search_input.clear()
            search_input.send_keys(search_format)
            sleep(1)
            search_input.send_keys(Keys.ENTER)
            sleep(3)

            # Validate the response to ensure correct data is loaded
            response = Selector(text=self.driver.page_source)
            url = self.driver.current_url
            self.logger.info(f"✅ Successfully loaded details for: {search_format}")
            sleep(3)
            return response, url

        except Exception as e:
            self.logger.error(f"❌ Error fetching details for {search_format}: {e}")
            return '', ''

    def close(spider, reason):
        """Shutdown logic when the spider closes. Logs summary statistics."""

        spider.write_logs("=" * 60)
        spider.write_logs(f"📌 Spider Name            : {spider.name}")
        spider.write_logs(f"🚀 Start Time             : {spider.script_starting_datetime}")
        spider.write_logs(f"📍 Total Input Records     : {spider.total_search_addresses}")
        spider.write_logs(f"✅ Successfully Scraped   : {spider.total_scrape_addresses}")
        spider.write_logs(f"⏭️ Skipped Records        : {spider.items_skipped}")
        spider.write_logs(f"🕔 End Time               : {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        spider.write_logs(f"📄 Close Reason           : {reason}")
        spider.write_logs("=" * 60)

        # Close the Chrome driver
        # spider.driver.close()
        try:
            if spider.driver:
                spider.driver.quit()
        except Exception as e:
            spider.write_logs(f"⚠️ Error during driver shutdown: {e}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(RedfinLeadsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        if self.search_addresses:
            self.write_logs(f"\n\n{len(self.search_addresses)}/{self.total_search_addresses} Home Addresses left to Scrape\n\n")

            current_record = self.search_addresses.pop()
            key = f"{current_record.get('Address', '')}, {current_record.get('City', '')}, {current_record.get('State', '')}, {current_record.get('Zip', '')}"

            if key in self.scraped_search_addresses:
                self.write_logs(f'{key} Already scraped last time.')
                self.spider_idle()
                return

            self.crawler.engine.crawl(Request(url='https://books.toscrape.com', callback=self.parse,
                                              dont_filter=True, meta={'current_record': current_record}))
