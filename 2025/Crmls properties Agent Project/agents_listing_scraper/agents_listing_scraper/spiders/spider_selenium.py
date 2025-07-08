import json
import os
import time
from datetime import datetime

import requests
from scrapy import Spider, Request, FormRequest, Selector
from selenium.common import TimeoutException

from seleniumwire import webdriver  # pip installs selenium-wire
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


class CrmlsSpider(Spider):
    name = "Crmls_selenium"
    allowed_domains = ["crmls.org", "member.recenterhub.com"]
    start_urls = ["https://member.recenterhub.com/?loginAor=XX"]

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
                'fields': []
            }
        },
        'URLLENGTH_LIMIT': 10000,  # Increase the limit beyond 2083'
        # "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
        # 'DOWNLOAD_HANDLERS': {
        #     "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        #     "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        # },
        # 'DOWNLOADER_MIDDLEWARES': {
        #     "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
        #     "scrapy_poet.InjectionMiddleware": 543,
        # },
        # 'REQUEST_FINGERPRINTER_CLASS': "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        # 'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        # 'ZYTE_API_KEY': "f693db95c418475380b0e70954ed0911",
        # "ZYTE_API_TRANSPARENT_MODE": True,
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'multipart/form-data; boundary=----WebKitFormBoundaryhiXEuIohdKxPX2dJ',
        'origin': 'https://matrix.crmls.org',
        'priority': 'u=1, i',
        'referer': 'https://matrix.crmls.org/Matrix/s?c=H4sIAAAAAAAEAItWMjY0NVfSUTICYlMDJZ280pwcdAIkiU0cP6FkSI4u2ppEE0Lp0MRDEw7ti0d3ZCwAH(HKlV0BAAA)',
        'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }

    def __init__(self):
        super().__init__()
        self.items_found = 0
        self.items_scraped = 0

        # Create directories for logs and output
        os.makedirs("output", exist_ok=True)
        os.makedirs("logs", exist_ok=True)

        # Logs
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

        self.driver = self.ini_driver()

    def start_requests(self):
        headers, cookies, current_url, token, post_request= self.selenium_login()
        subtypes= [('10279', 'Boat Slip'), ('2088', 'Cabin'), ('26222', 'Co-Ownership'), ('2089', 'Commercial Residential'), ('2090', 'Condominium'), ('7693', 'Deeded Parking'), ('7694', 'Duplex'), ('2096', 'Loft'), ('2098', 'Manufactured On Land'), ('2102', 'Own Your Own'), ('7695', 'Quadruplex'), ('2105', 'Single Family Residence'), ('2091', 'Stock Cooperative'), ('2107', 'Studio'), ('2109', 'Timeshare'), ('2110', 'Townhouse'), ('7696', 'Triplex')]
        counties = [('3428', 'Alameda'), ('3429', 'Alpine'), ('3430', 'Amador'), ('3431', 'Butte'), ('3432', 'Calaveras'), ('26102', 'Clark'), ('3434', 'Colusa'), ('3433', 'Contra Costa'), ('3435', 'del Norte'), ('3436', 'El Dorado'), ('3437', 'Foreign Country'), ('3438', 'Fresno'), ('3439', 'Glenn'), ('3440', 'Humboldt'), ('3441', 'Imperial'), ('3442', 'Inyo'), ('3444', 'Kern'), ('3443', 'Kings'), ('3446', 'Lake'), ('3447', 'Lassen'), ('3445', 'Los Angeles'), ('3448', 'Madera'), ('3455', 'Marin'), ('3454', 'Mariposa'), ('3449', 'Mendocino'), ('3450', 'Merced'), ('3452', 'Modoc'), ('3451', 'Mono'), ('3453', 'Monterey'), ('3456', 'Napa'), ('3457', 'Nevada'), ('3458', 'Orange'), ('3460', 'Other County'), ('3459', 'Other State'), ('3461', 'Placer'), ('3462', 'Plumas'), ('3463', 'Riverside'), ('3465', 'Sacramento'), ('3467', 'San Benito'), ('3466', 'San Bernardino'), ('3470', 'San Diego'), ('3471', 'San Francisco'), ('3475', 'San Joaquin'), ('3476', 'San Luis Obispo'), ('3477', 'San Mateo'), ('3464', 'Santa Barbara'), ('3468', 'Santa Clara'), ('3469', 'Santa Cruz'), ('3472', 'Shasta'), ('3473', 'Sierra'), ('3474', 'Siskiyou'), ('3478', 'Solano'), ('3479', 'Sonoma'), ('3480', 'Stanislaus'), ('3481', 'Sutter'), ('3482', 'Tehama'), ('3483', 'Trinity'), ('3484', 'Tulare'), ('3485', 'Tuolumne'), ('3486', 'Ventura'), ('3487', 'Yolo'), ('3488', 'Yuba')]
        verification_token = token if token else 'ezbbeUrnPINnL5JevgwRRnK_A6Dj5WpB-HqZEh-OcMgCuguWkvkDzB8Q4yZlaUOIVOi3QA2Qb1Ow1ep-2Bau6Hh_Aj41'

        for subtype_id, subtype_name in subtypes:
            for county_id, county_name in counties:
                records_list = self.get_records(subtype_id,subtype_name, county_id, county_name)
                parameters = self.get_files_paramaters(verification_token, subtype_id, county_id)
                form_data = {key: value[1] for key, value in parameters.items()}
                self.headers['referer'] = current_url

                resp = requests.post(url=current_url, cookies=cookies,
                    headers=headers,
                    files=parameters,
                    verify=False,
                )

                a=1
                # yield Request(url=current_url, cookies=cookies, headers=self.headers, body=json.dump(parameters), callback=self.parse, dont_filter=True, method='POST')
                # yield FormRequest(
                #     url=current_url,
                #     cookies=cookies,
                #     headers=headers,
                #     formdata=form_data,
                #     callback=self.parse,
                #     dont_filter=True,
                #     method='POST',
                #     meta={'handle_httpstatus_all': True, 'subtype':subtype_name, 'county':county_name}
                # )
    def parse(self, response, **kwargs):
        # Extract data from fully rendered JS page
        yield {"html": response.text}


    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def selenium_login(self):
        try:
            # Navigate to home page with explicit wait
            self.driver.get('https://go.crmls.org/')
            WebDriverWait(self.driver, 100).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            self.write_logs("Home page loaded successfully")

            # Navigate to login page
            self.driver.get('https://member.recenterhub.com/?loginAor=XX')
            WebDriverWait(self.driver, 5150).until(
                EC.presence_of_element_located((By.ID, 'Username'))
            )
            self.write_logs("Login page loaded successfully")

            # Fill login form (replace with your actual credentials)
            username = self.driver.find_element(By.ID, 'Username')
            password = self.driver.find_element(By.ID, 'Password')

            # Clear existing text and enter username
            username.clear()
            username.send_keys("U41280")
            password.clear()
            password.send_keys("Realestate12#")

            # Click login button with multiple assurance methods
            submit_button = WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[name="button"][value="login"]'))
            )

            # Try regular click first, fallback to JavaScript click
            try:
                submit_button.click()
            except Exception:
                self.driver.execute_script("arguments[0].click();", submit_button)

            # Wait for the page to fully load after login
            WebDriverWait(self.driver, 1000).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )

            # Wait until the username "Blake" is found in the page
            try:
                WebDriverWait(self.driver, 1000).until(
                    lambda driver: "Blake" in driver.find_element(
                        By.CSS_SELECTOR, 'button#headlessui-menu-button-2 span.text-secondary'
                    ).text
                )
                self.write_logs("Username 'Blake' found on the page and dashboard loaded")

            except TimeoutException:
                self.write_logs("Username 'Blake' not found within the timeout period")
                raise

            # Wait for the Matrix element to be present
            matrix_element = WebDriverWait(self.driver, 300).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-tip*="Matrix"]'))
            )
            self.write_logs("Matrix element found")

            # Use ActionChains to move to the element and click it
            actions = ActionChains(self.driver)
            actions.move_to_element(matrix_element).click().perform()
            self.write_logs("Matrix element clicked successfully")

            # Wait for the new tab to open and switch to it
            WebDriverWait(self.driver, 150).until(lambda d: len(d.window_handles) > 1)
            self.driver.switch_to.window(self.driver.window_handles[1])  # Switch to the new tab
            self.write_logs("Switched to the new tab")

            # Wait for the new page to load
            WebDriverWait(self.driver, 1000).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            self.write_logs("New tab page loaded successfully")

            self.driver.get('https://matrix.crmls.org/Matrix/Search/Residential/Detail')
            WebDriverWait(self.driver, 1000).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            self.write_logs("Residential page loaded successfully")

            new_search_button = WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.ID, 'm_lbSwitchTo3PanelSearch'))
            )
            self.write_logs("Found 'Try Our New Search' button")

            # Scroll the button into view (if needed)
            self.driver.execute_script("arguments[0].scrollIntoView(true);", new_search_button)

            # Click the button using ActionChains for reliability
            actions = ActionChains(self.driver)
            actions.move_to_element(new_search_button).click().perform()
            time.sleep(5)
            WebDriverWait(self.driver, 1000).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            self.write_logs("Clicked 'Try Our New Search' button successfully")

            # Fill Postal Code Input field
            post_code = self.driver.find_element(By.ID, 'Fm2_Ctrl19_TextBox')
            post_code.clear()
            post_code.send_keys("94501")
            post_code.send_keys(Keys.RETURN)
            self.write_logs("Entered postal code and pressed Enter")
            time.sleep(5)
            # Wait for the POST request to be triggered and capture it
            # post_request = [req for req in self.driver.requests if req.method == 'POST' and 'https://matrix.crmls.org/Matrix/s/Update?c=' in req.url][0]
            # Capture POST requests with retry
            post_request = None
            for _ in range(10):  # Retry for a few seconds
                post_requests = [req for req in self.driver.requests if
                                 req.method == 'POST' and 'https://matrix.crmls.org/Matrix/s/Update?c=' in req.url]
                if post_requests:
                    post_request = post_requests[0]
                    break
                time.sleep(1)

            headers_dict = dict(post_request.headers)

            try:
                token = post_request.body.decode('utf-8').split('RequestVerificationToken')[1].split('---')[0]
                clean_token = token.replace('\r', '').replace('\n', '').replace('"', '').strip()
            except:
                clean_token = ''

            # Retrieve cookies
            cookies = {cookie['name']: cookie['value'] for cookie in self.driver.get_cookies()}
            self.write_logs(f"Cookies: {cookies}")
            current_url = self.driver.current_url
            self.write_logs(f"Current URL in new tab: {current_url}")

            return headers_dict, cookies, current_url, clean_token, post_request
        except Exception as e:
            self.write_logs(f"Login failed: {str(e)}")
            return {}, {}, '', '', ''

    def get_files_paramaters(self, token, sub_type, county):
        # fm2_ctrl4_lb = str(10279)  # property Syb type
        fm2_ctrl4_lb = str(sub_type)  # property Syb type
        # fm2_ctrl9_lb = str('3428') # County id
        fm2_ctrl9_lb = str(county) # County id
        # fm2_ctrl10_lb = str('3790') # City ID
        fm2_ctrl10_lb = '' # City ID

        files = {
            '__RequestVerificationToken': (
            None, token),
            'responsiveSearch.SearchFormID': (None, '2'),
            'IsValid': (None, 'true'),
            'Fm2_Ctrl3_LB': (None, ''),
            'FmFm2_Ctrl3_25656_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6140_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6146_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6144_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6145_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6148_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6142_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6141_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6147_Ctrl3_TB': (None, ''),
            'Fm2_Ctrl4_LB_OP': (None, 'Or'),
            # 'Fm2_Ctrl4_LB': (None, '10279'),
            'Fm2_Ctrl4_LB': (None, fm2_ctrl4_lb),
            'Fm2_Ctrl19_TextBox': (None, ''),
            'Fm2_Ctrl9_LB_OP': (None, 'Or'),
            # 'Fm2_Ctrl9_LB': (None, '3428'),
            'Fm2_Ctrl9_LB': (None, fm2_ctrl9_lb),
            'Fm2_Ctrl10_LB_OP': (None, 'Or'),
            # 'Fm2_Ctrl10_LB': (None, '3790,3847,3875'),
            'Fm2_Ctrl10_LB': (None, fm2_ctrl10_lb),
            'Fm2_Ctrl11_LB_OP': (None, 'Or'),
            'Fm2_Ctrl11_LB': (None, ''),
            'Fm2_Ctrl185_DictionaryLookup': (None, ''),
            'Fm2_Ctrl13_TB': (None, ''),
            'Min_Fm2_Ctrl13_TB': (None, ''),
            'Max_Fm2_Ctrl13_TB': (None, ''),
            'Combined_Fm2_Ctrl13_TB': (None, ''),
            'Fm2_Ctrl15_TB': (None, ''),
            'Min_Fm2_Ctrl15_TB': (None, ''),
            'Max_Fm2_Ctrl15_TB': (None, ''),
            'Combined_Fm2_Ctrl15_TB': (None, ''),
            'Fm2_Ctrl22_TB': (None, ''),
            'Min_Fm2_Ctrl22_TB': (None, ''),
            'Max_Fm2_Ctrl22_TB': (None, ''),
            'Combined_Fm2_Ctrl22_TB': (None, ''),
            'Fm2_Ctrl16_TB': (None, ''),
            'Min_Fm2_Ctrl16_TB': (None, ''),
            'Max_Fm2_Ctrl16_TB': (None, ''),
            'Combined_Fm2_Ctrl16_TB': (None, ''),
            'Fm2_Ctrl25_TB': (None, ''),
            'Min_Fm2_Ctrl25_TB': (None, ''),
            'Max_Fm2_Ctrl25_TB': (None, ''),
            'Combined_Fm2_Ctrl25_TB': (None, ''),
            'Fm2_Ctrl18_TB': (None, ''),
            'Min_Fm2_Ctrl18_TB': (None, ''),
            'Max_Fm2_Ctrl18_TB': (None, ''),
            'Combined_Fm2_Ctrl18_TB': (None, ''),
            'Fm2_Ctrl17_TB': (None, ''),
            'Min_Fm2_Ctrl17_TB': (None, ''),
            'Max_Fm2_Ctrl17_TB': (None, ''),
            'Combined_Fm2_Ctrl17_TB': (None, ''),
            'Fm2_Ctrl135_LB': (None, ''),
            'Fm2_Ctrl5_LB': (None, ''),
            'Fm2_Ctrl23_LB': (None, ''),
            'Fm2_Ctrl6_LB_OP': (None, 'Or'),
            'Fm2_Ctrl6_LB': (None, ''),
            'Fm2_Ctrl4119_TB': (None, ''),
            'Fm2_Ctrl4120_LB': (None, ''),
            'Fm2_Ctrl26_TB': (None, ''),
            'Min_Fm2_Ctrl26_TB': (None, ''),
            'Max_Fm2_Ctrl26_TB': (None, ''),
            'Combined_Fm2_Ctrl26_TB': (None, ''),
            'Fm2_Ctrl27_LB_OP': (None, 'Or'),
            'Fm2_Ctrl27_LB': (None, ''),
            'Fm2_Ctrl29_TextBox': (None, ''),
            'Fm2_Ctrl30_LB_OP': (None, 'Or'),
            'Fm2_Ctrl30_LB': (None, ''),
            'Fm2_Ctrl28_TextBox': (None, ''),
            'Fm2_Ctrl42_TB': (None, ''),
            'Fm2_Ctrl45_TB': (None, ''),
            'Fm2_Ctrl44_TB': (None, ''),
            'Fm2_Ctrl43_TB': (None, ''),
            'Fm2_Ctrl47_TB': (None, ''),
            'Min_Fm2_Ctrl47_TB': (None, ''),
            'Max_Fm2_Ctrl47_TB': (None, ''),
            'Combined_Fm2_Ctrl47_TB': (None, ''),
            'Fm2_Ctrl48_LB_OP': (None, 'Or'),
            'Fm2_Ctrl48_LB': (None, ''),
            'Fm2_Ctrl52_LB_OP': (None, 'Or'),
            'Fm2_Ctrl52_LB': (None, ''),
            'Fm2_Ctrl1547_LB_OP': (None, 'Or'),
            'Fm2_Ctrl1547_LB': (None, ''),
            'Fm2_Ctrl54_LB_OP': (None, 'Or'),
            'Fm2_Ctrl54_LB': (None, ''),
            'Fm2_Ctrl35_LB_OP': (None, 'Or'),
            'Fm2_Ctrl35_LB': (None, ''),
            'Fm2_Ctrl36_LB_OP': (None, 'Or'),
            'Fm2_Ctrl36_LB': (None, ''),
            'Fm2_Ctrl37_LB_OP': (None, 'Or'),
            'Fm2_Ctrl37_LB': (None, ''),
            'Fm2_Ctrl39_LB_OP': (None, 'Or'),
            'Fm2_Ctrl39_LB': (None, ''),
            'Fm2_Ctrl57_LB_OP': (None, 'Or'),
            'Fm2_Ctrl57_LB': (None, ''),
            'Fm2_Ctrl62_LB_OP': (None, 'Or'),
            'Fm2_Ctrl62_LB': (None, ''),
            'mapshapes': (None, ''),
            # 'mapbounds': (None, '37.459335,-126.7382814375,37.459335,-113.1152345625'),
            'mapbounds': (None, ''), # for test nil the map value
            'clearCheckedItems': (None, 'true'),
            'isQuickView': (None, 'false'),
            'displayID': (None, 'C631'),
            'fullDisplayID': (None, ''),
        }
        return files

    def ini_driver(self):
        # Configure Selenium-Wire with explicit options
        chrome_options = webdriver.ChromeOptions()

        # Enable incognito mode
        chrome_options.add_argument("--incognito")

        # Disable images to make the browser lightweight
        prefs = {
            "profile.managed_default_content_settings.images": 2,  # Disable images
            "profile.default_content_setting_values.notifications": 2,  # Disable notifications
            "profile.managed_default_content_settings.stylesheets": 2,  # Disable CSS
            "profile.managed_default_content_settings.javascript": 1,  # Enable JavaScript
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # Handle "Not Secure" warnings (ignore SSL certificate errors)
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--allow-running-insecure-content')

        # Additional performance optimizations
        chrome_options.add_argument('--disable-gpu')  # Disable GPU hardware acceleration
        chrome_options.add_argument('--disable-extensions')  # Disable extensions
        chrome_options.add_argument('--no-sandbox')  # Disable sandbox for faster execution
        chrome_options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems
        chrome_options.add_argument('--start-maximized')  # Start maximized to avoid UI issues
        # chrome_options.add_argument('--headless')  # Run in headless mode

        # Zyte Proxy configuration
        ZYTE_API_KEY = "f693db95c418475380b0e70954ed0911"
        proxy_url = f"http://{ZYTE_API_KEY}:@proxy.zyte.com:8011"

        # Configure Selenium-Wire with explicit options
        seleniumwire_options = {
            'proxy': {
                'http': proxy_url,
                'https': proxy_url,
                'no_proxy': 'localhost,127.0.0.1'
            },
            'timeout': 300,
            'disable_encoding': True,  # Optional: for easier response reading
            'exclude_hosts': [],  # Optional: exclude specific hosts
        }

        # Initialize WebDriver with options
        self.driver = webdriver.Chrome(
            options=chrome_options,  # Pass Chrome options
            seleniumwire_options=seleniumwire_options
        )
        self.driver.implicitly_wait(10)  # Global implicit wait

        return self.driver

    def get_records(self, subtype_id,subtype_name, county_id, county_name):
        # Fill Postal Code Input field
        post_code = self.driver.find_element(By.ID, 'Fm2_Ctrl19_TextBox')
        post_code.clear()
        post_code.send_keys("94501")
        post_code.send_keys(Keys.RETURN)
        self.write_logs("Entered postal code and pressed Enter")
        time.sleep(5)
