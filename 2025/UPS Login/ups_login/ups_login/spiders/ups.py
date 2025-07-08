import os
import glob
from time import sleep
from datetime import datetime

from scrapy import Spider, Request

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC


class UpsSpider(Spider):
    name = "ups"
    allowed_domains = ["www.ups.com"]
    start_urls = ["https://www.ups.com/lasso/login"]
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    def __init__(self):
        super().__init__()

        # Initialize Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

        self.creds = self.read_creds_file()
        self.driver = self.init_driver()
        a=1

    def start_requests(self):
        """Starts the login process before sending requests"""
        self.login()  # Perform login with Selenium

        # Continue with Scrapy requests after login
        for url in self.start_urls:
            yield Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        """Handles the post-login page
        :param **kwargs:
        """
        self.write_logs("[SCRAPY] Scrapy started parsing the page")

    def read_creds_file(self):
        file_path = glob.glob('input/creds.txt')[0]

        try:
            with open(file_path, mode='r') as txt_file:
                creds = {}
                for line in txt_file:
                    line = line.strip()
                    if "==" in line:  # Ensure correct format
                        key, value = line.split("==", 1)  # Split only at the first occurrence
                        creds[key.strip()] = value.strip()
                return creds

        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return {}
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return {}

    def init_driver(self):
        """Launch system-installed Chrome in Guest mode and attach Selenium WebDriver to it."""
        chrome_exe_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

        # Automatically download the correct ChromeDriver version
        chrome_driver_path = ChromeDriverManager().install()

        chrome_options = Options()
        chrome_options.binary_location = chrome_exe_path  # Set custom Chrome executable
        chrome_options.add_argument(
            "--user-data-dir=C:\\Users\\Muhammad Ahmed\\AppData\\Local\\Google\\Chrome\\User Data")

        chrome_options.add_argument("--profile-directory=Default")  # Use Default profile
        chrome_options.add_argument("--remote-debugging-port=9222")  # Enable debugging
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Avoid detection
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        # Set up the WebDriver service
        service = Service(chrome_driver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)

        return driver

    def login(self):
        """Automates the UPS login process using Selenium"""
        self.driver.get("https://www.ups.com/lasso/login")

        try:
            # Wait until page loads
            WebDriverWait(self.driver, 30).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            self.write_logs("[LOGIN] Login page loaded successfully")

            # Enter username/email
            email_input = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, "email"))
            )
            email_input.clear()
            email_input.send_keys(self.creds.get("username", ""))

            # Click Continue button
            continue_btn = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.ID, "submitBtn"))
            )
            continue_btn.click()
            self.write_logs("[LOGIN] Clicked 'Continue' button")
            sleep(5)

            # Wait for the next page to load
            WebDriverWait(self.driver, 30).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

            # Enter Password
            pass_input = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, "pwd"))
            )
            pass_input.clear()
            pass_input.send_keys(self.creds.get('password', ""))

            # Click Login button
            login_btn = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.ID, "submitBtn"))
            )
            login_btn.click()
            self.write_logs("[LOGIN] Clicked 'Login' button")
            sleep(5)

            # Wait for the next page to load
            WebDriverWait(self.driver, 30).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

            current_url = self.driver.current_url
            if 'dashboard' in current_url:
                self.write_logs("[LOGIN] Login successfully")
            else:
                self.write_logs("[LOGIN] After Clicking Login face an Error")

        except Exception as e:
            self.write_logs(f"[LOGIN ERROR] {str(e)}")
            self.driver.quit()

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)
