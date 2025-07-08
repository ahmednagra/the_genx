import csv
import glob
import json
import mimetypes
import os
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

import pytz
import requests
from scrapy import Spider, Request, Selector
from scrapy.http import JsonRequest



class KickStarterSpider(Spider):
    name = "KickStarter"
    allowed_domains = ['kickstarter.com', 'www.kickstarter.com', 'api.kickstarter.com']
    start_urls = ["https://www.kickstarter.com/"]
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        'OFFSITE_ENABLED': False,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'CONCURRENT_REQUESTS': 2,
        'FEED_EXPORTERS': {
            'xlsx': 'scrapy_xlsx.XlsxItemExporter',
        },
        'FEEDS': {
            f'kickstarterscraper/output/{name} Projects Information_{current_dt}.xlsx': {
                'format': 'xlsx',
                'fields': ['Index No', 'Project URL', 'Project Title','Project Description', 'Pledged Amount', 'Goal Amount', 'Number of Backers', 'Funding Status',
                           'Location', 'Project Date', 'Last Updated Date', 'Story Text', 'Risks and Challenges Text', 'Creator Name', 'Projects Created',
                           'Projects Backed', 'Creator Last Login', 'Creator Account Created', 'Creator Bio',
                           'Creator Image', 'URL']
            }
        }
    }

    graph_headers = {
        'accept': '*/*',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'content-type': 'application/json',
        'origin': 'https://www.kickstarter.com',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': '',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'x-csrf-token': '',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scrape_urls_count = 0

        os.makedirs('kickstarterscraper/logs', exist_ok=True)
        os.makedirs('kickstarterscraper/input', exist_ok=True)
        os.makedirs('kickstarterscraper/output', exist_ok=True)

        self.logs_filepath = f'kickstarterscraper/logs/{self.name}_logs {self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')
        self.urls = self.process_csv_file()
        self.total_urls = len(self.urls)
        self.seen_urls = self.read_csv_file()
        self.write_logs(f'[INFO] Total project URLs found in CSV: {self.total_urls}')
        self.proxy = 'http://scraperapi.keep_headers=true:de8bc3a18a39c84d2cdcdc402afbda20@proxy-server.scraperapi.com:8001' # client apikey

        self.seen_url_path = 'kickstarterscraper/output/seen_urls.csv'

    def start_requests(self):
        for row in self.urls:
            index = int(row[''])
            url = row['project_url']

            if url in self.seen_urls:
                print('Project Url is already scraped')
                continue

            yield Request(url, meta={'url_index': index, 'proxy':self.proxy})

    def parse(self, response, **kwargs):
        try:
            text = response.css('script:contains("window.current_project")::text').re_first(r' window.current_project = "(.*)";')
            text = text.replace('\\\'', "'").replace('\\"', '"').replace('undefined', 'null')  # JS 'undefined' -> JSON 'null'
            dict_data = json.loads(text)
        except json.JSONDecodeError as e:
            self.write_logs(f"parse() - JSON decode or extraction error: {e} & URL:{response.url}")
            return

        try:
            last_update_dict = json.loads(response.css('script:contains("window.ksr_track_properties")').re_first(r'window.ksr_track_properties = (.*);'))
        except json.JSONDecodeError as e:
            last_update_dict = {}

        time_stamp = last_update_dict.get('project', {}).get('project_deadline', '')
        dt = datetime.fromisoformat(time_stamp) # Parse the ISO string with timezone info
        tz_target = pytz.timezone("Asia/Karachi") # Convert to desired timezone (e.g., Asia/Karachi)
        dt_target = dt.astimezone(tz_target)
        formatted_deadline = dt_target.strftime('%a, %B %d %Y %I:%M %p UTC %z') # Format to match: Sun, May 20 2012 7:39 AM UTC +05:00
        project_date = formatted_deadline[:-2] + ':' + formatted_deadline[-2:]  # Optional: Insert colon in UTC offset (+0500 → +05:00)
        last_update_date = dt_target.strftime('%B %#d, %Y')

        funding_status_raw = dict_data.get('funding_status', '').lower() or dict_data.get('state', '').lower()

        if funding_status_raw == 'failed':
            funding_status = 'Funding Unsuccessful'
        elif funding_status_raw == 'successful':
            funding_status = 'Funding Successful'
        else:
            funding_status = funding_status_raw

        pledge_amount = f"{int(dict_data.get('pledged', 0.0)):,}"
        goal_amount = f"{int(dict_data.get('goal', 0.0)):,}"

        project_data = {
            'Index No' :response.meta.get('url_index', 0),
            'Project URL': response.url,
            'Project Title': dict_data.get('name', ''),
            'Project Description': dict_data.get('blurb', ''),
            'Pledged Amount': pledge_amount,
            'Goal Amount': goal_amount,
            'Number of Backers': dict_data.get('backers_count', 0),
            'Funding Status': funding_status,
            'Location': dict_data.get('location', {}).get('displayable_name', ''),
            'Project Date': project_date,
            'Last Updated Date': response.css('[data-format="LL"] ::text').get('') or last_update_date,
            'URL': response.url,
        }

        creator_url = dict_data.get('creator', {}).get('urls', {}).get('api', {}).get('user', '')
        csrf_token = response.css('meta[name="csrf-token"]::attr(content)').get('')
        self.graph_headers['referer'] = response.url
        self.graph_headers['x-csrf-token'] = csrf_token

        p_id = dict_data.get('id', 0)
        s_id = dict_data.get('creator', {}).get('id', 0)
        s_value = dict_data.get('slug','')
        slug = f'{s_id}/{s_value}'
        graph_ql = self.graphql_formdata(slug, p_id)
        url = 'https://www.kickstarter.com/graph'
        response.meta['project_data'] = project_data
        response.meta['creator_url'] = creator_url
        response.meta['p_url'] = response.url
        response.meta['proxy'] =  self.proxy
        response.meta.pop('_auth_proxy', None)
        yield JsonRequest(url, data=graph_ql, headers=self.graph_headers, meta=response.meta, callback=self.parse_campaign)

    def parse_campaign(self, response):
        project_data = response.meta.get('project_data', {})
        p_url = response.meta.get('p_url', '')
        try:
            dict_data = response.json()
            campaign_dict = next((d['data']['project'] for d in dict_data if d.get('data', {}).get('project', '')), {})
        except json.JSONDecodeError as e:
            self.write_logs(f"parse_campaign() - GraphQL response parsing error: {e} & Project URL:{p_url}")
            return

        risks_text = campaign_dict.get('risks', '').strip()
        story_html = campaign_dict.get('story', '').strip()

        # Convert HTML to plain text
        story_selector = Selector(text=story_html)
        story_text = '\n'.join(t.replace('\r', '').strip() for t in story_selector.css('::text').getall() if t.strip())

        # Add to project data
        project_data.update({
            'Story Text': story_text,
            'Risks and Challenges Text': risks_text,
        })

        creator_url = response.meta.get('creator_url')
        response.meta['project_data'] = project_data
        response.meta['proxy'] = self.proxy
        response.meta.pop('_auth_proxy', None)
        if creator_url:
            yield Request(creator_url, callback=self.parse_creator, meta=response.meta)
        else:
            self.write_logs(f'[WARNING] No creator URL found for project: {project_data.get("Project Title")}')
            self.scrape_urls_count += 1
            self.mark_url_seen(p_url)
            yield project_data

    def parse_creator(self, response):
        project_data = response.meta.get('project_data', {})
        p_url = response.meta.get('p_url', '')

        try:
            dict_data = response.json()
        except json.JSONDecodeError as e:
            self.write_logs(f"parse_creator() - Creator JSON parsing error: {e} & Project URL:{p_url}")
            self.mark_url_seen(p_url)
            self.scrape_urls_count += 1
            yield project_data
            return

        try:
            created_at_ts = dict_data.get('created_at')
            updated_at_ts = dict_data.get('updated_at')
            last_login = datetime.fromtimestamp(updated_at_ts, timezone.utc).strftime('%b %d %Y').replace(' 0',
                                                                                                          ' ') if updated_at_ts else ''
            account_created = datetime.fromtimestamp(created_at_ts, timezone.utc).strftime(
                '%b %Y') if created_at_ts else ''
            c_image = dict_data.get('avatar', {}).get('large', '')
            creator_name = dict_data.get('name', '').strip()

            creator_data = {
                'Creator Name': creator_name,
                'Projects Created': dict_data.get('created_projects_count', 0),
                'Projects Backed': dict_data.get('backed_projects_count', 0),
                'Creator Last Login': last_login,
                'Creator Account Created': account_created,
                'Creator Bio': dict_data.get('biography', ''),
                'Creator Image': c_image
            }

            project_data.update(creator_data)

            if c_image and creator_name:
                self.download_creator_image(c_image, creator_name)

            self.mark_url_seen(p_url)
            self.scrape_urls_count += 1
            print('Items Are Scraped:', self.scrape_urls_count)
            yield project_data

        except Exception as e:
            self.write_logs(f"parse_creator() - Creator data processing error: {e} & Project URL: {p_url}")

    def write_logs(self, log_msg):
        time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f"[{time_stamp}] {log_msg}\n")
            print(log_msg)

    def process_csv_file(self):
        # input_file = glob.glob('input/kickstarter_JAN.csv')[0]
        input_file = 'kickstarterscraper/input/kickstarter_JAN.csv'

        data = []
        try:
            with open(input_file, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                data = list(csv_reader)

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

    def graphql_formdata(self,slug, p_id):
        json_data = [
            {
                'operationName': 'Campaign',
                'variables': {
                    # 'slug': '1032337254/all-american-food-truck',
                    'slug': str(slug),
                },
                'query': 'query Campaign($slug: String!) {\n  project(slug: $slug) {\n    id\n    isSharingProjectBudget\n    risks\n    story(assetWidth: 680)\n    storyRteVersion\n    currency\n    spreadsheet {\n      displayMode\n      public\n      url\n      data {\n        name\n        value\n        phase\n        rowNum\n        __typename\n      }\n      dataLastUpdatedAt\n      __typename\n    }\n    environmentalCommitments {\n      id\n      commitmentCategory\n      description\n      __typename\n    }\n    aiDisclosure {\n      fundingForAiAttribution\n      fundingForAiConsent\n      fundingForAiOption\n      generatedByAiConsent\n      generatedByAiDetails\n      otherAiDetails\n      involvesAi\n      involvesFunding\n      involvesGeneration\n      involvesOther\n      __typename\n    }\n    __typename\n  }\n}',
            },
            {
                'operationName': 'FetchProjectSignalAndWatchStatus',
                'variables': {
                    # 'pid': 709170432,
                    'pid': int(p_id),
                },
                'query': 'query FetchProjectSignalAndWatchStatus($pid: Int) {\n  project(pid: $pid) {\n    ...project\n    __typename\n  }\n  me {\n    ...user\n    __typename\n  }\n}\n\nfragment project on Project {\n  id\n  pid\n  isDisliked\n  isLiked\n  isWatched\n  isWatchable\n  isLaunched\n  __typename\n}\n\nfragment user on User {\n  id\n  uid\n  canSeeConfirmWatchModal\n  canSeeConfirmSignalModal\n  isEmailVerified\n  __typename\n}',
            },
        ]

        return json_data

    def mark_url_seen(self, url):
        # Ensure the path exists
        file_exists = os.path.isfile(self.seen_url_path)

        # Read existing URLs
        existing_urls = set()
        if file_exists:
            with open(self.seen_url_path, mode='r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                existing_urls = {row[0] for row in reader if row}

        # Skip if URL already exists
        if url in existing_urls:
            return

        # Write the new URL
        with open(self.seen_url_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['urls'])  # Write header only once
            writer.writerow([url])

    def read_csv_file(self):
        try:
            with open(self.seen_url_path, mode='r', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                return [row['urls'] for row in reader if row.get('urls')]
        except Exception as e:
            self.write_logs(f'[ERROR] Failed to read seen URLs file: {e}')
            return []

    def download_creator_image(self, c_image, creator_name):
        image_folder = os.path.join("kickstarterscraper", "output", "Images")

        try:
            os.makedirs(image_folder, exist_ok=True)
            headers = {
                'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
                'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
                'referer': 'https://www.kickstarter.com/',
                 "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                }
            response_img = requests.get(c_image, headers=headers, timeout=100)
            if response_img.status_code == 200:

                # Get clean filename from URL
                parsed_url = urlparse(c_image)
                filename = os.path.basename(parsed_url.path)
                filename = unquote(filename)  # decode URL-encoded characters

                # Get file extension from Content-Type
                content_type = response_img.headers.get('Content-Type', '')
                ext = mimetypes.guess_extension(content_type.split(';')[0]) or '.jpg'

                # Ensure filename has the correct extension
                if not filename.lower().endswith(ext):
                    filename += ext

                # Final full image path
                image_path = os.path.join(image_folder, filename)

                with open(image_path, 'wb') as f:
                    f.write(response_img.content)
            else:
                self.write_logs(f"Image download failed: {c_image} | Status code: {response_img.status_code}")
        except Exception as e:
            self.write_logs(f"Error downloading image: {e} | URL: {c_image}")

        return