import csv
import glob

import requests

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
}

headers_1 = {
        'Accept': '*/*;q=0.5, text/javascript, application/javascript, application/ecmascript, application/x-ecmascript',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://registre.oaciq.com',
        'Pragma': 'no-cache',
        'Referer': 'https://registre.oaciq.com/en/find-broker/C6E365F4',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'X-CSRF-Token': 'tYID3/71yplYTHrBQR7QykdjbIRb26Q0Zz4Hrn+6JnFA8w3NzDh0KaqX8pH56E4I3MLywRjxe/El2oRSHWl9ig==',
        'X-Requested-With': 'XMLHttpRequest',
}
cookie_jar = 1

def read_csv_to_dicts(file_path):
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)  # Preserves order from CSV

def make_post_requests(data_list):
    for i, data in enumerate(data_list, 1):
        url = data.get('URL', '')
        try:
            response = requests.post(url, headers=headers)
            print(f"[{i}] Status: {response.status_code}, Response: {response.text}")
        except requests.RequestException as e:
            print(f"[{i}] Request failed: {e}")

if __name__ == '__main__':
    csv_file_path = glob.glob('URLS*.csv')[0]
    data_rows = read_csv_to_dicts(csv_file_path)
    print(f"Total rows to process: {len(data_rows)}")

    make_post_requests(data_rows)
