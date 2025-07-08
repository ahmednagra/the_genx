import re
import time
import json
import requests
from seleniumwire import webdriver  # pip install selenium-wire
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
"""'        Username': 'U41280',
            'Password': 'Realestate12#'     """
# Set up Selenium Wire options with an increased timeout.
seleniumwire_options = {
    'timeout': 300  # Timeout in seconds
}
driver = webdriver.Chrome(seleniumwire_options=seleniumwire_options)

# Navigate to the home page.
driver.get("https://matrix.crmls.org/Matrix/Default.aspx")
input("Press Enter after reaching Home Page... CRMLS")

# Navigate to the detail page and trigger the API call.
driver.get("https://matrix.crmls.org/Matrix/Search/Residential/Detail")

# Wait for and click the 'Switch to 3-Panel Search' button
wait = WebDriverWait(driver, 40)  # Define a wait object with a timeout of 10 seconds
button = wait.until(EC.element_to_be_clickable((By.ID, "m_lbSwitchTo3PanelSearch")))
driver.execute_script("arguments[0].click();", button)

# Wait for and click the status dropdown button
status_button = wait.until(EC.element_to_be_clickable((By.ID, "dropdown_Fm2_Ctrl3_LB")))
driver.execute_script("arguments[0].click();", status_button)

# Wait for and select the 'Closed' checkbox
checkbox = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-mtrx-item-text="Closed"]')))
driver.execute_script("arguments[0].click();", checkbox)

# Wait for and click the 'Apply' button
apply_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='button' and @value='Apply']")))
driver.execute_script("arguments[0].click();", apply_button)

time.sleep(10)
# Wait for API response after clicking Apply
wait.until(lambda d: any("matrix.crmls.org/Matrix/s/Update" in req.url for req in d.requests))

# Iterate through captured requests.
for request in driver.requests:
    token = ''
    if request.response and "matrix.crmls.org/Matrix/s/Update" in request.url:
        print("API Request URL:", request.url)
        update_url = request.url
        print("Request Method:", request.method)
        headers = {}
        # Print Request Headers
        print("\n--- Request Headers ---")
        for key, value in request.headers.items():
            print(f"{key}: {value}")
            if key != 'cookie':
                headers[key] = value
        print('final headers:', headers)
        # Extract and print Request Cookies (if present in the headers).
        request_cookies = request.headers.get('Cookie', None)
        print("\n--- Request Cookies ---")
        # Convert the cookie string into a dictionary
        cookie_dict = {}
        if request_cookies:
            # Split the string into individual cookies using ';' as a delimiter.
            for cookie in request_cookies.split(';'):
                cookie = cookie.strip()  # Remove leading/trailing whitespace
                if cookie:
                    # Split on the first '=' to separate key and value.
                    if '=' in cookie:
                        key, value = cookie.split('=', 1)
                        if key != 'AWSALB' and key != 'AWSALBCORS':

                            cookie_dict[key] = value
                    else:
                        # If no '=', store the key with an empty value.
                        cookie_dict[cookie] = ""
        else:
            print("No request cookies found.")

        print("\n--- Request Cookies Dictionary ---")
        print(cookie_dict)

        # If the request method is POST, print out the payload.
        if request.method == "POST":
            if request.body:
                try:
                    payload = request.body.decode('utf-8')
                except Exception:
                    payload = request.body
                print("\n--- Payload ---")
                print(payload)
                with open('payload.txt', 'w') as f:
                    f.write(payload)
                pattern = r'name="__RequestVerificationToken"\s*\r?\n\r?\n([^\r\n]+)'

                match = re.search(pattern, payload)

                if match:
                    token = match.group(1)
                    print("Token:", token)
                else:
                    print("Token not found")
            else:
                print("\nNo payload found in the request body.")
        response_headers = []
        # Print Response Headers.
        print("\n--- Response Headers ---")
        for key, value in request.response.headers.items():
            print(f"{key}: {value}")
            response_headers.append(f"{key}: {value}")
        resp_headers = '\n'.join(response_headers)
        # Define regex patterns that look for the set-cookie lines.
        pattern_awsalb = r"set-cookie:\s*AWSALB=([^;]+)"
        pattern_awsalb_cors = r"set-cookie:\s*AWSALBCORS=([^;]+)"

        awsalb_match = re.search(pattern_awsalb, resp_headers)
        awsalb_cors_match = re.search(pattern_awsalb_cors, resp_headers)

        if awsalb_match:
            awsalb_value = awsalb_match.group(1)
            print("AWSALB:", awsalb_value)
            cookie_dict['AWSALB'] = awsalb_value
        else:
            print("AWSALB not found.")
        awsalb_value = None
        awsalb_cors_value = None
        if awsalb_cors_match:
            awsalb_cors_value = awsalb_cors_match.group(1)
            print("AWSALBCORS:", awsalb_cors_value)
            cookie_dict['AWSALBCORS'] = awsalb_cors_value
        else:
            print("AWSALBCORS not found.")

        print("updated cookie dict: ", json.dumps(cookie_dict, indent=4))
        files = {
            '__RequestVerificationToken': (
            None, token),
            'responsiveSearch.SearchFormID': (None, '2'),
            'IsValid': (None, 'true'),
            'Fm2_Ctrl3_LB': (None, '6145'),
            'FmFm2_Ctrl3_25656_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6140_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6146_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6144_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6145_Ctrl3_TB': (None, '0-180'),
            'FmFm2_Ctrl3_6148_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6142_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6141_Ctrl3_TB': (None, ''),
            'FmFm2_Ctrl3_6147_Ctrl3_TB': (None, ''),
            'Fm2_Ctrl4_LB_OP': (None, 'Or'),
            'Fm2_Ctrl4_LB': (None, ''),
            'Fm2_Ctrl19_TextBox': (None, ''),
            'Fm2_Ctrl9_LB_OP': (None, 'Or'),
            'Fm2_Ctrl9_LB': (None, '3434'),
            'Fm2_Ctrl10_LB_OP': (None, 'Or'),
            'Fm2_Ctrl10_LB': (None, ''),
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
            'clearCheckedItems': (None, 'true'),
            'isQuickView': (None, 'false'),
            'displayID': (None, 'C631'),
            'fullDisplayID': (None, ''),
        }
        response_data = requests.post(update_url, cookies=cookie_dict, headers=headers, files=files)
        print(response_data.text)
        # Break after finding the first matching API call.
        break

input("Press Enter to exit...: ")
driver.quit()
