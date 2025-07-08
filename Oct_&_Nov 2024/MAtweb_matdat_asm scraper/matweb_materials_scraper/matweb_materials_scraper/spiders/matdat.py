from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict

from scrapy import Request, FormRequest

from .base import BaseSpider


class MatdatSpider(BaseSpider):
    name = "Mat_Dat"
    start_urls = ['https://www.matdat.com/DAT-Login.aspx']

    login_headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
                    'Cache-Control': 'max-age=0',
                    'Connection': 'keep-alive',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Origin': 'https://www.matdat.com',
                    'Referer': 'https://www.matdat.com/DAT-Login.aspx',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                }

    fields = [# MADDAT Headers
                'Material Group',
                'Material ID (MAT_ID)', 'Material Designation', 'Manufacturer/Supplier', 'Chemical Composition',
                'Semifinished Material Information', 'Heat Treatment', 'Microstructure', 'Hardness', 'Testing Conditions (Axial Loading)',
                "Young's Modulus (N/mm⁻²)", "Poisson's Ratio", 'Yield Strength (N/mm⁻²)', 'Ultimate Tensile Strength (N/mm⁻²)',
                'Elongation (A5, %)', 'Reduction of Area (RA, %)', 'True Fracture Stress (N/mm⁻²)', 'True Fracture Strain',
                'Monotonic Stress-Strain Curves (Ramberg-Osgood Model)', 'Cyclic/Fatigue Properties (Axial Loading, Fully Reversed)',
                'Testing Temperature (°C)', 'Testing Medium', 'Loading Type', 'Loading Control', 'Specimen', 'Loading Ratio',
                'Additional Remarks', 'Fatigue Properties', 'Fatigue Strain-Life Parameters (Coffin-Manson-Basquin Model)',
                'Collecting Cyclic Stress-Strain Plot Data', 'Collecting Strain-Life Fatigue Plot Data'
            ]

    def start_requests(self):
        yield Request(url=self.start_urls[0], headers=self.login_headers,
                      dont_filter=True, callback=self.parse,  meta={'handle_httpstatus_all': True})

    def parse(self, response, **kwargs):
        try:
            event_validation = response.css('#__EVENTVALIDATION ::attr(value)').get('')
            view_stat = response.css('#__VIEWSTATE ::attr(value)').get('')
            data = self.get_form_data(event_validation, view_stat, group=False, key='login')
            yield FormRequest(url=self.start_urls[0], formdata=data, callback=self.parse_login,
                                  dont_filter=True, headers=self.login_headers)
        except Exception as e:
            self.write_logs(f'Error In parse Function URL:{response.url}  \n Error:{e} \n\n')

    def parse_login(self, response):
        if 'Javad Shirani' in response.text: # verifying response after login or not
            self.write_logs(f'login Successfully : {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
        else:
            self.write_logs(f'Login Failed : {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
            # Close the spider with a specific reason
            self.crawler.engine.close_spider(self, reason='Login failed')
            return

        groups = response.css('table:contains("Material group:") option ::text').getall()[2:] or response.css('#sadrzaj_ddMaterialGroup option::attr(value)').getall()[2:]
        texts = response.css('[id^="GridView1_lblDesAll_"] b a::text').getall()[2:] or response.css('table:contains("Material group:") option::attr(value)').getall()[2:]

        # Combine the URLs and texts into a list of tuples
        results = list(zip(groups, texts))
        self.write_logs(f'Groups are Found: {results}')

        for group_name , group_value in results:
            self.write_logs(f'Group Started:{group_name}')
            event_validation = response.css('#__EVENTVALIDATION ::attr(value)').get('')
            view_stat = response.css('#__VIEWSTATE ::attr(value)').get('')
            data = self.get_form_data(event_validation, view_stat, group_value, key='group')
            self.login_headers['Referer'] = 'https://www.matdat.com/Dashboard/Search.aspx'
            url= 'https://www.matdat.com/Dashboard/popUpSearchResult.aspx'
            yield FormRequest(url, formdata=data, callback=self.parse_category_pagination,
                              dont_filter=True, headers=self.login_headers, meta={'group_name':group_name})

    def parse_category_pagination(self, response):
        try:
            urls = response.css('[id^="GridView1_lblDesAll_"] b a::attr(href)').getall()
            texts = response.css('[id^="GridView1_lblDesAll_"] b a::text').getall()

            self.all_categories_found_records += len(urls)
            # Combine the URLs and texts into a list of tuples
            results = list(zip(urls, texts))

            materials_urls = response.css('[id^="GridView1_lblDesAll_"] b ::attr(href)').getall()
            self.write_logs(f"Group:{response.meta.get('group_name', '')} Total Materials Found {len(materials_urls)}")

            for url, title in results:
                print(f'Material Name: {title} & URL: {url}')
                self.login_headers['Referer'] = 'https://www.matdat.com/Dashboard/popUpSearchResult.aspx'
                url = urljoin('https://www.matdat.com/Dashboard/', url)
                response.meta['material_name'] = title
                yield Request(url,
                    callback=self.parse_detail,
                    headers=self.login_headers,
                    dont_filter=True,
                    meta=response.meta
                )

        except Exception as e:
            self.write_logs(f"Error in parse_category_pagination: {str(e)}")

    def parse_detail(self, response):
        try:
            item = OrderedDict()

            item['Material Group'] = response.meta.get('group_name', '') # for verification
            item['Material ID (MAT_ID)'] = int(response.css('#pnl_00 td:contains("Material ID (MAT_ID)") + td ::text').get(''))
            item['Material Designation'] = self.get_material_desg(response)
            item['Manufacturer/Supplier'] = self.get_manufacturer_info(response)
            item['Chemical Composition'] = self.get_chemical_composition(response )
            item['Semifinished Material Information'] = self.get_semifinished_mat_info(response)
            item['Heat Treatment'] = self.get_heat_treatment(response)
            item['Microstructure'] = self.get_micro_str(response)
            item['Hardness'] = self.get_hardness(response)
            item['Testing Conditions (Axial Loading)'] = self.get_test_condition(response)
            item["Young's Modulus (N/mm⁻²)"] = self.get_mono_properties(response).get('young_module', '')
            item["Poisson's Ratio"] = self.get_mono_properties(response).get('poisson_ratio', '')
            item["Yield Strength (N/mm⁻²)"] = self.get_mono_properties(response).get('yield_str', '')
            item["Ultimate Tensile Strength (N/mm⁻²)"] = self.get_mono_properties(response).get('ult_tensile_str', '')
            item["Elongation (A5, %)"] = self.get_mono_properties(response).get('elongation', '')
            item["Reduction of Area (RA, %)"] = self.get_mono_properties(response).get('reduction_of_era', '')
            item["True Fracture Stress (N/mm⁻²)"] = self.get_mono_properties(response).get('fracture_stress', '')
            item["True Fracture Strain"] = self.get_mono_properties(response).get('fracture_strain', '')

            item['Monotonic Stress-Strain Curves (Ramberg-Osgood Model)'] = self.get_mono_osgood_model(response)
            item['Cyclic/Fatigue Properties (Axial Loading, Fully Reversed)'] = ''
            item['Testing Temperature (°C)'] = self.get_test_cond_speciman_info(response, key='Testing temperature')
            item['Testing Medium'] = self.get_test_cond_speciman_info(response, key='Testing medium')
            item['Loading Type'] = self.get_test_cond_speciman_info(response, key='Loading type')
            item['Loading Control'] = self.get_test_cond_speciman_info(response, key='Loading control')
            item['Specimen'] = self.get_test_cond_speciman_info(response, key='Specimen')
            item['Loading Ratio'] = self.get_test_cond_speciman_info(response, key='Loading ratio')
            item['Additional Remarks'] = self.get_cyclic_add_remarks(response)

            item['Fatigue Properties'] = self.get_fatigue_properties(response)
            item['Fatigue Strain-Life Parameters (Coffin-Manson-Basquin Model)'] = self.get_fatigue_parameters(response)

            # after meeting place image url
            cycle_strain_stress_chart_url = response.css('#cycChart::attr(src)').get('')
            if cycle_strain_stress_chart_url:
                cycle_strain_stress_chart_url = urljoin('https://www.matdat.com', cycle_strain_stress_chart_url)
                # print('cycle_strain_stress_chart_url', cycle_strain_stress_chart_url)

            collecting_strain_fatigue_plot_url = response.css('#fatChart::attr(src)').get('')
            if collecting_strain_fatigue_plot_url:
                collecting_strain_fatigue_plot_url = urljoin('https://www.matdat.com', collecting_strain_fatigue_plot_url)
                # print('collecting_strain_fatigue_plot_url', collecting_strain_fatigue_plot_url)

            item['Collecting Cyclic Stress-Strain Plot Data'] = cycle_strain_stress_chart_url
            item['Collecting Strain-Life Fatigue Plot Data'] = collecting_strain_fatigue_plot_url
            item['spider'] = response.url
            self.write_xlsx(item)

        except Exception as e:
            self.write_logs(f"Error in Item Yield URL:{response.url} & Error:{str(e)}")

    def get_form_data(self, event_validation, view_stat, group, key):
        data= {}
        if key=='login':
            data = {
                '__EVENTTARGET': 'ctl00$MainContent$ctl00',
                '__EVENTARGUMENT': '',
                '__VIEWSTATE': view_stat,
                '__VIEWSTATEGENERATOR': '5972A543',
                '__SCROLLPOSITIONX': '0',
                '__SCROLLPOSITIONY': '488.79998779296875',
                '__EVENTVALIDATION': event_validation,
                'ctl00$MainContent$email': 'javad@nergyai.com',
                'ctl00$MainContent$passw': 'NERGYAI2024',
                'ctl00$txtNewsletterEmail': '',
            }
        elif key =='group':
            data = {
                '__EVENTTARGET': '',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE':  view_stat,
                '__VIEWSTATEGENERATOR': '8A22162B',
                '__SCROLLPOSITIONX': '0',
                '__SCROLLPOSITIONY': '1440',
                '__EVENTVALIDATION': event_validation,
                'ctl00$sadrzaj$ddStandardMaterial': '0',
                'ctl00$sadrzaj$ddStandardMaterial_2': '0',
                'ctl00$sadrzaj$ddStandardMaterial_3': '0',
                'ctl00$sadrzaj$ddStandardMaterial_4': '0',
                'ctl00$sadrzaj$ddStandardMaterial_5': '0',
                'ctl00$sadrzaj$ddMaterialGroup': group,
                'ctl00$sadrzaj$txtDesignation': '',
                'ctl00$sadrzaj$ddMechProperties': '0',
                'ctl00$sadrzaj$prop1min': 'min',
                'ctl00$sadrzaj$prop1max': 'max',
                'ctl00$sadrzaj$ddMechProperties2': '0',
                'ctl00$sadrzaj$prop2min': 'min',
                'ctl00$sadrzaj$prop2max': 'max',
                'ctl00$sadrzaj$ddMechProperties3': '0',
                'ctl00$sadrzaj$prop3min': 'min',
                'ctl00$sadrzaj$prop3max': 'max',
                'ctl00$sadrzaj$ddMatCondHeatTr': '0',
                'ctl00$sadrzaj$ddMatCondHeatTr_2': '0',
                'ctl00$sadrzaj$ddMatCondHeatTr_3': '0',
                'ctl00$sadrzaj$tTempMin': 'min',
                'ctl00$sadrzaj$tTempMax': 'max',
                'ctl00$sadrzaj$ddTestingMedium': '0',
            }

        return data

    def get_material_desg(self, response):
        # Define the list of keywords to search for
        keywords = [
            "DIN", "W.Nr.", "EN", "AISI", "AFNOR", "ASTM", "BS", "GOST", "HRN",
            "ISO", "JIS", "JUS", "SAE", "SS", "UNS", "Other"
        ]

        # Use a dictionary comprehension to extract values dynamically
        values = {
            # Adjust key label based on specific conditions
            "W.Nr. (DIN 17007)" if key == "W.Nr." else
            "Other & Commercial" if key == "Other" else
            key: response.css(
                f'#pnl_0 td:contains("{key}") + td.w240 ::text'
            ).get(default='')
            for key in keywords
        }

        # Format as a string with newlines
        result = "\n".join([f"{key}: {value}" for key, value in values.items()])
        return result

    def get_manufacturer_info(self, response):
        contribute_by = response.css('#pnl_00 td:contains("Contributed by") + td[colspan="3"] ::text').get('').replace('\xa0', ' ')
        entry_date = response.css('#pnl_00 td:contains("Entry date") + td[colspan="3"] ::text').get('').replace('\xa0', ' ')
        source_reference = response.css('#pnl_00 td:contains("Source reference") + td[colspan="3"] ::text').get('').replace('\xa0', ' ')
        other_references = response.css('#pnl_00 td:contains("Other reference(s)") + td[colspan="3"] ::text').get('').replace('\xa0', ' ')
        material_group = response.css('#pnl_0 td:contains("Material group") + td[colspan="4"] ::text').get('').replace('\xa0', ' ')
        steel_subgroup = response.css('#pnl_0 td:contains("Steel subgroup") + td[colspan="4"] ::text').get('').replace('\xa0', ' ')
        typical_application = response.css('#pnl_0 td:contains("Typical application") + td[colspan="4"] ::text').get('').replace('\xa0', ' ')


        values = {
                'Contributed by': contribute_by,
                'Entry Date': entry_date,
                'Source Reference': source_reference,
                'Other References': other_references,
                'Material Group': material_group,
                'Steel Subgroup': steel_subgroup,
                'Typical Application': typical_application}

        # Format as a string with newlines
        result = "\n".join([f"{key}: {value}" for key, value in values.items()])
        return result

    def get_chemical_composition(self, response):
        chemical_composition = {}
        # Extract all headers (elements)
        headers_1 = response.css('#pnl_1 table tr:nth-child(1) b::text').getall()[1:] or []
        values_1 = []
        values_tag = response.css('#pnl_1 table tr:nth-child(2) td')
        for value_tag in values_tag:
            values_1.append(value_tag.css('::text').get('').replace(',', '.'))
        dict_1 = dict(zip(headers_1, values_1))
        chemical_composition.update(dict_1)

        headers_2 = response.css('#pnl_1 table tr:nth-child(3) b::text').getall()
        values_2 = []
        values_tag = response.css('#pnl_1 table tr:nth-child(4) td')
        for value_tag in values_tag:
            values_2.append(value_tag.css('::text').get('').replace(',', '.'))
        dict_2 = dict(zip(headers_2, values_2))
        chemical_composition.update(dict_2)

        headers_3 = response.css('#pnl_1 table tr:nth-child(5) b::text').getall()
        values_3 = []
        values_tag = response.css('#pnl_1 table tr:nth-child(6) td')
        for value_tag in values_tag:
            values_3.append(value_tag.css('::text').get('').replace(',', '.'))

        dict_3 = dict(zip(headers_3, values_3))
        chemical_composition.update(dict_3)

        headers_4 = response.css('#pnl_1 table tr:nth-child(7) b::text').getall()
        values_4 = []
        values_tag = response.css('#pnl_1 table tr:nth-child(8) td')
        for value_tag in values_tag:
            values_4.append(value_tag.css('::text').get('').replace(',', '.'))

        dict_4 = dict(zip(headers_4, values_4))
        chemical_composition.update(dict_4)

        chem_dict = {"chemical_composition": chemical_composition}

        return str(chem_dict)

    def get_semifinished_mat_info(self, response):
        info_value = response.css('#pnl_1 td:contains("Semifinished material (source)") + td ::text').get('')

        # Define default values for Source and Dimensions
        result = {
            "Source": "",
            "Dimensions (mm)": ""
        }

        if info_value:
            parts = info_value.split('/')
            result["Source"] = parts[0].strip() if len(parts) > 0 else ""
            result["Dimensions (mm)"] = parts[1].strip() if len(parts) > 1 else ""

        # Format the result as a string
        return f"Source: {result['Source']}\nDimensions (mm): {result['Dimensions (mm)']}"

    def get_heat_treatment(self, response):
        # Initialize result with default values
        result = {
            "heat_treatment": "",
            "add_remarks": ""
        }

        # Extract heat treatment and additional remarks
        heat_treatment = response.css('#pnl_1 td:contains("Heat treatment") + td ::text').get('')
        add_remarks = response.css('#pnl_1 td:contains("Additional remarks") + td ::text').get('')

        # Safely assign extracted values or default to empty strings
        result['heat_treatment'] = heat_treatment.strip() if heat_treatment else ""
        result['add_remarks'] = add_remarks.strip() if add_remarks else ""

        # Format the result as a string
        return (
            f"Heat Treatment Type: {result['heat_treatment']}\n"
            f"Additional Remarks: {result['add_remarks']}"
        )

    def get_micro_str(self, response):
        # Initialize result with default values
        result = {
            "mic_info": "",
            "add_remarks": ""
        }

        # Extract heat treatment and additional remarks
        mic_info = response.css('#pnl_1 td:contains("Microstructure") + td ::text').get('')
        add_remarks = response.css('#pnl_1 tr:contains("Microstructure") + tr td:nth-child(2) ::text').get('')

        # Safely assign extracted values or default to empty strings
        result['mic_info'] = mic_info.strip() if mic_info else ""
        result['add_remarks'] = add_remarks.strip() if add_remarks else ""

        # Format the result as a string
        return (
            f"Microstructure Information: {result['mic_info']}\n"
            f"Additional Remarks: {result['add_remarks']}"
        )

    def get_hardness(self, response):
        def parse_float(value):
            """Helper function to parse a string to float safely."""
            try:
                return float(value.strip())
            except (ValueError, AttributeError):
                return ''

            # Extract and parse values

        brinell = parse_float(response.css('#pnl_1 td:contains("Brinell") + td::text').get(''))
        vickers = parse_float(response.css('#pnl_1 td:contains("Vickers") + td::text').get(''))
        rockwell = parse_float(response.css('#pnl_1 td:contains("Rockwell") + td::text').get(''))

        # Format and return the result
        return (
            f"Hardness Value (Brinell): {brinell}\n"
            f"Hardness Value (Vickers): {vickers}\n"
            f"Hardness Value (Rockwell): {rockwell}"
        )

    def get_test_condition(self, response):
        result = {
            "Testing Temperature (°C)": '',
            "Testing Medium": "",
            "Loading Type": "",
            "Loading Control": "",
            "Specimen Information": "",
            "Strain Rate": "",
            "Additional Remarks": ""
        }

        # Find the table rows
        rows = response.css('#pnl_12 table tr')

        for row in rows:
            # Extract the key (header) and value from each row
            header = row.css('td.w240.even b::text').get('').replace('\xa0', ' ')
            value = row.css('td:nth-child(2)::text').get('').replace('\xa0', ' ')

            # Match the header with the appropriate key in the result dictionary
            if header and value:
                header = header.strip().lower()

                try:
                    if "testing temperature" in header:
                        result["Testing Temperature (°C)"] = float(value.strip())
                    elif "testing medium" in header:
                        result["Testing Medium"] = value.strip()
                    elif "loading type" in header:
                        result["Loading Type"] = value.strip()
                    elif "loading control" in header:
                        result["Loading Control"] = value.strip()
                    elif "specimen" in header:
                        result["Specimen Information"] = value.strip()
                except Exception as e:
                    # If any error occurs while processing, just move to the next header
                    print(f"Error processing {header}: {e}")
                    continue

        try:
            # Get Additional Remarks and Strain Rate separately
            text = response.css('#pnl_12 table tr:contains("Aditional remarks") td:not(.even)::text').get('')
            if text:
                add_remarks = ''.join(text.split('strain rate:')[0:1]) if text.split('strain rate:')[0] else ''
                if add_remarks:
                    result["Additional Remarks"] = add_remarks.replace(';', '').strip()
                result["Strain Rate"] = ''.join(text.split('strain rate:')[1:])
            else:
                result["Additional Remarks"] = ''
                result["Strain Rate"] = ''
        except Exception as e:
            # Handle any error that occurs when processing Additional Remarks and Strain Rate
            self.write_logs(f"Error processing Additional Remarks or Strain Rate: {e}")
            result["Additional Remarks"] = ''
            result["Strain Rate"] = ''

        # Format the output1
        return (
            f"Testing Temperature (°C): {result['Testing Temperature (°C)']}\n"
            f"Testing Medium: {result['Testing Medium']}\n"
            f"Loading Type: {result['Loading Type']}\n"
            f"Loading Control: {result['Loading Control']}\n"
            f"Specimen Information: {result['Specimen Information']}\n"
            f"Strain Rate: {result['Strain Rate']}\n"
            f"Additional Remarks: {result['Additional Remarks']}"
        )

    def get_mono_properties(self, response):
        result = {
            "young_module": 0.0,
            "poisson_ratio": 0.0,
            "yield_str": 0.0,
            "ult_tensile_str": 0.0,
            "elongation": 0.0,
            "reduction_of_era": 0.0,
            "fracture_stress": 0.0,
            "fracture_strain": 0.0,
        }

        # Find the table rows
        rows = response.css('#pnl_2 table tr')

        for row in rows:
            # Extract the header (key) and value for each property
            header = row.css('td.even b::text').get('')
            value = row.css('td.centered50::text').get('').replace('\xa0', ' ')

            # Clean and map the extracted values
            if header:
                header = header.strip().lower()
                if value:
                    value = value.replace(',', '.').strip()  # Replace comma with dot for float conversion
                else:
                    value = "0.0"  # Default to "0.0" if value is missing

                try:
                    # Parse and assign values based on matching headers
                    if "young's modulus" in header:
                        result["young_module"] = float(value)
                    elif "poisson's ratio" in header:
                        result["poisson_ratio"] = float(value)
                    elif "yield strength" in header:
                        result["yield_str"] = float(value)
                    elif "ultimate tensile strength" in header:
                        result["ult_tensile_str"] = float(value)
                    elif "elongation" in header:
                        result["elongation"] = float(value)
                    elif "reduction of area" in header:
                        result["reduction_of_era"] = float(value)
                    elif "true fracture stress" in header:
                        result["fracture_stress"] = float(value)
                    elif "true fracture strain" in header:
                        result["fracture_strain"] = float(value)
                except ValueError:
                    # If parsing fails, keep the default value (0.0)
                    pass

        return result

    def get_mono_osgood_model(self, response):
        def extract_and_clean(selector, default='0.0'):
            """
            Extracts, normalizes, and converts a value to float.
            Handles encoding issues and assigns a default value if extraction fails.
            """
            value = response.css(selector).get(default)
            if value:
                # Normalize text: replace commas with periods, strip spaces, and handle non-breaking spaces
                value = value.strip().replace('\xa0', ' ').replace(',', '.')
            try:
                # Convert to float
                return float(value)
            except ValueError:
                return 0.0  # Fallback to default if conversion fails

        st_coefficient = extract_and_clean('#pnl_2_1 td:contains("Strength coefficient") + td::text')
        st_hard = extract_and_clean('#pnl_2_1 td:contains("Strain hardening") + td::text')

        return (
            f"Strength Coefficient (N/mm⁻²): {st_coefficient}\n"
            f"Strain Hardening Exponent: {st_hard}"
        )

    def get_test_cond_speciman_info(self, response, key):
        selector = response.css(f'#pnl_4 tr td:contains("{key}") + td::text').get('0.0')
        if isinstance(selector, str):
            selector = selector.replace(',', '.')

            # Convert the value to float (if it’s a valid number)
            try:
                selector = float(selector)
            except ValueError:
                selector = selector # If conversion fails, default to 0.0

            # Return the float value
            return selector

    def get_cyclic_add_remarks(self, response):
        def extract_and_clean(selector, default='0.0'):
            """
            Extracts, normalizes, and converts a value to float.
            Handles encoding issues and assigns a default value if extraction fails.
            """
            value = response.css(selector).get(default)
            if value:
                # Normalize text: replace commas with periods, strip spaces, and handle non-breaking spaces
                value = value.strip().replace('\xa0', ' ').replace(',', '.')
            try:
                # Convert to float
                return float(value)
            except ValueError:
                return 0.0  # Fallback to default if conversion fails

        # Extract and process values
        yield_strgth = extract_and_clean('#pnl_4_1 td:contains("Cyclic yield strength") + td::text')
        strhgth_coeffcient = extract_and_clean('#pnl_4_2 td:contains("Cyclic strength") + td::text')
        strain_hardening = extract_and_clean('#pnl_4_2 td:contains("strain hardening") + td::text')

        # Return the results as formatted string
        return (
            f"Cyclic Yield Strength (N/mm⁻²): {yield_strgth}\n"
            f"Cyclic Strength Coefficient (N/mm⁻²): {strhgth_coeffcient}\n"
            f"Cyclic Strain Hardening Exponent: {strain_hardening}"
        )

    def get_fatigue_properties(self, response):
        keys = ['Transition life', 'cycles corresponding to the endurance/fatigue limit', 'Stress amplitude at the number',
                'Strain amplitude at the number of cycles','scatter band of stress amplitudes for',
                'scatter band of plastic', 'Exponent of S-N curve']

        # Initialize variables with default values
        transition_life = 0
        endurance_cycles = 0
        stress_amplitude = 0.0
        strain_amplitude = 0.0
        stress_scatter_band = 0.0
        strain_scatter_band = 0.0
        exponent_sn_curve = 0.0

        for key in keys:
            selector = response.css(f'#pnl_5 td:contains("{key}") + td::text').get('')
            if selector:
                # Clean and normalize extracted text
                selector = selector.strip().replace('\xa0', ' ').replace(',', '.')

            if key=='Transition life' or key=='cycles corresponding to the endurance/fatigue limit':
                if isinstance(selector, str) and selector:
                    try:
                        selector = int(selector)
                    except ValueError:
                        selector = 0
            else:
                if isinstance(selector, str) and selector:
                    selector = selector.replace(',', '.')
                try:
                    selector = float(selector)
                except Exception:
                    selector = 0.0

            # Assign the extracted value to the correct variable based on the key
            if key == 'Transition life':
                transition_life = selector
            elif key == 'cycles corresponding to the endurance/fatigue limit':
                endurance_cycles = selector
            elif key == 'Stress amplitude at the number':
                stress_amplitude = selector
            elif key == 'Strain amplitude at the number of cycles':
                strain_amplitude = selector
            elif key == 'scatter band of stress amplitudes for':
                stress_scatter_band = selector
            elif key == 'scatter band of plastic':
                strain_scatter_band = selector
            elif key == 'Exponent of S-N curve':
                exponent_sn_curve = selector

        # Return formatted output1 as a string
        return (
            f"Transition Life (Number of Cycles): {transition_life}\n"
            f"Endurance/Fatigue Limit Cycles (NE): {endurance_cycles}\n"
            f"Stress Amplitude at NE (N/mm⁻²): {stress_amplitude}\n"
            f"Strain Amplitude at NE: {strain_amplitude}\n"
            f"Scatter Band of Stress Amplitudes (σa,10% / σa,90%): {stress_scatter_band}\n"
            f"Scatter Band of Plastic Strain Amplitudes (εa,p,10% / εa,p,90%): {strain_scatter_band}\n" '(εa,p,10% / εa,p,90%)'
            f"Exponent of S-N Curve: {exponent_sn_curve}"
        )

    def get_fatigue_parameters(self, response):
        keys = ['strength coefficient', 'strength exponent', 'ductility coefficient', 'ductility exponent']

        strength_coefficient = 0.0
        strength_exponent = 0.0
        ductility_coefficient = 0.0
        ductility_exponent = 0.0

        for key in keys:
            selector = response.css(f'#pnl_5_1 td:contains("{key}") + td::text').get('0.0')
            if selector:
                # Normalize and clean the extracted text
                selector = (
                    selector.strip()  # Remove leading and trailing whitespace
                    .replace('\xa0', ' ')  # Replace non-breaking spaces
                    .replace(',', '.')  # Replace commas with periods for decimal conversion
                )

            else:
                selector = '0.0'

            # if isinstance(selector, str) and selector:
            #     selector = selector.replace(',', '.')
            try:
                selector = float(selector)
            except Exception:
                selector = 0.0

            # Assign the extracted value to the correct variable based on the key
            if key == 'strength coefficient':
                strength_coefficient = selector
            elif key == 'strength exponent':
                strength_exponent = selector
            elif key == 'ductility coefficient':
                ductility_coefficient = selector
            elif key == 'ductility exponent':
                ductility_exponent = selector

        # Return the formatted result
        return (
            f"Fatigue Strength Coefficient (N/mm⁻²): {strength_coefficient}\n"
            f"Fatigue Strength Exponent: {strength_exponent}\n"
            f"Fatigue Ductility Coefficient: {ductility_coefficient}\n"
            f"Fatigue Ductility Exponent: {ductility_exponent}"
        )