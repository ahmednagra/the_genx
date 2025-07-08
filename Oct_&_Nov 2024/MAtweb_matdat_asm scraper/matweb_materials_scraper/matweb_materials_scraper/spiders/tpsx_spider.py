import json
from collections import defaultdict, OrderedDict
from .base import BaseSpider
import scrapy
from scrapy import Request
import csv, os


class TpsxDataSpider(BaseSpider):  # Inherit from BaseSpider
    name = "TPSX"

    fields = [
                'Name', 'Database', 'Category', 'Composition', 'Manufacturer',
        'Description', 'URL'
                ]

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }
    # Accumulate material data across requests
    materials_data = defaultdict(lambda: defaultdict(dict))

    def start_requests(self):
        url = "https://tpsx.arc.nasa.gov/database/?&sEcho=1&iColumns=4&sColumns=name%2Cdatabase%2Ccategory%2Clink&iDisplayStart=0&iDisplayLength=1549&mDataProp_0=0&bSortable_0=true&mDataProp_1=1&bSortable_1=true&mDataProp_2=2&bSortable_2=true&mDataProp_3=3&bSortable_3=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&_=1730786868578"
        yield Request(url, callback=self.parse, headers=self.headers)

    # Store dynamic headers
    dynamic_headers = set()

    def parse(self, response, **kwargs):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as e:
            self.write_logs(f'Error Decoding Json In Parse Function')
            return

        records = data.get("aaData", [])
        if not records:
            self.write_logs(f'No record found in Parse Function')

        for item in records:
            material = item[0].strip()
            database = item[1].strip()
            category = item[2].strip()
            url = f"https://tpsx.arc.nasa.gov/Material?id={item[3]}"
            print('Requested Url:', url)

            response.meta['Material'] = material
            response.meta['Database'] = database
            response.meta['Category'] = category
            response.meta['url'] = url

            yield Request(url, callback=self.parse_next, headers=self.headers, meta=response.meta)

    def parse_next(self, response):
        try:
            material = response.meta.get("Material", "")
            database = response.meta.get("Database", "")
            category = response.meta.get("Category", "")
            url = response.meta.get("url", "")

            composition = response.xpath("//p/strong[contains(text(), 'Composition:')]/following-sibling::text()[1]").get(
                default="N/A"
            ).strip()

            manufacturer = response.xpath("//p/strong[contains(text(), 'Manufacturer:')]/following-sibling::text()[1]").get(
                default="N/A"
            ).strip()

            description = response.xpath("//p[contains(., 'Description:')]/following-sibling::ul/li/text()").get(
                default="N/A"
            ).strip()

            item = OrderedDict()
            item['Name'] = response.css('#material h2::text').get('') or material
            item['Database'] = database
            item['Category'] = category
            item['Composition'] = composition
            item['Manufacturer'] = manufacturer
            item['Description'] = description
            item['URL'] = url

            for row in response.css("table tr")[1:]:
                name = row.css("td:nth-child(1)::text").get(default="").strip().replace(" ", "").replace('_-', '')
                # value = row.css("td:nth-child(2)::text").get(default="N/A").strip()
                value = float(row.css("td:nth-child(2)::text").get(0.0).strip())
                units = row.css("td:nth-child(3)::text").get(default="N/A").strip()
                uncertainty = row.css("td:nth-child(4)::text").get(0.0).strip()
                uncertainty = float(uncertainty) if 'N/A' not in uncertainty else uncertainty

                property_name = f"{name}_{units}"

                # Add property_name to self.fields if not already present
                if property_name not in self.fields:
                    self.fields.append(property_name)  # Add property field
                    self.fields.append(f"{property_name}_Uncertainty")  # Add uncertainty field

                # Store the property value and uncertainty in the item dictionary
                item[property_name] = value
                item[f"{property_name}_Uncertainty"] = uncertainty

            self.ready_records_counter += 1
            self.all_categories_found_records += 1
            print(f'Records Ready: {self.ready_records_counter}')
            self.write_xlsx(record=item, key=False)
        except Exception as e:
            self.write_logs(f'Url: {response.url} Record yield Error : {e}')

    def parse_property(self, response):
        material_data = response.meta["material_data"]
        property_name = response.meta["property_name"]

        # Add dynamic headers
        self.dynamic_headers.add(f"{property_name}_properties")
        property_temperature_key = f"{property_name}_properties"
        if property_temperature_key not in material_data:
            material_data[property_temperature_key] = {}

        for idx, row in enumerate(response.css("#materials-property-data-table tbody tr"), start=1):
            value = row.css("td:nth-child(1)::text").get(default="N/A").strip()
            temperature = row.css("td:nth-child(3)::text").get(default="N/A").strip()

            # Update material data
            material_data[property_temperature_key][f"value_{idx}"] = value or "N/A"
            material_data[property_temperature_key][f"temperature_{idx}"] = temperature or "N/A"

        # Save data after processing
        self.save_dynamic_csv()

    def save_dynamic_csv(self):
        """Save data with dynamic headers."""
        # Combine static and dynamic headers
        headers = self.fields + sorted(self.dynamic_headers)

        output_dir = 'output1'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_file = f'{output_dir}/{self.name} Materials Details {self.current_dt}.csv'

        # Write CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()

            for material_data in self.materials_data.values():
                row = {header: material_data.get(header, "N/A") for header in headers}
                writer.writerow(row)

