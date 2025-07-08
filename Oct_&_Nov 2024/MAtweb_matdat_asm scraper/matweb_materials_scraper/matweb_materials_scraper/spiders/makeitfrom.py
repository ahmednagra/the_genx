from urllib.parse import urljoin
from collections import OrderedDict

from scrapy import Request

from .base import BaseSpider


class MakeitSpider(BaseSpider):
    name = "Make_It_From"

    fields = [
                'Material','Category', 'URL', 'Description', 'Alloy Composition',
                # Mechanical Properties
                'Compressive (crushing) Strength (MPa)', "Elastic (Young's, Tensile) Modulus (GPa)",'Flexural Strength (MPa)',
                'Fracture Toughness (MPa·m)', 'Knoop Hardness', "Poisson's Ratio", 'Tensile Strength: Ultimate (uts) (MPa)',
                'Elongation At Break (%)', 'Shear Modulus (GPa)', 'Reduction In Area (%)', 'Brinell Hardness', 'Rockwell B Hardness',
                'Rockwell C Hardness', 'Rockwell M Hardness',
                #Thermal Properties
                'Maximum Temperature: Mechanical (°C)', 'Shear Strength (MPa)', 'Maximum Thermal Shock (°C)', 'Specific Heat Capacity (J/kg·K)',
                'Thermal Conductivity (W/m·K)', 'Melting Onset (solidus) (°C)', 'Melting Completion (liquidus) (°C)', 'Thermal Expansion (µm/m·K)',
                'Glass Transition Temperature (°C)', 'Latent Heat Of Fusion (J/g)', 'Tensile Strength: Yield (proof) (MPa)', 'Fatigue Strength (MPa)',
                # Other Material Properties
                'Density (g/cm³)', 'Dielectric Constant (relative Permittivity) At 1Hz', 'Dielectric Constant (relative Permittivity) At 1Mhz',
                'Dielectric Strength (breakdown Potential) (kV/mm)', 'Electrical Resistivity Order Of Magnitude (10x Ω-m)', 'Limiting Oxygen Index (loi) (%)',
                'Embodied Water (L/kg)', 'Embodied Energy (MJ/kg)', 'Embodied Carbon (kg CO₂)', 'Base Metal Price (% relative)', 'Light Transmission Range (µm)',
                'Refractive Index',
                # Common Calculations
                'Stiffness To Weight: Axial (points)', 'Stiffness To Weight: Bending (points)', 'Strength To Weight: Axial (points)', 'Strength To Weight: Bending (points)',
                'Thermal Diffusivity (mm²/s)', 'Thermal Shock Resistance (points)', 'Resilience: Ultimate (unit Rupture Work) (MJ/m³)', 'Resilience: Unit (modulus Of Resilience) (kJ/m³)',
                'Electrical Conductivity: Equal Weight (specific) (% IACS)', 'Electrical Dissipation At 1Hz', 'Electrical Dissipation At 1MHz', 'Electrical Conductivity: Equal Volume (% IACS)',
                'Pren (pitting Resistance)',
                # Not Yet Cross_Match from Website
                'Curie Temperature (°C)', 'Flexural Modulus (GPa)', 'Follow-up Questions',
                # 'Further Reading',  its arises multiple errors for multiple tile so commented
                'Heat Deflection Temperature At 1.82Mpa (264Psi) (°C)',
                'Impact Strength: Notched Izod (J/m)', 'Impact Strength: V-notched Charpy (J)', 'Maximum Temperature: Autoignition (°C)', 'Maximum Temperature: Corrosion (°C)',
                'Maximum Temperature: Decomposition (°C)', "Solidification (pattern Maker's) Shrinkage (%)", 'Vicat Softening Temperature (°C)', 'Water Absorption After 24 Hours (%)',
                'Water Absorption At Saturation (%)'
            ]

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        url = 'https://www.makeitfrom.com'
        yield Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        all_links = response.css('.break-mid a::attr(href)').getall()[:18]
        all_categories = response.css('.break-mid a::text').getall()[:18]
        cat_dict = zip(all_categories, all_links)

        for title,link in cat_dict:
            category = title
            link = urljoin(response.url,link)
            yield Request(link, callback=self.parse_groups, headers=self.headers, meta={'category': category})

    def parse_groups(self, response):
        group_links = response.css('.links.break-mid div a::attr(href)').getall() or []
        title = response.css('.anchor[name="Intro"]')

        if title:
            cat = response.meta.get('category', '')
            yield from self.parse_detail(response=response, category=cat)

        for link in group_links:
            url = urljoin(response.url, link)
            skipped_urls = ['explore', 'compare']
            if any(word in url for word in skipped_urls):
                continue

            yield Request(url, callback=self.parse_groups, headers=self.headers, meta=response.meta)

    def parse_detail(self, response, category):
        try:
            item = OrderedDict()

            title = ''.join(response.css('h1 .nowrap-mid ::text').getall()).strip() or ''.join(response.css('title::text').get('').split(':')[0:1]).strip()
            item['Material'] = title
            item['Category'] = category
            item['URL'] = response.url
            item['Description'] = ''.join(response.css('div.prose p ::text').getall()).strip()
            alloy_compos =  str(self.get_alloy_info(response))
            item['Alloy Composition'] =  alloy_compos if not 'N/A' else ''

            #Mechanical Properties
            item['Compressive (crushing) Strength (MPa)'] = response.css('section p:contains("Compressive (Crushing) Strength") ~ p::text').get('').strip()
            item["Elastic (Young's, Tensile) Modulus (GPa)"] = response.css('section p:contains("Elastic (Young") ~ p::text').get('').strip()
            item['Flexural Strength (MPa)'] = response.css('section p:contains("Flexural Strength") ~ p::text').get('').strip()
            item['Fracture Toughness (MPa·m)'] = response.css('section p:contains("Fracture Toughness") ~ p::text').get('').strip()
            item['Knoop Hardness'] = response.css('section p:contains("Knoop Hardness") ~ p::text').get('').strip()
            item["Poisson's Ratio"] = response.css('section p:contains("Poisson") ~ p::text').get('').strip()
            item['Tensile Strength: Ultimate (uts) (MPa)'] = response.css('section p:contains("Tensile Strength: Ultimate") ~ p::text').get('').strip()
            item['Elongation At Break (%)'] = response.css('section p:contains("Elongation at Break") ~ p::text').get('').strip()
            item['Shear Modulus (GPa)'] = response.css('section p:contains("Shear Modulus") ~ p::text').get('').strip()
            item['Reduction In Area (%)'] = response.css('section p:contains("Reduction in Area") ~ p::text').get('').strip()
            item['Brinell Hardness'] = response.css('section p:contains("Brinell Hardness") ~ p::text').get('').strip()
            item['Rockwell B Hardness'] = response.css('section p:contains("Rockwell B Hardness") ~ p::text').get('').strip()
            item['Rockwell C Hardness'] = response.css('section p:contains("Rockwell C Hardness") ~ p::text').get('').strip()
            item['Rockwell M Hardness'] = response.css('section p:contains("Rockwell M Hardness") ~ p::text').get('').strip()

            #Thermal Properties
            item['Maximum Temperature: Mechanical (°C)'] = response.css('section p:contains("Maximum Temperature: Mechanical") ~ p::text').get('').strip()
            item['Shear Strength (MPa)'] = response.css('section p:contains("Shear Strength") ~ p::text').get('').strip()
            item['Maximum Thermal Shock (°C)'] = response.css('section p:contains("Maximum Thermal Shock") ~ p::text').get('').strip()
            item['Specific Heat Capacity (J/kg·K)'] = response.css('section p:contains("Specific Heat Capacity") ~ p::text').get('').strip()
            item['Thermal Conductivity (W/m·K)'] = response.css('section p:contains("Thermal Conductivity") ~ p::text').get('').strip()
            item['Melting Onset (solidus) (°C)'] = response.css('section p:contains("Melting Onset (Solidus)") ~ p::text').get('').strip()
            item['Melting Completion (liquidus) (°C)'] = response.css('section p:contains("Melting Completion (Liquidus)") ~ p::text').get('').strip()
            item['Thermal Expansion (µm/m·K)'] = response.css('section p:contains("Thermal Expansion") ~ p::text').get('').strip()
            item['Glass Transition Temperature (°C)'] = response.css('section p:contains("Glass Transition Temperature") ~ p::text').get('').strip()
            item['Latent Heat Of Fusion (J/g)'] = response.css('section p:contains("Latent Heat of Fusion") ~ p::text').get('').strip()
            item['Tensile Strength: Yield (proof) (MPa)'] = response.css('section p:contains("Tensile Strength: Yield (Proof)") ~ p::text').get('').strip()
            item['Fatigue Strength (MPa)'] = response.css('section p:contains("Fatigue Strength") ~ p::text').get('').strip()

            #Other Material Properties
            item['Density (g/cm³)'] = response.css('section p:contains("Density") ~ p::text').get('').strip()
            item['Dielectric Constant (relative Permittivity) At 1Hz'] = response.css('section p:contains("Dielectric Constant (Relative Permittivity) At 1 Hz") ~ p::text').get('').strip()
            item['Dielectric Constant (relative Permittivity) At 1Mhz'] = response.css('section p:contains("Dielectric Constant (Relative Permittivity) At 1 MHz") ~ p::text').get('').strip()
            item['Dielectric Strength (breakdown Potential) (kV/mm)'] = response.css('section p:contains("Dielectric Strength (Breakdown Potential)") ~ p::text').get('').strip()
            item['Electrical Resistivity Order Of Magnitude (10x Ω-m)'] = response.css('section p:contains("Electrical Resistivity Order of Magnitude") ~ p::text').get('').strip()
            item['Limiting Oxygen Index (loi) (%)'] = response.css('section p:contains("Limiting Oxygen Index (LOI)") ~ p::text').get('').strip()
            item['Embodied Water (L/kg)'] =response.css('section p:contains("Embodied Water") ~ p::text').get('').strip()
            item['Embodied Energy (MJ/kg)'] =response.css('section p:contains("Embodied Energy") ~ p::text').get('').strip()
            item['Embodied Carbon (kg CO₂)'] = response.css('section p:contains("Embodied Carbon") ~ p::text').get('').strip()
            item['Base Metal Price (% relative)'] = response.css('section p:contains("Base Metal Price") ~ p::text').get('').strip()
            item['Light Transmission Range (µm)'] = response.css('section p:contains("Light Transmission Range") ~ p::text').get('').strip()
            item['Refractive Index'] = response.css('section p:contains("Refractive Ind") ~ p::text').get('').strip()

            #Common Calculations
            item['Stiffness To Weight: Axial (points)'] = response.css('section p:contains("Stiffness to Weight: Axial") ~ p::text').get('').strip()
            item['Stiffness To Weight: Bending (points)'] = response.css('section p:contains("Stiffness to Weight: Bending") ~ p::text').get('').strip()
            item['Strength To Weight: Axial (points)'] = response.css('section p:contains("Strength to Weight: Axial") ~ p::text').get('').strip()
            item['Strength To Weight: Bending (points)'] = response.css('section p:contains("Strength to Weight: Bending") ~ p::text').get('').strip()
            item['Thermal Diffusivity (mm²/s)'] = response.css('section p:contains("Thermal Diffusivity") ~ p::text').get('').strip()
            item['Thermal Shock Resistance (points)'] = response.css('section p:contains("Thermal Shock Resistance") ~ p::text').get('').strip()
            item['Resilience: Ultimate (unit Rupture Work) (MJ/m³)'] = response.css('section p:contains("Resilience: Ultimate (Unit Rupture Work)") ~ p::text').get('').strip()
            item['Resilience: Unit (modulus Of Resilience) (kJ/m³)'] = response.css('section p:contains("Resilience: Unit (Modulus of Resilience)") ~ p::text').get('').strip()
            item['Pren (pitting Resistance)'] = response.css('section p:contains("PREN (Pitting Resistance)") ~ p::text').get('').strip()

            #Electrical Properties
            item['Electrical Conductivity: Equal Weight (specific) (% IACS)'] = response.css('section p:contains("Electrical Conductivity: Equal Weight (Specific)") ~ p::text').get('').strip()
            item['Electrical Dissipation At 1Hz'] = response.css('section p:contains("Electrical Dissipation At 1 Hz") ~ p::text').get('').strip()
            item['Electrical Dissipation At 1MHz'] = response.css('section p:contains("Electrical Dissipation At 1 MHz") ~ p::text').get('').strip()
            item['Electrical Conductivity: Equal Volume (% IACS)'] = response.css('section p:contains("Electrical Conductivity: Equal Volume") ~ p::text').get('').strip()

            # Not Yet Cross_Match from Website
            item['Curie Temperature (°C)'] = response.css('section p:contains("Curie Temperature") ~ p::text').get('').strip()
            item['Flexural Modulus (GPa)'] = response.css('section p:contains("Flexural Modulus") ~ p::text').get('').strip()
            item['Follow-up Questions'] =response.css('section p:contains("Follow-up Questions") ~ p::text').get('').strip()
            item['Further Reading'] = response.css('section p:contains("Further Reading") ~ p::text').get('').strip()
            item['Heat Deflection Temperature At 1.82Mpa (264Psi) (°C)'] = response.css('section p:contains("Heat Deflection Temperature At 1.82Mpa") ~ p::text').get('').strip()
            item['Impact Strength: Notched Izod (J/m)'] = response.css('section p:contains("Impact Strength: Notched Izod") ~ p::text').get('').strip()
            item['Impact Strength: V-notched Charpy (J)'] = response.css('section p:contains("Impact Strength: V-notched Charpy") ~ p::text').get('').strip()
            item['Maximum Temperature: Autoignition (°C)'] = response.css('section p:contains("Maximum Temperature: Autoignition") ~ p::text').get('').strip()
            item['Maximum Temperature: Corrosion (°C)'] = response.css('section p:contains("Maximum Temperature: Corrosion") ~ p::text').get('').strip()
            item['Maximum Temperature: Decomposition (°C)'] = response.css('section p:contains("Maximum Temperature: Decomposition") ~ p::text').get('').strip()

            item["Solidification (pattern Maker's) Shrinkage (%)"] = response.css('section p:contains("Solidification (pattern Make") ~ p::text').get('').strip()
            item['Vicat Softening Temperature (°C)'] = response.css('section p:contains("Vicat Softening Temperature") ~ p::text').get('').strip()
            item['Water Absorption After 24 Hours (%)'] = response.css('section p:contains("Water Absorption After 24 Hours") ~ p::text').get('').strip()
            item['Water Absorption At Saturation (%)'] = response.css('section p:contains("Water Absorption At Saturation") ~ p::text').get('').strip()

            if title:
                self.ready_records_counter += 1
                self.all_categories_found_records += 1
                print(f'Records Ready: {self.ready_records_counter}')
                self.write_xlsx(record=item, key=False)
                # yield item  # Ensure the item is yielded
            else:
                self.write_logs(f'Not title found URL: {response.url}')

            # Follow additional material links if present on the web page
            yield from self.get_materials_requests(response)
        except Exception as e:
            self.write_logs(f'Url: {response.url} Record yield Error : {e}')

    def get_materials_requests(self, response):
        materials_urls = response.xpath('//section[a[@name="Intro"]]//div[contains(@class, "break-mid")]//a/@href').getall()
        if materials_urls:
            for url in materials_urls:
                link = urljoin(response.url, url)
                # cat = response.meta.get('category', '')
                yield Request(link, callback=self.parse_groups, headers=self.headers, meta=response.meta)

    def get_alloy_info(self, response):
        try:
            alloy_table = response.css('table.comps tr')

            if not alloy_table:
                return 'N/A'

            info_dict = {}
            for row in alloy_table:
                name = row.css('.inline-narrow ::text').get(default='').strip()
                value = row.css('td:not(.comp-bars)::text').get(default='').strip().replace(' to ', '-')
                if name:  # Ensure the name is not empty
                    info_dict[name] = value if value else 'N/A'

            return info_dict
        except Exception as e:
            return 'N/A'