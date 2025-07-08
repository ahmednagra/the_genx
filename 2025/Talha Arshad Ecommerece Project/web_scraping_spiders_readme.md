
# Web Scraping Spiders for Various E-commerce Sites

This Project contains Scrapy spiders designed to scrape product details from a variety of e-commerce websites. The spiders are designed to collect comprehensive product information such as product names, prices, availability, descriptions, images, sizes, and more. 

The spiders output the scraped data in a structured JSON format, which can be used for further analysis or processing.

## Project Structure

```plaintext
Ecommerce_Websites_Scraper/
│
├── Ecommerce_Websites_Scraper/                # Scrapy project folder
│   ├── spiders/                               # Contains the spider files
│   │   ├── Bathandbody_spider.py              # Scraper for Bath & Body Works
│   │   ├── BloomingDales_spider.py            # Scraper for Bloomingdales
│   │   ├── Diesel_spider.py                  # Scraper for Diesel
│   │   ├── Farfetch_spider.py                # Scraper for Farfetch
│   │   ├── LuxuryCloset_spider.py            # Scraper for LuxuryCloset
│   │   ├── MAje_spider.py                    # Scraper for MAje
│   │   ├── NewBalance_spider.py              # Scraper for NewBalance
│   │   ├── TedBaker_spider.py                # Scraper for TedBaker
│   │   └── ...                               # Additional spiders can be added here
│   ├── __init__.py                            # Initialization file for the Scrapy project
│   ├── items.py                              # Defines the data structure for scraped items
│   ├── middlewares.py                        # Contains custom middleware for handling requests/responses
│   ├── pipelines.py                          # Defines pipelines for processing scraped data
│   ├── settings.py                           # Configuration settings for the Scrapy project
│   └── ...                                   # Other project files
├── run_spider.bat                            # Script to run the spiders (with spider selection options)
└── web_scraping_spiders_readme.md                                 # Project instructions (this file)

```

## Spider Descriptions

### 1. **Bath & Body Works Scraper**
This Scrapy spider is designed to scrape product details from the **Bath & Body Works Saudi Arabia** website. It extracts product information such as name, price, availability, category, and promotions using API requests to Algolia's search service and the website's GraphQL API.

**Key Features:**
- Scrapes multiple categories including Candles, Body Care, Hand Soaps & Sanitizers, and Fresheners.
- Use Algolia API to fetch product listings.
- Fetches detailed product data via GraphQL requests.
- Implements robust error handling and logging for debugging.
- Supports proxy usage via Zyte for reliable scraping.

**Data Extracted:**
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Images, and More.

---

### 2. **Bloomingdale's UAE Scraper**
This Scrapy spider extracts product details from the **Bloomingdale's UAE** website. It scrapes product listings by category, retrieves detailed product data, and paginates through available products efficiently.

**Key Features:**
- Extracts product details (title, price, brand, sizes, colors, availability, etc.).
- Uses structured API requests and GraphQL queries to fetch accurate data.
- Implements robust error handling and logging for debugging.
- Supports pagination to ensure full product coverage.
- Avoid duplicate scraping by tracking previously scraped items.

**Data Extracted:**
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Images, and More.

---

### 3. **Diesel Scraper**
This Scrapy spider extracts product details from **Diesel's** website. It navigates through categories, extracts product URLs, and scrapes product details.

---

### 4. **Farfetch Scraper**
FarfetchSpider is a Scrapy spider designed to scrape product information from the **Farfetch** website.

**Key Features:**
- Handles category parsing, brand parsing, and product detail parsing.
- Retrieve detailed information such as product title, brand, pricing, availability, sizes, colors, images, and more.
- Fetches size and variation data from a secondary endpoint.
- Implements retries, timeouts, and logging for robust scraping.

**Data Extracted:**
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Images, and More.

---

### 5. **Luxury Closet Scraper**
The **Luxury Closet** Spider is designed to scrape product details from the **Luxury Closet** website, an online luxury fashion marketplace.

**Key Features:**
- Scrapes product details such as title, brand, price, description, images, and sizes.
- Supports pagination to navigate through multiple pages of product listings.
- Handles product details extraction, including nested lists for sizes and other attributes.
- Uses Scrapy’s retry mechanism for robust scraping.
- Outputs scraped data in JSON format with product ID, title, and more.

**Data Extracted:**
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Images, and More.

---

### 6. **Maje Scraper**
A Scrapy spider for scraping product details from the **Maje (Saudi Arabia)** website.

**Key Features:**
- Scrapes product information such as product ID, title, brand, category, pricing, and sizes.
- Collects data about discounts, images, colors, and other product attributes.
- Use BeautifulSoup for parsing product descriptions and handling HTML formatting for material and care information.

**Data Extracted:**
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Images, and More.

---

### 7. **New Balance Scraper**
A Scrapy spider to scrape product details from the **New Balance Saudi Arabia** website.

**Key Features:**
- Extracts product data using Scrapy, Algolia API, and GraphQL requests.
- Captures product title, price, stock status, images, variations, and descriptions.
- Scrapes paginated product listings and formats the results into a structured JSON file.

**Data Extracted:**
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Images, and More.

---

### 8. **Ted Baker Scraper**
A Scrapy spider to scrape product details from the **Ted Baker Saudi Arabia** website.

**Key Features:**
- Scrapes product details from category pages and API responses.
- Handles pagination and dynamically constructs API requests.
- Captures product metadata, including images, sizes, and stock status.

**Data Extracted:**
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Images, and More.

---

### 9. **Sandro Scraper**
- Sandro Spider for extracting product details from Sandro's Saudi Arabia website.
- The spider navigates through the main categories (e.g., Woman, Man), fetches the product listings, and extracts detailed information about each product. It also handles pagination to scrape multiple pages and supports retries for failed requests.

**Key Features:**
- Scrapes product data such as title, price, discount, description, images, colors, sizes, and more.
- Handles pagination to fetch data across multiple pages.
- Uses regular expressions to extract product and pagination data from JSON responses.
- Supports retries for failed requests with a retry limit and custom HTTP codes.
- Uses Scrapy's `OrderedDict` for structured output, saved in a timestamped JSON file.
- Collects detailed product data including product variants, availability, and additional product details like delivery, returns, and payment methods.
- Captures metadata in a structured JSON format suitable for further processing or analysis.

**Data Extracted:**
- Product Title, Price, Discount, Stock Status, Brand, Category, Size, Images, and More.

---

## Data Output Format

The scraped data from all spiders is output in JSON format with the following fields:

```json
"fields": [
    'source_id', 'product_url', 'brand', 'product_title', 'product_id', 'category', 
    'price', 'discount', 'currency', 'description', 'main_image_url', 'other_image_urls', 
    'colors', 'variations', 'sizes', 'other_details', 'availability', 
    'number_of_items_in_stock', 'last_update', 'creation_date'
]
```

---

## Installation Instructions

```
To run this project, you need to have Python installed on your system. Follow the instructions below to install Python:

1. **Download Python**:  
   Go to the official Python website and download the latest version of Python from the following link:  
   https://www.python.org/downloads

2. **Install Python**:  
   During the installation process, **make sure to check the box that says** `Add Python.exe to PATH`. This ensures that Python is accessible from the command line.

3. **Verify Installation**:  
   After the installation is complete, open a command prompt or terminal and run the following command to verify the installation:

 
   python --version
 

This should display the Python version number, confirming that Python is installed and added to your system's `PATH`
````

Before running any spider, make sure you have Python installed on your system. Then, follow these steps:

## Prerequisites
- Python 3.x installed on your machine.
- An internet connection to install required dependencies and to extract the data.
Windows OS (for .bat file usage).

1. **Python**
- **Download Python**:  
   Go to the official Python website and download the latest version of Python from the following link:  
   https://www.python.org/downloads

- **Install Python**:  
   During the installation process, **make sure to check the box that says** `Add Python.exe to PATH`. This ensures that Python is accessible from the command line.

- **Verify Installation**:  
   After the installation is complete, open a command prompt or terminal and run the following command to verify the installation:

   ```bash
   python --version
  ```

2. **Install the required packages:**
   Make sure you have the `requirements.txt` file in the root of the repository. Install the necessary dependencies with:

   ```bash
   pip install -r requirements.txt
   ```

   This will install Scrapy, the necessary libraries for proxy usage (e.g., Zyte), and any other dependencies specified.

---

## Running the Spiders

To run any of the spiders, use the following command in your terminal:

```bash
scrapy crawl <spider_name>
```

For example, to run the **Bath and Body** spider, use:

```bash
scrapy crawl BathandBody
```

Each spider will scrape the relevant data and save it as a JSON file in the `output` directory. The filenames will include the current date and time to ensure they are unique and do not overwrite previous files.

---

## Proxy and Error Handling
BathandBody, Farfetch and NewBalances in this repository support Zyte (formerly Scrapinghub) proxies, ensuring anonymous scraping and handling errors like timeouts and retries.


## License

This project is licensed under the MIT License—see the [LICENSE](LICENSE) file for details.

