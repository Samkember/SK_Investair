import cloudscraper
from lxml import html
import csv
import time

# === Setup ===
scraper = cloudscraper.create_scraper()

# === First, get all fund URLs ===
sector_url = "https://citywire.com/selector/sector/equity-australia-small-and-medium-companies/i2642"
response = scraper.get(sector_url)

if response.status_code != 200:
    print(f"Failed to retrieve sector page. Status code: {response.status_code}")
    exit()

tree = html.fromstring(response.content)

# XPath to find funds table
table_xpath = '/html/body/div[4]/div/div[1]/div/div[2]/div[3]/div/div/div[2]/table/tbody'
table_body = tree.xpath(table_xpath)
if not table_body:
    print("Funds table not found.")
    exit()

# Extract fund URLs
rows = table_body[0].xpath('.//tr')
base_url = "https://citywire.com"
fund_urls = []

for row in rows:
    link = row.xpath('.//a[@href]')
    if link:
        fund_name = link[0].text_content().strip()
        fund_url = base_url + link[0].get('href')
        fund_urls.append((fund_name, fund_url))

print(f"Found {len(fund_urls)} fund URLs.")

# === Now scrape the Portfolio area from each fund ===
output_data = []

for fund_name, fund_url in fund_urls:
    print(f"Scraping portfolio for: {fund_name}")
    try:
        response = scraper.get(fund_url)
        if response.status_code != 200:
            print(f"Failed to retrieve {fund_url}. Skipping.")
            continue

        tree = html.fromstring(response.content)

        # Locate the Portfolio section
        portfolio_xpath = '/html/body/div[5]/div/div[1]/div/div[2]/div[9]'
        portfolio_elements = tree.xpath(portfolio_xpath)

        if portfolio_elements:
            portfolio_text = portfolio_elements[0].text_content().strip()
            output_data.append([fund_name, fund_url, portfolio_text])
        else:
            print(f"No portfolio section found for {fund_name}.")
            output_data.append([fund_name, fund_url, "Portfolio section not found."])
    
    except Exception as e:
        print(f"Error scraping {fund_name}: {e}")
        output_data.append([fund_name, fund_url, "Error occurred."])

    # Sleep politely between requests
    time.sleep(1)

# === Save results to CSV ===
output_csv = "funds_portfolio_sections.csv"

with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["Fund Name", "Fund URL", "Portfolio Information"])
    writer.writerows(output_data)

print(f"Saved all portfolio data to {output_csv}")
