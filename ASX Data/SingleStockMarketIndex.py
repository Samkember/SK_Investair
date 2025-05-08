import undetected_chromedriver as uc
import pandas as pd
import json
import time

def fetch_asx_companies_via_browser():
    url = "https://www.marketindex.com.au/api/v1/companies"

    options = uc.ChromeOptions()
    options.headless = True
    driver = uc.Chrome(options=options)

    try:
        driver.get("https://www.marketindex.com.au/asx/mqg")
        time.sleep(3)

        driver.get(url)
        time.sleep(2)

        # Get JSON rendered directly
        response_text = driver.find_element("tag name", "pre").text
        data = json.loads(response_text)

        df = pd.DataFrame(data)
        df.to_csv("marketindex_asx_companies.csv", index=False)
        print("✅ Saved to marketindex_asx_companies.csv")
        return df

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        driver.quit()

# Run it
df = fetch_asx_companies_via_browser()
if df is not None:
    print(df.head())
