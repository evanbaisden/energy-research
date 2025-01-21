import requests
import pandas as pd
import os

def fetch_data_from_api(api_url, params=None):
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    data = response.json()
    return data

if __name__ == "__main__":
    eia_api_url = "https://api.eia.gov/v2/electricity/operating-generator-capacity/data/"
    params = {
        "api_key": "YOUR_EIA_API_KEY",  # sign up for an API key
        # ...
    }
    data = fetch_data_from_api(eia_api_url, params)
    df = pd.DataFrame(data['response']['data'])
    os.makedirs("data/raw", exist_ok=True)
    df.to_csv("data/raw/eia_data.csv", index=False)


import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_oil_prices(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    # Logic to find the relevant table or price data
    # ...
    # Create a DataFrame and return it
    return pd.DataFrame(...)

if __name__ == "__main__":
    oil_price_url = "https://example.com/oil-prices"
    df_prices = scrape_oil_prices(oil_price_url)
    df_prices.to_csv("data/raw/oil_prices.csv", index=False)