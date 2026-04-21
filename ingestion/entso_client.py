import os
import requests
from dotenv import load_dotenv
import xmltodict
from datetime import datetime, timedelta

load_dotenv()

url = os.environ.get("ENTSOE_URL")
token = os.environ.get("ENTSOE_TOKEN")


def fetch_entsoe_data(token, start_date, end_date) :
    params = {
        "securityToken": token,
        "documentType": "A44",
        "in_Domain": "10YFR-RTE------C",
        "out_Domain":"10YFR-RTE------C",
        "periodStart":start_date,
        "periodEnd":end_date
    }

    response = requests.get(url, params = params)
    response.raise_for_status()
    return xmltodict.parse(response.content)

def parse_entsoe_data(data):
    all_prices = []
    
    root = data.get('Publication_MarketDocument')
    if not root or 'TimeSeries' not in root:
        return []

    timeseries_list = root['TimeSeries']
    
    if not isinstance(timeseries_list, list):
        timeseries_list = [timeseries_list]

    for ts in timeseries_list:
        period = ts['Period']
        start_str = period['timeInterval']['start']
        # Conversion du texte "2023-12-31T23:00Z" en objet Python
        start_dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
        
        points = period['Point']
        if not isinstance(points, list):
            points = [points]

        for p in points:
            offset = int(p['position']) - 1
            price = float(p['price.amount'])
            
            all_prices.append({
                "timestamp_utc": (start_dt + timedelta(hours=offset)).isoformat(),
                "price_eur_mwh": price,
                "country_code": "FR"
            })
            
    return all_prices




