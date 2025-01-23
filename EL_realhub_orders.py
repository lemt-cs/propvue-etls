#order items

import pandas as pd
import os
import requests
import json
from tqdm import tqdm
import time
from pymongo import MongoClient
from datetime import datetime
import pytz
from dotenv import load_dotenv

load_dotenv()

mongodb_uri = os.getenv("MONGODB_URI")
client = MongoClient(mongodb_uri)
db = client.realhub
collection = db.orders
realhub_company = os.getenv("REALHUB_COMPANY")
realhub_company2 = os.getenv("REALHUB_COMPANY2")
realhub_subcompany = os.getenv("REALHUB_SUBCOMPANY")
apikey = os.getenv("REALHUB_APIKEY")

modified_since = '2023-01-01T11:00:00'

source_script = os.path.basename(__file__)

def update_document(data, office, subcomp, apikey):
    for x in data:
        if isinstance(x, dict):
            document = {
                "date_inserted_by_me": datetime.now(),
                "Source Script": source_script,
                "Company": office,
                "SubCompany": subcomp,
                "API Key": apikey
            }
            document.update(x)

            try:
                result = collection.update_one(
                        {"id": document["id"]},
                        {"$set": document},
                        upsert=True
                    )
            except Exception as e:
                print(e)

def get_data(url, apikey, company):
    r = requests.get(url, headers={'Accept': 'application/json',
                                'User-Agent': company,
                                'x-api-token': apikey,
                                })
    data = r.json()
    # print(f"number of items = {len(data)}\ncompany = {company}")
    return data, r

print(f"Now extracting for {realhub_subcompany}")
# put high maximum number as it will break if no data is available to extract
pages = range(1400*50, 200001, 50)
for y in tqdm(pages, total=len(pages), desc="Extracting per offset", ncols=70):
    url = f'https://realhub.realbase.io/api/v2/orders.json?modified_since={modified_since}&include_order_items=true&limit=50&offset={str(y)}'
    data, r = get_data(url, apikey, realhub_subcompany)
    update_document(data, realhub_company, realhub_subcompany, apikey)
    if len(data) == 0:
        print("no data now. breaking")
        break
