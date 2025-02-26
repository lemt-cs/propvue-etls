#Performance testing all metrics & all offices script
#run first the Offices script
import requests
import pandas as pd
from tqdm import tqdm
import json
import pytz
from datetime import datetime
import os
from datetime import datetime, timedelta
import calendar
from pymongo import MongoClient
from dotenv import load_dotenv
from office365.sharepoint.client_context import ClientContext

firstDate = '2025-02-01'
lastDate = '2025-02-28'
mongodb_identifier = f"{datetime.now().strftime("%Y-%m-%d")} for executive sales report"
load_dotenv()
mongodb_uri_local = os.getenv("MONGODB_URI_LOCAL")
client = MongoClient(mongodb_uri_local)
db = client['agentbox']
collection = db['offices']
dfOffices = list(collection.find())
dates_list = []
monthtext = pd.to_datetime(firstDate).strftime('%b')
realDate = pd.to_datetime(firstDate).strftime('%m-%d-%Y')
dates_list.append({'month': monthtext, 'first_date': firstDate, 'last_date': lastDate, 'realDate': realDate})

coll = db['all_performance']
print(f"ensure date coverage is correct: extracting for {firstDate} - {lastDate}")
df_list = []
df_list2 = []
print(f'start of month --> {x['month']}')
for i in tqdm(dfOffices, total=len(dfOffices)):
    base_url = 'https://api.agentboxcrm.com.au/reports/performance?filter[saleStatus]=UnconditionalByUnconditionalDate&filter[dateFrom]='
    first_date = firstDate
    last_date = lastDate
    mid_url = '&filter[dateTo]='
    end_url = f'&filter[memberOfficeId]={i['id']}&version=2'

    url_to_query = base_url + first_date + mid_url + last_date + end_url

    try:
        r = requests.get(url_to_query, 
                        headers={'Accept': 'application/json',
                                'X-Client-ID': i['Client ID'],
                                'X-API-Key': i['API Key']})
        data = r.json()
        new_metrics = []
        for_mongodb = {
            "date_synched": datetime.now(),
            "for_report": "executive_sales",
            "identifier": mongodb_identifier,
            "officeId": i['id'],
            "officeName": i['name'],
            "officeState": i['address']['state'],
            "all_performance": data['response']['report']['metrics']
        }
        coll.update_one({"identifier", for_mongodb['identifier']}, {"$set", for_mongodb}, upsert=True)
    except Exception as e:
        print(e)
        print(f"Error with {i['id']} {i['name']}")
