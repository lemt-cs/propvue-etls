#order items

import pandas as pd
import os
import requests
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.operators.python import PythonOperator
from airflow import DAG
import time

load_dotenv()
modified_since = '2024-01-01T11:00:00'
script_name = os.path.basename(__file__)
mongo_uri = os.getenv("MONGODB_URI_LOCAL")
client = MongoClient(mongo_uri)
db = client['realhub']
collection = db['sold_campaigns']
coll_api_keys = db['api_keys']
json_credentials = list(coll_api_keys.find())

def bulk_update_documents(data, company, subcompany, apikey):
    operations = []
    for x in data:
        if isinstance(x, dict):
            document = {
                "Company": company,
                "Sub Company": subcompany,
                "API Key": apikey,
            }
            document.update(x)
            operations.append(UpdateOne({"id": document['id']}, {"$set": document}, upsert=True))
        else:
            print(f'{x} is not a dictionary.')

    if operations:
        try:
            result = collection.bulk_write(operations)
            return result.upserted_count, result.modified_count
        except Exception as e:
            print(e)
    return 0, 0
    
def get_data(url, apikey, company):
    r = requests.get(url, headers={'Accept': 'application/json',
                                'User-Agent': company,
                                'x-api-token': apikey,
                                })
    data = r.json()
    return data

def realhub_sold_campaigns():
    offsets = list(range(0,200001, 50))
    for i in json_credentials:
        for z in tqdm(offsets, total=len(offsets), desc="Processing Sold Campaigns", leave=True):
            time.sleep(0.1)
            url = f'https://realhub.realbase.io/api/v2/campaigns.json?campaign_statuses[]=sold&include_campaign_ad_copies=true&include_campaign_agency=true&include_campaign_agents=true&limit=50&offset={z}'
            data = get_data(url, i['API Key'], i['Company'])
            if data:
                bulk_update_documents(data, i['Company'], i['SubCompany'], i['API Key'])
            else:
                break


# Define the DAG
default_args = {
    'owner': 'Lemuel Torrefiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'realhub_sold_campaigns',
    default_args=default_args,
    description='To extract Sold Campaigns from Realhub API for Daily reporting.',
    schedule_interval='30 22 * * *',
    start_date=datetime(2025, 2, 9),
    catchup=False,
)

# Define the task in the DAG
task = PythonOperator(
    task_id='realhub_sold_campaigns',
    python_callable=realhub_sold_campaigns,
    dag=dag,
)