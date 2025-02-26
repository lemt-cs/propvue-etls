#order items

import pandas as pd
import os
import requests
from tqdm import tqdm
from pymongo import MongoClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.operators.python import PythonOperator
from airflow import DAG

load_dotenv()
mongo_uri = os.getenv("MONGODB_URI_LOCAL")
client = MongoClient(mongo_uri)
db = client['realhub']
coll = db['orders']
coll_api_keys = db['api_keys']
api_keys = list(coll_api_keys.find())

modified_since = '2024-01-01T11:00:00'

def update_document(data, office, subcomp, apikey):
    for x in data:
        if isinstance(x, dict):
            document = {
                "date_synched": datetime.now(),
                "Source Script": "wsl_airflow",
                "Company": office,
                "SubCompany": subcomp,
                "API Key": apikey
            }
            document.update(x)

            try:
                result = coll.update_one(
                        {"id": document["id"]},
                        {"$set": document},
                        upsert=True
                    )
            except Exception as e:
                print(e)

def get_data(url, apikey, company):
    try:
        r = requests.get(url, headers={'Accept': 'application/json',
                                    'User-Agent': company,
                                    'x-api-token': apikey,
                                    })
        data = r.json()
        # print(f"number of items = {len(data)}\ncompany = {company}")
        return data, r
    except Exception as e:
        print(r.status_code)
        print(r.content)
        print(e)

def realhub_order_items():
    for index, i in enumerate(api_keys[2:]):
        # if index == 0:
        #     continue
            # print(f"Now extracting for {i['SubCompany']}")
            # # put high maximum number as it will break if no data is available to extract
            # pages = range((1371+88+6+63+25+97)*50, 100001, 50)
            # for y in tqdm(pages, total=len(pages), desc="Extracting per offset", ncols=70):
            #     url = f'https://realhub.realbase.io/api/v2/orders.json?modified_since={modified_since}&include_order_items=true&limit=50&offset={str(y)}'
            #     data, r = get_data(url, i['API Key'], i['SubCompany'])
            #     update_document(data, i['Company'], i['SubCompany'], i['API Key'])
            #     if len(data) == 0:
            #         break
        # else:
            print(f"Now extracting for {i['SubCompany']}")
            # put high maximum number as it will break if no data is available to extract
            pages = range(0, 200001, 50)
            for y in tqdm(pages, total=len(pages), desc="Extracting per offset", ncols=70):
                url = f'https://realhub.realbase.io/api/v2/orders.json?modified_since={modified_since}&include_order_items=true&limit=50&offset={str(y)}'
                data, r = get_data(url, i['API Key'], i['SubCompany'])
                update_document(data, i['Company'], i['SubCompany'], i['API Key'])
                if len(data) == 0:
                    break

    print(f"Now having {coll.count_documents({})} documents in coll {coll.name}")


# Define the DAG
default_args = {
    'owner': 'Lemuel Torrefiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'realhub_order_items',
    default_args=default_args,
    description='To extract Order Items from Realhub API for Daily reporting.',
    schedule_interval='30 22 * * *',
    start_date=datetime(2025, 2, 9),
    catchup=False,
)

# Define the task in the DAG
task = PythonOperator(
    task_id='realhub_order_items',
    python_callable=realhub_order_items,
    dag=dag,
)