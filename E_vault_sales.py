import pandas as pd
import os
import requests
from tqdm import tqdm
from pymongo import MongoClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from office365.sharepoint.client_context import ClientContext
from set_dates import set_dates

load_dotenv()
mongodb_uri_local = os.getenv("MONGODB_URI_LOCAL")
client = MongoClient(mongodb_uri_local)
db = client['staging_vault']
coll = db['api_keys']
api_keys = list(coll.find())
coll = db['listings']
coll_count = db['sales_count']
source_script = os.path.join(os.getcwd(), os.path.basename(__file__)) 
# change this per month
# -----
dateone, datetwo = set_dates()
# -----
perpage = 100

def extract_vault_settled_sales():
    my_identifier = f"Residential Settled Solds {datetime.today().strftime("%m-%d-%Y")}"
    mod = 0
    ups = 0
    ups_count = 0
    mod_count = 0
    print(f'ensure that the date range is accurate --> {dateone} - {datetwo}')
    for x in tqdm(api_keys, total=len(api_keys), ncols=70):
        url1 = f'https://ap-southeast-2.api.vaultre.com.au/api/v1.3/properties/residential/sale/sold?settlementSince={dateone}'
        url2 = f'&settlementBefore={datetwo}&page=1&pagesize=100&sort=salePrice&sortOrder=desc'
        url_to_query = url1 + url2        
        try:
            r = requests.get(url_to_query, 
                            headers={'Accept': 'application/json',
                                    'X-API-Key': x['API Key'],
                                    'Authorization': x['Client ID']
                                    })
            data = r.json()
            total_items = int(data["totalItems"])
            total_pages = int(data["totalPages"])
            for page in range(1, total_pages + 1):
                url1 = f'https://ap-southeast-2.api.vaultre.com.au/api/v1.3/properties/residential/sale/sold?settlementSince={dateone}'
                url2 = f'&settlementBefore={datetwo}&page={page}&pagesize=100&sort=salePrice&sortOrder=desc'
                url_to_query = url1 + url2        
                r = requests.get(url_to_query, 
                                headers={'Accept': 'application/json',
                                        'X-API-Key': x['API Key'],
                                        'Authorization': x['Client ID']
                                        })
                r.status_code
                data = r.json()
                records = data['items']
                records_in_batch = len(records)
                for y in tqdm(records, total=records_in_batch, ncols=70):
                    primary_fields = {
                        "date_synched": datetime.now(),
                        "source_script": source_script,
                        "settled_include": True,
                        "api_key_office": x['Office'],
                        "my_identifier": my_identifier 
                    }
                    primary_fields.update(y)
                    result = coll.update_one({"id": primary_fields['id'], "my_identifier": my_identifier},
                                            {"$set": primary_fields}, upsert=True)
                    if result.upserted_id:
                        ups += 1
                    elif result.modified_count:
                        mod += 1
                    count_fields = {
                        "date_synched": datetime.now(),
                        "source_script": source_script,
                        "count_type": "settled",
                        "total_items": total_items,
                        "month": dateone,
                        "api_key_office": x['Office'],
                        "my_identifier": my_identifier 
                    }
                    result_count = coll_count.update_one({"count_type": "settled", "api_key_office": x['Office'], "my_identifier": my_identifier},
                                    {"$set": count_fields}, upsert=True)
                    if result_count.upserted_id:
                        ups_count += 1
                    elif result_count.modified_count:
                        mod_count += 1
        except KeyError as e:
            print(f"KeyError {e}")
            print(data)
    print(f"modified: {mod}")
    print(f"upserted: {ups}")
    print(f"modified count: {mod_count}")
    print(f"upserted count: {ups_count}")

def extract_vault_unconditional():
    my_identifier = f"Residential Unconditional Solds {datetime.today().strftime("%m-%d-%Y")}"
    mod = 0
    ups = 0
    ups_count = 0
    mod_count = 0
    print(f'ensure that the date range is accurate --> {dateone} - {datetwo}')
    for x in tqdm(api_keys, total=len(api_keys), ncols=70):
        url1 = f'https://ap-southeast-2.api.vaultre.com.au/api/v1.3/properties/residential/sale/sold?unconditionalSince={dateone}'
        url2 = f'&unconditionalBefore={datetwo}&page=1&pagesize=100&sort=salePrice&sortOrder=desc'
        url_to_query = url1 + url2
        try:
            r = requests.get(url_to_query, 
                            headers={'Accept': 'application/json',
                                    'X-API-Key': x['API Key'],
                                    'Authorization': x['Client ID']
                                    })
            data = r.json()
            total_items = int(data["totalItems"])
            total_pages = int(data["totalPages"])
            for page in range(1, total_pages + 1):
                url1 = f'https://ap-southeast-2.api.vaultre.com.au/api/v1.3/properties/residential/sale/sold?unconditionalSince={dateone}'
                url2 = f'&unconditionalBefore={datetwo}&page={page}&pagesize=100&sort=salePrice&sortOrder=desc'
                url_to_query = url1 + url2
                r = requests.get(url_to_query, 
                                headers={'Accept': 'application/json',
                                        'X-API-Key': x['API Key'],
                                        'Authorization': x['Client ID']
                                        })
                r.status_code
                data = r.json()
                records = data['items']
                records_in_batch = len(records)
                for y in tqdm(records, total=records_in_batch, ncols=70):
                    primary_fields = {
                        "date_synched": datetime.now(),
                        "source_script": source_script,
                        "unconditional_include": True,
                        "api_key_office": x['Office'],
                        "my_identifier": my_identifier 
                    }
                    primary_fields.update(y)
                    result = coll.update_one({"id": primary_fields['id'], "my_identifier": my_identifier},
                                            {"$set": primary_fields}, upsert=True)
                    if result.upserted_id:
                        ups += 1
                    elif result.modified_count:
                        mod += 1
                    count_fields = {
                        "date_synched": datetime.now(),
                        "source_script": source_script,
                        "count_type": "unconditional",
                        "total_items": total_items,
                        "month": dateone,
                        "api_key_office": x['Office'],
                        "my_identifier": my_identifier 
                    }
                    result_count = coll_count.update_one({"count_type": "unconditional", "api_key_office": x['Office'], "my_identifier": my_identifier},
                                                        {"$set": count_fields}, upsert=True)
                    if result_count.upserted_id:
                        ups_count += 1
                    elif result_count.modified_count:
                        mod_count += 1
        except KeyError as e:
            print(f"KeyError {e}")
            print(data)
    print(f"modified: {mod}")
    print(f"upserted: {ups}")
    print(f"modified count: {mod_count}")
    print(f"upserted count: {ups_count}")        

   

# Define the DAG
default_args = {
    'owner': 'Lemuel Torrefiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'extract_vault_settled_sales',
    default_args=default_args,
    description='To extract Settled Sales from Vault API and store into Mongodb.',
    schedule_interval='15 22 * * *',
    start_date=datetime(2025, 2, 3),
    catchup=False,
)

dag2 = DAG(
    'extract_vault_unconditional',
    default_args=default_args,
    description='To extract Unconditional Sales from Vault API and store into Mongodb.',
    schedule_interval=None,
    start_date=datetime(2025, 2, 3),
    catchup=False,
)

# Define the task in the DAG
task = PythonOperator(
    task_id='extract_vault_settled_sales',
    python_callable=extract_vault_settled_sales,
    dag=dag,
)

task = PythonOperator(
    task_id='extract_vault_unconditional',
    python_callable=extract_vault_unconditional,
    dag=dag2,
)

trigger_second_dag = TriggerDagRunOperator(
    task_id="trigger_extract_vault_unconditional",
    trigger_dag_id="extract_vault_unconditional",
    wait_for_completion=True,
    dag=dag
)
