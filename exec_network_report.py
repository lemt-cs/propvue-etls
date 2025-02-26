from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from pymongo import MongoClient
import time
from email_table import email_df
from dotenv import load_dotenv
import os
from tqdm import tqdm

firstdate = '2025-02-01'
lastdate = '2025-02-28'
load_dotenv()
mongo_uri = os.getenv("MONGODB_URI_LOCAL")
client = MongoClient(mongo_uri)

def etl_listings():
    db = client['agentbox']
    coll = db['exec_network_report']
    coll_creds = db['offices']
    office_creds = list(coll_creds.find())

    print("starting by created dates")
    for i in tqdm(office_creds, total=len(office_creds)):
        time.sleep(0.1)
        url = f'https://api.agentboxcrm.com.au/reports/performance?filter[saleStatus]=UnconditionalByUnconditionalDate&filter[dateFrom]=2025-01-01&filter[dateTo]=2025-01-31&filter[memberOfficeId]={i['id']}&version=2'
        try:
            r = requests.get(url, 
                            headers={'Accept': 'application/json',
                                    'X-Client-ID': i['Client ID'],
                                    'X-API-Key': i['API Key']})    
            r.raise_for_status()
        except requests.RequestException as e:
            print(f'Error in request: {e}')

        try:
            data = r.json()
        except (KeyError, json.JSONDecodeError) as e:
            print(f"Error in response: {e}")

        for x in data['response']['report']['metrics']:
            if x['ref'] == "Listings":
                primary_fields = {
                    "date_synched": datetime.now(),
                    "email_identifier": datetime.today().strftime("%Y-%m-%d") + "_extract",
                    "dateFrom": firstdate,
                    "dateTo": lastdate,
                    "office_id": i['id'],
                    "office_name": i['name'],
                    "office_state": i['address']['state']
                }
                primary_fields.update({"listings": x})
                coll.insert_one(primary_fields)

    df_to_be_sent = pd.json_normalize(coll.find({"email_identifier": datetime.today().strftime("%Y-%m-%d") + "_extract"}), sep="_")
    df_to_be_sent['listings_value'] = pd.to_numeric(df_to_be_sent['listings_value'])
    pivot = pd.pivot_table(df_to_be_sent, index=['office_id', 'office_name', 'office_state'], values='listings_value', aggfunc='sum')
    pivot_to_send = pivot.reset_index()
    try:
        email_df(df=pivot_to_send, subject=f"Listings by Offices for {pd.to_datetime(firstdate).strftime("%b. %Y")}")
        pivot.to_csv("exec_network_report/listings_per_office.csv")
        print(pivot)
    except Exception as e:
        print(e)
    pivot = pd.pivot_table(df_to_be_sent, index=['office_state'], values='listings_value', aggfunc='sum')
    pivot_to_send = pivot.reset_index()
    try:
        email_df(df=pivot_to_send, subject=f"Listings by States for {pd.to_datetime(firstdate).strftime("%b. %Y")}")
        pivot.to_csv("exec_network_report/listings_per_state.csv")
        print(pivot)
    except Exception as e:
        print(e)

# Define the Single DAG
default_args = {
    'owner': 'Lemuel Torrefiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'exec_network_report',
    default_args=default_args,
    description='To extract Executive Network Report data from Agentbox and Vault APIs and store into Mongodb or send as email.',
    schedule_interval=None,
    start_date=datetime(2025, 2, 3),
    catchup=False,
)

# Define the task in the DAG
task = PythonOperator(
    task_id='exec_network_report',
    python_callable=etl_listings,
    dag=dag,
)

