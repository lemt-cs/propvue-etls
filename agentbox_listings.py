import pandas as pd
import os
import requests
from pymongo import MongoClient
from tqdm import tqdm
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from error_email_manual_trigger import error_send_email

load_dotenv()
mongo_uri = os.getenv("MONGODB_URI_LOCAL")
client = MongoClient(mongo_uri)
db = client['agentbox']

firstdate = '2024-10-01'
lastdate = '2025-02-31'

def update_docs(data, office, apikey, clientid, coll):
    upserted = 0
    modified = 0
    try:
        for x in data['response']['listings']:
                if isinstance(x, dict):
                    document = {
                        "date_synched": datetime.now(),
                        "for_report": "Executive Network Report Dec. 2024",
                        "source_script": "wsl_dag",
                        "AB Instance": office,
                        "API Key": apikey,
                        "Client ID": clientid
                    }
                    document.update(x)
                    try:
                        result = coll.update_one(
                            {"id": document["id"]},
                            {"$set": document},
                            upsert=True
                        )
                        if result.upserted_id:
                            upserted += 1
                        elif result.modified_count:
                            modified += 1
                    except Exception as e:
                        print(e)
        return upserted, modified 
    except Exception as e:
        print(e)
        return 0, 0

def get_data(url, clientid, apikey):
    try:
        r = requests.get(url, 
                    headers={'Accept': 'application/json',
                            'X-Client-ID': clientid,
                            'X-API-Key': apikey})
        data = r.json()
        return data
    except Exception as e:
        print(f"Error in response: {e}")
        print(r.content)
        print(r.status_code)

def exec_report_listings_full():
    coll = db['listings']
    coll_api_keys = db['api_keys']
    offices_keys = list(coll_api_keys.find())

    print("starting by created dates")
    for i in offices_keys:
        print(f"Extracting for AB Instance: {i['Office']}")
        url = f'https://api.agentboxcrm.com.au/listings?page=1&limit=100&filter[incSurroundSuburbs]=false&filter[matchAllFeature]=false&include=relatedContacts%2CrelatedStaffMembers%2CinspectionDates&filter[createdBefore]={lastdate}&filter[createdAfter]={firstdate}&orderBy=lastModified&order=DESC&version=2'
        data = get_data(url, i['Client ID'], i['API Key'])
        try:
            listingsPages = int(data['response']['last'])

            rangeToQuery = range(2, listingsPages + 1)
            total = listingsPages
            modified = 0
            upserted = 0
            for y in tqdm(rangeToQuery, total=total, desc="Extracting Listings.", ncols=50):
                url = f'https://api.agentboxcrm.com.au/listings?page={y}&limit=100&filter[incSurroundSuburbs]=false&filter[matchAllFeature]=false&include=relatedContacts%2CrelatedStaffMembers%2CinspectionDates&filter[createdBefore]={lastdate}&filter[createdAfter]={firstdate}&orderBy=lastModified&order=DESC&version=2'
                data = get_data(url, i['Client ID'], i['API Key'])
                upserted_single, modified_single = update_docs(data, i['Office'], i['API Key'], i['Client ID'], coll)
                upserted += upserted_single
                modified += modified_single
        except KeyError as e:
            print(e)
            
    print(f"modified for created: {modified}")
    print(f"modified for upserted: {upserted}")

    print("starting by modified dates")
    for i in offices_keys:
        print(f"Extracting for AB Instance: {i['Office']}")
        url = f'https://api.agentboxcrm.com.au/listings?page=1&limit=100&filter[incSurroundSuburbs]=false&filter[matchAllFeature]=false&include=relatedContacts%2CrelatedStaffMembers%2CinspectionDates&filter[createdBefore]={lastdate}&filter[createdAfter]={firstdate}&orderBy=lastModified&order=DESC&version=2'
        data = get_data(url, i['Client ID'], i['API Key'])
        try:
            listingsPages = int(data['response']['last'])

            rangeToQuery = range(2, listingsPages + 1)
            total = listingsPages
            print(total)
            modified = 0
            upserted = 0
            for y in tqdm(rangeToQuery, total=total, desc="Extracting Listings.", ncols=50):
                url = f'https://api.agentboxcrm.com.au/listings?page={y}&limit=100&filter[incSurroundSuburbs]=false&filter[matchAllFeature]=false&include=relatedContacts%2CrelatedStaffMembers%2CinspectionDates&filter[modifiedBefore]={lastdate}&filter[modifiedAfter]={firstdate}&orderBy=lastModified&order=DESC&version=2'
                data = get_data(url, i['Client ID'], i['API Key'])
                upserted_single, modified_single = update_docs(data, i['Office'], i['API Key'], i['Client ID'], coll)
                upserted += upserted_single
                modified += modified_single
        except KeyError as e:
            print(e)
    print(f"modified for created: {modified}")
    print(f"modified for upserted: {upserted}")

    start_date = pd.to_datetime(firstdate)
    end_date = pd.to_datetime(lastdate)

    df = pd.json_normalize(coll.find(), sep="_")

    # Convert listedDate to datetime
    df['listedDate'] = pd.to_datetime(df['listedDate'], errors='coerce')

    df.drop(columns=['API Key', 'Client ID'], inplace=True)

    filtered_df = df[(df['listedDate'] >= start_date) & (df['listedDate'] < end_date)]

    print(f"number of listings for specified month: {len(filtered_df['id'].value_counts())}")

# Define the DAG
default_args = {
    'owner': 'Lemuel Torrefiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ab_listings_for_exec_report',
    default_args=default_args,
    description='To extract listings from Agentbox API for Executive Network Report.',
    schedule_interval='0 23 1 * *',
    start_date=datetime(2025, 2, 9),
    catchup=False,
)

# Define the task in the DAG
task = PythonOperator(
    task_id='ab_listings_for_exec_report',
    python_callable=exec_report_listings_full,
    dag=dag,
)