import requests
from pymongo import MongoClient
import os
from tqdm import tqdm
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import math
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from error_email_manual_trigger import error_send_email

load_dotenv()
source_script = os.path.join(os.getcwd(), os.path.basename(__file__))
api_key = os.getenv("API_KEY")
mongodb_uri_local = os.getenv("MONGODB_URI_LOCAL") 
client = MongoClient(mongodb_uri_local)
db = client['domain']
coll_smaps = db['smaps_mapped_unique']
coll_core = db['solds_search_using_smaps']
coll_error = db['error_smaps_search']
listedSince = "2023-01-01T00:00:00.000Z"
headers = {
    "accept": "application/json",
    "Content-Type": "application/json",
    "X-Api-Key": api_key,
    "X-Api-Call-Source": "live-api-browser"
    }
errors_email_id = os.getenv("DOMAIN_SOLDS_ERRORS_EMAIL_ID")
errors_subject = os.getenv("DOMAIN_SOLDS_SUBJECT")

def perform_etl(officeId, office, smapsID, max_page, url, polygon):
    for w in range(1, max_page + 1):
        try:
            json_params = {
                "listingType": "SOLD",
                "pageSize": 100,
                "pageNumber": w,
                "geoWindow": {
                    "polygon": polygon
                },
                "listedSince": listedSince,
                "sort": {"sortKey": "soldDate", "direction": "Descending"}
            }
            r = requests.post(url, headers=headers, json=json_params)
            data = r.json()
            for x in data:
                try:
                    if x['type'] == 'PropertyListing':
                        primary_fields = {
                            "officeId": officeId,
                            "office": office,
                            "smapsID": smapsID
                        }
                        primary_fields.update(x['listing'])
                        coll_core.update_one({"id": primary_fields['id']}, {"$set": primary_fields}, upsert=True)
                    elif x['type'] == 'Project':
                        for i in x['listings']:
                            primary_fields = {
                                "officeId": officeId,
                                "office": office,
                                "smapsID": smapsID
                            }
                            primary_fields.update(i)
                            result = coll_core.update_many({"id": primary_fields['id']}, {"$set": primary_fields}, upsert=True)
                except TypeError as e:
                    error_send_email(msg=f"""Error in {source_script}
                    TypeError: {str(e)}
                    Error in page: {w} of {office}
                    Error in index: {index + 1}
                    """,
                    subject=errors_subject, original_message_id=errors_email_id)
        except Exception as e:
            error_send_email(msg=f"""Error in {source_script}
                Exception: {str(type(e))} {str(e)}
                Error in page: {w} of {office}
                Error in index: {index + 1}
                """,
                subject=errors_subject, original_message_id=errors_email_id)

                
def domain_solds_search():
    source_script = os.path.join(os.getcwd(), os.path.basename(__file__))
    smaps = list(coll_smaps.find({}, {"_id":0}))

    # updatedSince = "2024-01-01T00:00:00.000Z"
    source_script = os.path.basename(__file__)

    url = "https://api.domain.com.au/v1/listings/residential/_search"

    smaps_new = smaps[1600+995:]
    # smaps_new = smaps.copy()
    for index, z in tqdm(enumerate(smaps_new), total=len(smaps_new), desc="extracting per smap", ncols=100):
        try:
            data = {
                "listingType": "SOLD",
                "pageSize": 100,
                "geoWindow": {
                    "polygon": z['polygon']
                },
                # "polygon": {"points": [{"lat": 0,"lon": 0}]}
                "listedSince": listedSince,
                "sort": {"sortKey": "soldDate", "direction": "Descending"}
                }
            r = requests.post(url, headers=headers, json=data)

            # get total number of pages first from the response headers
            r_headers = dict(r.headers)
            try:
                all_items = int(r_headers['X-Total-Count'])
                max_page = 10
                # extract listings per page
                
                if all_items > 1000:
                    total_pages = math.ceil(all_items / 100)
                    new_dict = {
                        "date_inserted": datetime.now(),
                        "source_script": source_script,
                        "smapsID": z['smapsID'],
                        "listedSince": listedSince,
                        "total_items": all_items,
                        "total_pages": total_pages
                    }
                    coll_error.update_one({
                        "smapsID": z['smapsID']
                    }, {"$set": new_dict}, upsert=True)

                    perform_etl(officeId=z['officeId'], 
                                office=z['office'], 
                                smapsID=z['smapsID'], 
                                max_page=max_page, 
                                url=url, 
                                polygon=z['polygon'])
                else:
                    perform_etl(officeId=z['officeId'], 
                                office=z['office'], 
                                smapsID=z['smapsID'], 
                                max_page=10, 
                                url=url, 
                                polygon=z['polygon'])
            except KeyError as e:
                new_dict = {
                        "date_inserted": datetime.now(),
                        "source_script": source_script,
                        "smapsID": z['smapsID'],
                        "listedSince": listedSince,
                        "total_items": all_items,
                        "total_pages": total_pages,
                        "error": f"KeyError: {str(e)}"
                    }
                coll_error.update_one({
                        "smapsID": z['smapsID']
                    }, {"$set": new_dict}, upsert=True)
                error_send_email(msg=f"""Error in {source_script}
                    KeyError: {str(e)}
                    Error in smapsID: {z['smapsID']} {z['SM Group 1']}
                    """,
                    subject=errors_subject, original_message_id=errors_email_id)
                            
            print(f"see the collection {coll_error.name} for reprocessing of error smaps")
        except Exception as e:
            error_send_email(msg=f"""Error in {source_script}
                Exception: {str(type(e))} {str(e)}
                Error in smapsID: {z['smapsID']} {z['SM Group 1']}
                """,
                subject=errors_subject, original_message_id=errors_email_id)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'domain_solds_search',
    default_args=default_args,
    description='To extract data from Domain API and store into Mongodb.',
    schedule_interval=None,
    start_date=datetime(2025, 2, 3),
    catchup=False,
)

# Define the task in the DAG
task = PythonOperator(
    task_id='domain_solds_search',
    python_callable=domain_solds_search,
    dag=dag,
)