def agentbox_inspections():
    import requests
    from datetime import datetime, timedelta
    from tqdm import tqdm
    import os
    from pymongo import MongoClient
    import requests
    from tqdm import tqdm
    from dotenv import load_dotenv
    from error_email_manual_trigger import error_send_email

    source_script = os.path.join(os.getcwd(), os.path.basename(__file__))
    load_dotenv()
    mongodb_uri = os.getenv("MONGODB_URI_LOCAL")
    client = MongoClient(mongodb_uri)
    db = client['agentbox']
    coll = db['offices']
    api_keys = list(coll.find())
    startDateTo = datetime.today().strftime("%Y-%m-%d")
    startDateFrom = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    coll = db['inspections']
    errors_email_id = os.getenv("INSPECTIONS_ERRORS_EMAIL_ID")
    errors_subject = os.getenv("INSPECTIONS_SUBJECT")

    mod = 0
    ups = 0
    with requests.Session() as session:
        for i in tqdm(api_keys[75:], total=len(api_keys[75:]), ncols=70):
            url = f"https://api.agentboxcrm.com.au/inspections?page=1&limit=20&filter[listingOfficeId]={i['id']}&filter[startDateFrom]={startDateFrom}&filter[startDateTo]={startDateTo}&include=contact%2Clisting%2Cproject&orderBy=startDate&order=DESC&version=2"
            try:
                r = session.get(url, 
                                    headers={'Accept': 'application/json',
                                            'X-Client-ID': i['Client ID'],
                                            'X-API-Key': i['API Key']})    
                data = r.json() 
                last_page = int(data['response']['last'])
                ranges = range(1, last_page + 1)
                for z in tqdm(ranges, total=last_page, ncols=70):
                    try:
                        url = f"https://api.agentboxcrm.com.au/inspections?page={z}&limit=20&filter[listingOfficeId]={i['id']}&filter[startDateFrom]={startDateFrom}&filter[startDateTo]={startDateTo}&include=contact%2Clisting%2Cproject&orderBy=startDate&order=DESC&version=2"
                        r = session.get(url, 
                                            headers={'Accept': 'application/json',
                                                    'X-Client-ID': i['Client ID'],
                                                    'X-API-Key': i['API Key']})    

                        data = r.json() 
                        for x in data['response']['inspections']:
                            primary_fields = {
                                "date_synched": datetime.now(),
                                "source_script": source_script,
                                "my_identifier": "Extract inspections.",
                                "officeId": i['id'],
                                "officeName": i['name'],
                                "officeState": i['address']['state'],
                            }
                            primary_fields.update(x)
                            result = coll.update_one({"id": primary_fields['id']}, {"$set": primary_fields}, upsert=True)
                            if result.modified_count:
                                mod += 1
                            elif result.upserted_id:
                                ups += 1
                    except KeyError as e:
                        if r.status_code == 401:
                            error_send_email(msg=f"""Error in {source_script}
                            {str(e)}
                            Error in Instance: {i['Office']}
                            Error in Office: {i['id']} {i['name']}
                            Data: {r.content}
                            """,
                            subject=errors_subject, original_message_id=errors_email_id)                            
                print(f"modified: {mod}")
                print(f"upserted: {ups}")
            except KeyError as e:
                if r.status_code == 401:
                    error_send_email(msg=f"""Error in {source_script}
                                    {str(e)}
                                    Error in Instance: {i['Office']}
                                    Error in Office: {i['id']} {i['name']}
                                    Data: {r.content}
                                    """,
                                    subject=errors_subject, original_message_id=errors_email_id)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Define the DAG
default_args = {
    'owner': 'Lemuel Torrefiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'agentbox_inspections',
    default_args=default_args,
    description='To extract inspections from Agentbox API and store into local Mongodb.',
    schedule_interval=None,
    start_date=datetime(2025, 2, 3),
    catchup=False,
)

# Define the task in the DAG
task = PythonOperator(
    task_id='agentbox_inspections',
    python_callable=agentbox_inspections,
    dag=dag,
)

