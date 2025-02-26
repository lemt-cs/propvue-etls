def agentbox_enquiries():
    import requests
    from datetime import datetime, timedelta
    from tqdm import tqdm
    import os
    from pymongo import MongoClient, UpdateOne
    import requests
    from tqdm import tqdm
    from dotenv import load_dotenv
    from error_email_manual_trigger import error_send_email

    load_dotenv()
    mongodb_uri_local = os.getenv("MONGODB_URI_LOCAL")
    client = MongoClient(mongodb_uri_local)
    db = client['agentbox']
    coll = db['offices']
    source_script = os.path.join(os.getcwd(), os.path.basename(__file__))
    dateTo = datetime.now().strftime("%Y-%m-%d")
    dateFrom = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    api_keys_original = list(coll.find())
    coll = db['enquiries']
    errors_email_id = os.getenv("ENQUIRIES_ERRORS_EMAIL_ID")
    errors_subject = os.getenv("ENQUIRIES_SUBJECT")
    # api_keys = api_keys_original[67:]
    api_keys = api_keys_original.copy()
    with requests.Session() as session:
        for i in tqdm(api_keys, total=len(api_keys), ncols=50):
            url = f"https://api.agentboxcrm.com.au/enquiries?page=1&limit=20&filter[listingOfficeId]={i['id']}&filter[dateFrom]={dateFrom}&filter[dateTo]={dateTo}&include=contact%2Clisting&orderBy=enquiryDate&order=DESC&version=2"
            try:
                r = session.get(url, 
                                    headers={'Accept': 'application/json',
                                            'X-Client-ID': i['Client ID'],
                                            'X-API-Key': i['API Key']})    

                data = r.json()
                last_page = int(data['response']['last'])
                ranges = range(1, last_page + 1)
                operations = []
                for z in tqdm(ranges, total=last_page, ncols=50):
                    try:
                        url = f"https://api.agentboxcrm.com.au/enquiries?page={z}&limit=20&filter[listingOfficeId]={i['id']}&filter[dateFrom]={dateFrom}&filter[dateTo]={dateTo}&include=contact%2Clisting&orderBy=enquiryDate&order=DESC&version=2"
                        r = session.get(url, 
                                            headers={'Accept': 'application/json',
                                                    'X-Client-ID': i['Client ID'],
                                                    'X-API-Key': i['API Key']})    

                        data = r.json() 
                        for x in data['response']['enquiries']:
                            primary_fields = {
                                "date_synched": datetime.now(),
                                "source_script": source_script,
                                "my_identifier": f"{datetime.now().strftime("%m-%d-%Y")} Extract enquiries.",
                                "officeId": i['id'],
                                "officeName": i['name'],
                                "officeState": i['address']['state'],
                            }
                            primary_fields.update(x)
                            operation = UpdateOne({"id": primary_fields['id']}, {"$set": primary_fields}, upsert=True)
                            operations.append(operation)
                        result = coll.bulk_write(operations)
                        print(f"modified: {result.modified_count}")
                        print(f"upserted: {result.upserted_count}")
                    except KeyError as e:
                        error_send_email(msg=f"""Error in {source_script}
                        {str(e)}
                        Error in Instance: {i['Office']}
                        Error in Office: {i['id']} {i['name']}
                        Data: {r.content}
                        """,
                        subject=errors_subject, original_message_id=errors_email_id)
                # print(f"modified: {mod}")
                # print(f"upserted: {ups}")
            except KeyError as e:
                error_send_email(msg=f"""Error in {source_script}
                {str(e)}
                Error in Instance: {i['Office']}
                Error in Office: {i['id']} {i['name']}
                Data: {r.content}
                """,
                subject=errors_subject, original_message_id=errors_email_id)
            except requests.exceptions.JSONDecodeError as e:
                error_send_email(msg=f"""Error in {source_script}
                {str(e)}
                Error in Instance: {i['Office']}
                Error in Office: {i['id']} {i['name']}
                Data: {r.content}
                Error type: requests.exceptions.JSONDecodeError
                """,
                subject=errors_subject, original_message_id=errors_email_id)


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the DAG
default_args = {
    'owner': 'Lemuel Torrefiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'agentbox_enquiries',
    default_args=default_args,
    description='To extract enquiries from Agentbox API and store into local Mongodb.',
    schedule_interval=None,
    start_date=datetime(2025, 2, 3),
    catchup=False,
)

# Define the task in the DAG
task = PythonOperator(
    task_id='agentbox_enquiries',
    python_callable=agentbox_enquiries,
    dag=dag,
)
