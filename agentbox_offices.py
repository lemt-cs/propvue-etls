import pandas as pd
import os
import requests
from pymongo import MongoClient
from tqdm import tqdm
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from error_email_manual_trigger import error_send_email

def get_offices(clientid, apikey, page):
        url = f'https://api.agentboxcrm.com.au/offices?page={page}&limit=100&filter[status]=Active&order=DESC&version=2'
        r = requests.get(url, 
                headers={'Accept': 'application/json',
                'X-Client-ID': clientid,
                'X-API-Key': apikey})
        data = r.json()
        return data

def update_doc(data, office, clientid, apikey, collection):
    for x in data['response']['offices']:
        if isinstance(x, dict):
                document = {
                "Office": office,
                "Client ID": clientid,
                "API Key": apikey,
                "date_inserted": datetime.now(),
                "my_identifier": f"{datetime.now().strftime("%m-%d-%Y")} Daily Agentbox Offices ETL"
                }
                document.update(x)
                try:
                        result = collection.replace_one({"id": document['id']}, document, upsert=True)
                        if result.modified_count:
                            modified_count += 1
                        elif result.upserted_id:
                            upserted_count += 1
                            
                except Exception as e:
                        with open(r'c:\\Users\\Public\\OneDrive - BELLE PROPERTY\\Analyst Files\\Prod_Python_v2\\mongodb\\errorlogs.txt', 'a') as file:
                                file.write(f"\n{str(e)}\n")
                                modified_count = 0
                                upserted_count = 0
    return modified_count, upserted_count

# Define a simple function to be run by the task
def agentbox_offices():
    source_script = os.path.join(os.getcwd(), os.path.basename(__file__))
    try:
        load_dotenv()

        mongodb_uri_local = os.getenv("MONGODB_URI_LOCAL")
        client = MongoClient(mongodb_uri_local)
        db = client['agentbox']
        collection = db['offices']

        coll = db['api_keys']
        json_credentials = list(coll.find())

        modified_count = 0
        upserted_count = 0
        for i in tqdm(json_credentials, total=len(json_credentials), desc="Extractiong Offices", leave=True):
                try:
                        data = get_offices(i['Client ID'], i['API Key'], "1")
                        last_page = int(data['response']['last'])
                        for x in range(1, last_page + 1):
                            data = get_offices(i['Client ID'], i['API Key'], x)
                            modified_count_single, upserted_count_single = update_doc(data, i['Office'], i['Client ID'], i['API Key'], collection=collection)
                            modified_count += modified_count_single
                            upserted_count += upserted_count_single
                        print(f"upserted: {upserted_count}")
                        print(f"modified: {modified_count}")
                except Exception as e:
                        print(e)

        list_of_items = list(collection.find())
        df = pd.json_normalize(list_of_items, sep="_")
        df_new = df[['Office', 'id', 'name', 'status', 'address_state', 'address_suburb']]
        df_new['AB_Instance'] = df_new['Office']
        df_new['officeId'] = df_new['id']
        df_new['officeName'] = df_new['name']
        df_new['state'] = df_new['address_state']
        df_new['suburb'] = df_new['address_suburb']
        df_new['page'] = None
        df_new['API_Key'] = None
        df_new['Client_ID'] = None

        df_new = df_new[['AB_Instance',
                        'officeId',
                        'officeName',
                        'status',
                        'state',
                        'suburb',
                        'page',
                        'Office',
                        'API_Key',
                        'Client_ID']]

        local_file_path = (r'/home/lemuel-torrefiel/airflow/csvs/offices.csv').replace("\\", "/")
        df_new.to_csv(local_file_path, index=False)
        # start of sharepoint
        print(f'now uploading to sharepoint offices folder')
        from office365.sharepoint.client_context import ClientContext

        # SharePoint site details
        sharepoint_site_url = "https://belleaust.sharepoint.com/sites/belleproperty_bi"
        username = "lemuel.torrefiel@belleproperty.com"
        password = os.getenv("SHAREPOINT_PASSWORD")

        # SharePoint folder and local file details
        document_library_name = "Documents"
        folder_name = "Prod_CSVs"
        sub_folder = "offices"

        # Initialize the ClientContext
        ctx = ClientContext(sharepoint_site_url).with_user_credentials(username, password)

        # Get the folder
        folder = ctx.web.lists.get_by_title(document_library_name).root_folder.folders.get_by_url(folder_name).folders.get_by_url(sub_folder)

        # Open the local file in binary mode and upload it to SharePoint
        with open(local_file_path, "rb") as file_content:
                try:
                        target_file_name = local_file_path.split(r"/")[-1]
                        folder.upload_file(target_file_name, file_content).execute_query()
                except Exception as e:
                        target_file_name = local_file_path.split("\\")[-1]
                        folder.upload_file(target_file_name, file_content).execute_query()


        print(f"File '{target_file_name}' has been uploaded to SharePoint.")
    except Exception as e:
            error_send_email(msg=f"""Error in {source_script}
                            {str(e)}
                            """,
                            subject=f"Error in {source_script}")
            raise Exception("Details about the error sent!")




# Define the DAG
default_args = {
    'owner': 'Lemuel Torrefiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'agentbox_offices',
    default_args=default_args,
    description='To extract offices from Agentbox API and store into Mongodb.',
    schedule_interval=None,
    start_date=datetime(2025, 2, 3),
    catchup=False,
)

# Define the task in the DAG
task = PythonOperator(
    task_id='agentbox_offices',
    python_callable=agentbox_offices,
    dag=dag,
)
