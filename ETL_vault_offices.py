def vault_offices():
    import pandas as pd
    import os
    import requests
    from pymongo import MongoClient
    from dotenv import load_dotenv
    from datetime import datetime
    from office365.sharepoint.client_context import ClientContext
    from L_insert_df_to_postgresql import insert_to_postgresql
    
    load_dotenv(dotenv_path=r"/home/lemuel-torrefiel/airflow/dags/.env")
    sharepoint_site_url = os.getenv("SHAREPOINT_SITE_URL")
    username = os.getenv("SHAREPOINT_EMAIL")
    password = os.getenv("SHAREPOINT_PASSWORD")
    mongodb_uri_local = os.getenv("MONGODB_URI_LOCAL")
    client = MongoClient(mongodb_uri_local)
    db = client['staging_vault']
    collection = db['accounts']
    coll_api_keys = db['api_keys']
    api_keys = list(coll_api_keys.find())
    total_offices = 0
    
    def get_offices(clientid, apikey):
        url = f'https://ap-southeast-2.api.vaultre.com.au/api/v1.3/account'
        try:
            r = requests.get(url, 
                        headers={'Accept': 'application/json',
                                'X-API-Key': apikey,
                                'Authorization': clientid
                                })
            data = r.json()
            return data
        except Exception as e:
            print()

    def update_doc(data, office, clientid, apikey):
        if isinstance(data, dict):
            document = {
                "date_synched": datetime.now(),
                "Office": office,
                "Client ID": clientid,
                "API Key": apikey
            }
            document.update(data)
            try:
                result = collection.update_one({"id": document['id']}, {"$set": document}, upsert=True)
            except Exception as e:
                print(e)
        else:
            print(e)
            
    for i in api_keys:
        data = get_offices(i['Client ID'], i['API Key'])
        print(data)
        listings = len(data)
        print(listings)
        print(i['Office'])
        total_offices = total_offices + listings

        update_doc(data, i['Office'], i['Client ID'], i['API Key'])

    print(f'final total_offices: {total_offices}')

    # turn to df
    list_of_accounts = list(collection.find({}, {"_id": 0}))
    df = pd.json_normalize(list_of_accounts, sep="_")
    local_file_path = os.path.join(r"/home/lemuel-torrefiel/airflow/csvs", "vault_offices.csv")
    df.to_csv(local_file_path, index=False, encoding='utf-8')
    insert_to_postgresql(schema='vault', table='accounts', df=df, replace_or_append='replace')

    # upload to sharepoint
    print(f'now uploading to sharepoint vault_offices folder')

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


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

# Define the DAG
default_args = {
    'owner': 'Lemuel Torrefiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'vault_offices',
    default_args=default_args,
    description='To extract offices from vault API and store into Mongodb.',
    schedule_interval='10 22 * * *',
    start_date=datetime(2025, 2, 3),
    catchup=False,
)

# Define the task in the DAG
# task = PythonOperator(
#     task_id='vault_offices',
#     python_callable=vault_offices,
#     dag=dag,
#     python_interpreter="/home/lemuel-torrefiel/airflow/venv/bin/python"
# )

# Read the requirements.txt file
with open("/home/lemuel-torrefiel/airflow/requirements.txt") as f:
    requirements = [line.strip() for line in f if line.strip()]

task = PythonVirtualenvOperator(
    task_id="vault_offices",
    python_callable=vault_offices,
    requirements=requirements,
    system_site_packages=False,
    dag=dag
)
