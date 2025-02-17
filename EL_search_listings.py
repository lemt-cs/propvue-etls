import requests
from pymongo import MongoClient, UpdateOne
import os
from tqdm import tqdm
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import math

load_dotenv()

api_key = os.getenv("API_KEY_LISTINGS")
mongodb_uri = os.getenv("MONGODB_URI")

client = MongoClient(mongodb_uri)
db = client['domain']
coll_smaps = db['smaps_mapped_unique']
coll_core = db['solds_search_using_smaps']
coll_error = db['error_smaps_search']

smaps = list(coll_smaps.find({}, {"_id":0}))

listedSince = "2024-01-01T00:00:00.000Z"
# updatedSince = "2024-01-01T00:00:00.000Z"
source_script = os.path.basename(__file__)

url = "https://api.domain.com.au/v1/listings/residential/_search"

headers = {
    "accept": "application/json",
    "Content-Type": "application/json",
    "X-Api-Key": api_key,
    "X-Api-Call-Source": "live-api-browser"
}

def perform_etl(officeId, office, smapsID, max_page):
    for w in range(1, max_page + 1):
        json_params = {
            "listingType": "Sale",
            "pageSize": 100,
            "pageNumber": w,
            "geoWindow": {
                "polygon": z['polygon']
            },
            "listedSince": listedSince,
            "sort": {"sortKey": "dateListed", "direction": "Descending"}
        }
        try:
            r = requests.post(url, headers=headers, json=json_params)
            data = r.json()
            operations = []
            for x in data:
                try:
                    if x['type'] == 'PropertyListing':
                        primary_fields = {
                            "officeId": officeId,
                            "office": office,
                            "smapsID": smapsID
                        }
                        primary_fields.update(x['listing'])
                        result = UpdateOne({"id": primary_fields['id']}, {"$set": primary_fields}, upsert=True)
                        operations.append(result)
                    elif x['type'] == 'Project':
                        for i in x['listings']:
                            primary_fields = {
                                "officeId": officeId,
                                "office": office,
                                "smapsID": smapsID
                            }
                            primary_fields.update(i)
                            result = UpdateOne({"id": primary_fields['id']}, {"$set": primary_fields}, upsert=True)
                            operations.append(result)
                except TypeError as e:
                    print(e)
                    print(data)
            if operations:
                print("now bulk writing")
                bulk_result = coll_core.bulk_write(operations)
                print(f"modified_count: {bulk_result.modified_count}")
                print(f"upserted_count: {bulk_result.upserted_count}")
        except requests.exceptions.JSONDecodeError as e:
            print(e)
            print(data)

smaps_new = smaps[1937:]
# smaps_new = smaps.copy()
for z in tqdm(smaps_new, total=len(smaps_new), desc="extracting per smap", ncols=100):
    json_params = {
        "listingType": "Sale",
        "pageSize": 100,
        "geoWindow": {
            "polygon": z['polygon']
        },
        # "polygon": {"points": [{"lat": 0,"lon": 0}]}
        "listedSince": listedSince,
        "sort": {"sortKey": "dateListed", "direction": "Descending"}
        }
    try:
        r = requests.post(url, headers=headers, json=json_params)
        r_headers = dict(r.headers)
        all_items = int(r_headers['X-Total-Count'])
        max_page = 10
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

            perform_etl(officeId=z['officeId'], office=z['office'], smapsID=z['smapsID'], max_page=max_page)
        else:
            perform_etl(officeId=z['officeId'], office=z['office'], smapsID=z['smapsID'], max_page=max_page)
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
    except NameError as e:
        print(e)
        print(r.content)
                    
print(f"see the collection {coll_error.name} for reprocessing of error smaps")