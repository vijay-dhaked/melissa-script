import csv
import requests
import random
import json
import threading

import config
from db import DBHelperClass


db = DBHelperClass()

def read_csv():
    all_addresses = []
    with open('Pull_Melissa_Corona.csv', mode='r', newline='') as file:
        reader = csv.reader(file)
        for row in reader:
            if reader.line_num == 1:
                continue
            all_addresses.append(row)
            
        print("total rows", len(list(reader)))
        return all_addresses

def get_melissa_data(address, original_data, thread_id, start_index):
    url = f"https://property.melissadata.net/v4/WEB/LookupProperty?id={config.MELISSA_KEY}&t={random.randint(1000000000, 9999999999)}&format=JSON&ff={address}&cols=GrpAll"

    payload = {}
    headers = {}
    print("getting data", thread_id, start_index, url)
    response = requests.request("GET", url, headers=headers, data=payload)
    print("got data", thread_id, start_index)

    if response.status_code == 200:
        data= json.loads(response.text)
        final_data = {
            "address": original_data[1],
            "city": original_data[2],
            "state": original_data[3],
            "zip": original_data[4]
        }
        if data.get("Records", []) and len(data.get("Records", [])) > 0:
            final_data["melissa_data"] = data["Records"][0]
            
        db.insert_one_record("melissa", final_data)



if __name__ == "__main__":
    all_addresses = read_csv()
    # use threading , use 50 threads
    for i in range(3728, len(all_addresses), 10):
        threads = []
        j = 0
        for address in all_addresses[i:i+10]:
            j += 1
            t = threading.Thread(target=get_melissa_data, args=(f"{address[1]}, {address[2]}, {address[3]} {address[4]}", address, j, i))
            threads.append(t)
            t.start()
            # get_melissa_data(f"{address[1]}, {address[2]}, {address[3]} {address[4]}", address, j, i)
            
        for t in threads:
            t.join()
            