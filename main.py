import csv
import sys
import time
import requests
import random
import json
import threading

import config
from db import DBHelperClass


db = DBHelperClass()
# ---------------- CONFIG ----------------
CSV_FILE = "Temecula_Murrieta_needs_Melissa.csv"
BATCH_SIZE = 50        # max concurrent threads (safe for Melissa)
REQUEST_TIMEOUT = 10   # seconds
SLEEP_BETWEEN_BATCHES = 1  # seconds
# ------------------------------------

def read_csv():
    all_addresses = []
    with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)  # skip header
        for row in reader:
            all_addresses.append(row)

    print("Total rows:", len(all_addresses))
    return all_addresses


def get_melissa_data(address, original_data, index):
    try:
        url = (
            f"https://property.melissadata.net/v4/WEB/LookupProperty"
            f"?id={config.MELISSA_KEY}"
            f"&t={random.randint(1000000000, 9999999999)}"
            f"&format=JSON"
            f"&ff={address}"
            f"&cols=GrpAll"
        )

        response = requests.get(url, timeout=REQUEST_TIMEOUT)

        if response.status_code == 429:
            print(f"[429 RATE LIMITED] index={index} | {address}")
            return

        if response.status_code != 200:
            print(f"[HTTP ERROR] status={response.status_code} index={index} | {address}")
            return

        data = response.json()

        final_data = {
            "address": original_data[1],
            "city": original_data[2],
            "state": original_data[3],
            "zip": original_data[4],
            "melissa_data": None
        }

        if data.get("Records") and len(data.get("Records", [])) > 0:
            final_data["melissa_data"] = data["Records"][0]

        db.insert_one_record("melissa", final_data)

        # print(f"[OK] index={index} | {address}")

    except requests.exceptions.Timeout:
        print(f"[TIMEOUT] index={index} | {address}")

    except requests.exceptions.RequestException as e:
        print(f"[REQUEST ERROR] index={index} | {address} | {e}")

    except Exception as e:
        print(f"[UNEXPECTED ERROR] index={index} | {address} | {e}")


#limit 200 per minute
if __name__ == "__main__":
    all_addresses = read_csv()
    
    total = len(all_addresses)

    for i in range(1920, total, BATCH_SIZE):
        print(f"--- Processing batch starting at index {i} ---")
        threads = []
        batch = all_addresses[i:i + BATCH_SIZE]
        for idx, address in enumerate(batch, start=i):
            full_address = f"{address[1]}, {address[2]}, {address[3]} {address[4]}"
            t = threading.Thread(
                target=get_melissa_data,
                args=(full_address, address, idx)
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=REQUEST_TIMEOUT + 5)
        time.sleep(SLEEP_BETWEEN_BATCHES)

    print("\n✅ DONE — All records processed")