import csv
from concurrent.futures import ThreadPoolExecutor

import requests
import json
from threading import Lock


def npi_detail_fetcher(configs):
    files_to_be_read = configs.get("npi_files").data.split(",")
    npi_ids = None
    with open(files_to_be_read[0], 'r') as file1:
        npi_ids = set(file1.readlines())
        print('npi ids in {} are {} ', files_to_be_read[0], len(npi_ids))

    with open(files_to_be_read[0], 'r') as file2:
        npi_list = file2.readlines()
        npi_ids.update(npi_list)
        print('npi ids in {} are {} ', files_to_be_read[1], len(npi_list))

    filtered_npi_list = list(npi_ids)
    chunk_size = configs.get('npi_chunk')
    thread_pool_size = configs.get('npi_threads')
    final_npi_list_size = len(filtered_npi_list)
    batch_size = chunk_size*thread_pool_size
    num_batches = len(filtered_npi_list)/batch_size + (1 if len(filtered_npi_list)%batch_size != 0 else 0)
    api_url = configs.get('npi_rege_url')

    doctors_file = configs.get('tokens_file')

    for i in range(num_batches):
        to_be_processed = filtered_npi_list[i*batch_size:min((i+1)*batch_size,final_npi_list_size)]
        try :
            with ThreadPoolExecutor(thread_pool_size) as pool:
                for i in thread_pool_size:
                    pool.map(fetch_details_npi_api, to_be_processed[i*chunk_size:min(len(to_be_processed),(i+1)*chunk_size)], api_url, doctors_file)

        except Exception as ex:
            print(to_be_processed)


def fetch_details_npi_api(npi_ids, api, doctor_file):
    lock = Lock()
    row_list = []
    for npi_id in npi_ids:
        parameters = {
            "number": npi_id,
            "version": 2.0
        }
        try:
            response = requests.get(api, parameters)
            if response.status_code == 200:
                response_json = json.load(response.json())
                details = response_json['results'][0]['basic']
                name = details['first_name'] + " " + details['middle_name'] + " " + details['last_name']
                row = [npi_id, name]
                row_list.append(row)
            else:
                print('error occurred during GET request for npi id {} ', npi_id)

        except Exception as ex:
            print("error occurred while fetching details for npi id : {} ", npi_id)

    lock.acquire()
    with open(doctor_file, 'a') as csvfile:

        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(row_list)
    lock.release()
