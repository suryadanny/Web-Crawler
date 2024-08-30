import csv
import time
from concurrent.futures import ThreadPoolExecutor

import requests
import json
from threading import Lock


def npi_detail_fetcher(configs):
    files_to_be_read = configs.get("npi_files").data.split(",")
    npi_ids = set()
    for file_name in files_to_be_read:
        with open(file_name, 'r') as file:
            npi_list = file.readlines()[1:]
            npi_ids.update(npi_list)
            print('npi ids in {} are {} '.format(file_name, len(npi_ids)))

    filtered_npi_list = list(npi_ids)
    print('final npi list size {} '.format(len(filtered_npi_list)))
    chunk_size = int(configs.get('npi_chunk').data)
    thread_pool_size = int(configs.get('npi_threads').data)
    final_npi_list_size = len(filtered_npi_list)
    batch_size = chunk_size * thread_pool_size
    num_batches = int(len(filtered_npi_list) / batch_size) + (1 if len(filtered_npi_list) % batch_size != 0 else 0)
    print(num_batches)
    api_url = configs.get('npi_reg_api').data

    doctors_file = configs.get('tokens_file').data
    prep_doctor_list(doctors_file)
    for i in range(num_batches):
        print('Batch Run- {}'.format(i))
        to_be_processed = filtered_npi_list[i * batch_size:min((i + 1) * batch_size, final_npi_list_size)]
        row_list = []
        failed_ids = []
        try:
            with ThreadPoolExecutor(thread_pool_size) as pool:
                tasks = []
                for thread in range(thread_pool_size):

                    tasks.append((to_be_processed[
                                  thread * chunk_size:min(len(to_be_processed), (thread + 1) * chunk_size)], api_url,
                                  doctors_file))

                    if (thread+1)*chunk_size >= len(to_be_processed):
                        print('task ended')
                        break

                # print(tasks)
                for future_result in pool.map(fetch_details_npi_api, tasks):
                    row_list.extend(future_result[0])
                    failed_ids.extend(future_result[1])
                    #print(future_result)

                with open(doctors_file, 'a', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile,
                                            quotechar='"', quoting=csv.QUOTE_ALL)
                    csv_writer.writerows(row_list)
                print(failed_ids)

                with open('failed.csv', 'a', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile,
                                            quotechar='"', quoting=csv.QUOTE_ALL)
                    csv_writer.writerows(failed_ids)
                print('Batch exec {} finished'.format(i))

            time.sleep(0.25)

        except Exception as ex:
            print(str(ex))
            print('error occurred')


def fetch_details_npi_api(args):
    npi_ids, api, doctor_file = args
    failed_npi_ids = []

    row_list = []
    for npi_id in npi_ids:
        if len(npi_id.strip()) < 10 or not npi_id.strip().isnumeric():
            failed_npi_ids.append([npi_id.strip(),'npi id less than 10 digits or not numeric'])
            continue
        parameters = {
            "number": int(npi_id),
            "version": 2.1
        }

        try:
            response = requests.get(api, parameters)
            if response.status_code == 200:
                response_json = response.json()

                if response_json['result_count'] == 0:
                    failed_npi_ids.append([npi_id.strip(), 'No result found in registry'])
                    continue

                if response_json['result_count'] > 1:
                    failed_npi_ids.append([npi_id.strip(), 'result set greater than one'])

                details = response_json['results'][0]['basic']

                if 'first_name' not in details and 'organization_name' in details:
                    failed_npi_ids.append([npi_id.strip(),'npi belongs to organisation'])
                    continue

                if 'name_prefix' not in details:
                    reason = response_json['results'][0]['taxonomies'][0]['desc'] if 'taxonomies' in response_json['results'][0] else 'could not identify'
                    failed_npi_ids.append([npi_id.strip(),reason])
                    continue

                name = details['first_name']
                if 'middle_name' in details:
                    name += " " + details['middle_name']

                if 'last_name' in details:
                    name += " " + details['last_name']

                row = [npi_id.strip(), name.strip()]
                row_list.append(row)
            else:
                print('error occurred during GET request for npi id {} '.format(npi_id))
            time.sleep(0.75)

        except Exception as ex:
            print(str(ex))
            print("error occurred while fetching details for npi id : {} ".format(npi_id))
    print('failed npi ids  - {}'.format(failed_npi_ids))
    return row_list, failed_npi_ids
    # lock.acquire()
    # print('lock acquired')
    # with open(doctor_file, 'a') as csvfile:
    #
    #     csv_writer = csv.writer(csvfile,
    #                             quotechar='"', quoting=csv.QUOTE_ALL)
    #     csv_writer.writerow(row_list)
    # print('releasing lock')
    # lock.release()


def prep_doctor_list(file_name):

    with open(file_name, 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(['npi_id','doctor_name'])

    with open('failed.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(['npi_id','reason'])
