# from webParser import connect_site
import csv
from concurrent.futures import ThreadPoolExecutor

from webmdCrawler import webmd_crawler
from vitalCrawler import vital_crawler
from threading import Thread
from jproperties import Properties
from utils import prepare_file_headers
from npiCrawler import npi_detail_fetcher


def start_crawling():
    configs = get_properties()
    exec_order = set(configs.get('exec_order').data.split(','))

    if 'fetch' in exec_order:
        npi_detail_fetcher(configs)

    doctors_list = read_file(configs)
    print('doctor list : {}'.format(doctors_list))
    prepare_file_headers()
    if 'vital' in exec_order:
        parallel_exec('vital', configs, doctors_list[1:])

    if 'webmd' in exec_order:
        parallel_exec('webmd', configs, doctors_list[1:])
    # print(doctors_list)

    # name = 'Dr. Bobby Brice Niemann, MD'
    # if doctors_list[0] in name:
    #     print('present')
    # vitalCrawler(doctors_list)
    # webMdCrawler(doctors_list)


def parallel_exec(site, config, doctor_list):
    thread_pool_size = int(config.get('thread_pool_size').data)
    chunk_size = int(config.get('chunk_size').data)
    print(thread_pool_size)
    print(chunk_size)
    batch_size = thread_pool_size * chunk_size
    num_batches = (len(doctor_list) / batch_size) + (1 if len(doctor_list) % batch_size != 0 else 0)

    for i in range(int(num_batches)):
        to_be_scraped = doctor_list[i * batch_size:min((i + 1) * batch_size, len(doctor_list))]
        tasks = []
        for thread in range(thread_pool_size):
            doc_chunk = to_be_scraped[thread * chunk_size:min((thread + 1) * chunk_size, len(to_be_scraped))]
            tasks.append(doc_chunk)
            if (thread + 1) * chunk_size >= len(doc_chunk):
                print('task chunking ended the batch : {}'.format(thread))
                break

        with ThreadPoolExecutor(thread_pool_size) as pool:
            if site == 'vital':
                print('parsing vital')
                pool.map(vital_crawler, to_be_scraped)
            else:
                pool.map(webmd_crawler, to_be_scraped)


def read_file(configs):
    file_name = configs.get("tokens_file").data
    doctors_list = []
    with open(file_name) as f:
        doctors_list = f.read().splitlines()
    return doctors_list


def get_properties():
    configs = Properties()
    with open('crawler.properties', 'rb') as config_file:
        configs.load(config_file)
    return configs


if __name__ == '__main__':
    print("starting  crawler")
    start_crawling()
    # try:
    #     print('stra'+90)
    # except Exception as ex:
    #     print(type(ex).__name__)
    #     print(str(ex))
    #     print(type(ex))

    print('webmd crawler')

