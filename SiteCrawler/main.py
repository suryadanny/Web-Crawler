# from webParser import connect_site
import csv

from webmdCrawler import webMdCrawler
from vitalCrawler import vitalCrawler
from threading import Thread
from jproperties import Properties
from utils import prepare_file_headers
from npiCrawler import npiDetailFetcher

def start_crawling():
    configs = get_properties()
    npiDetailFetcher(configs)
    doctors_list = read_file(configs)
    print(doctors_list)
    prepare_file_headers()
    name = 'Dr. Bobby Brice Niemann, MD'
    if doctors_list[0] in name:
        print('present')
    vitalCrawler(doctors_list)
    webMdCrawler(doctors_list)



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
    #vitalCrawler(None)

    print('webmd crawler')
    #webMdCrawler()



