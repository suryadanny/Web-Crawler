import csv

from selenium import webdriver
from selenium.webdriver.common.by import By
from threading import Lock, Thread
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

file_lock = Lock()


def vital(doctor_list, config):
    thread_pool_size = int(config.get('thread-pool-size').data)
    chunk_size = int(config.get('chunk-size').data)
    print(thread_pool_size)
    print(chunk_size)
    batch_size = thread_pool_size * chunk_size
    num_batches = (len(doctor_list) / batch_size) + (1 if len(doctor_list) % batch_size != 0 else 0)

    for i in range(num_batches):
        to_be_scraped = doctor_list[i * batch_size:min((i + 1) * batch_size, len(doctor_list))]
        tasks = []
        for thread in range(thread_pool_size):
            doc_chunk = to_be_scraped[thread * chunk_size:min((thread + 1) * chunk_size, len(to_be_scraped))]
            if (thread + 1) * chunk_size >= len(to_be_scraped):
                print('task chunking ended the batch')
                break

        with ThreadPoolExecutor(thread_pool_size) as pool:
            pool.map(vital_crawler, tasks)


def vital_crawler(doctor_name):
    print(doctor_name)
    options = webdriver.ChromeOptions()
    options.accept_insecure_certs = True
    driver = webdriver.Chrome(options)
    site = 'vital'
    try:

        status = find_doctor(driver, doctor_name)

        if not status:
            print('couldn\'t find doctor : {} in {}'.format(doctor_name, site))
            with file_lock:
                write_failed([doctor_name,'couldn\'t find doctor : {} in {}'.format(doctor_name, site)])
            return

        driver.implicitly_wait(3)

        nameElement = driver.find_element(By.CSS_SELECTOR,
                                          '#app #app-content #vitals-top-header .profile-header-container ' +
                                          '.profile-header .profile-header-section ' +
                                          '.valign-wrapper ' + '.header-info .name .name-info .h1-container .loc-vs-fname')

        doctorInfo = driver.find_element(By.CSS_SELECTOR, '#app #app-content .webmd-container .webmd-main ' +
                                         '.webmd-row .webmd-col ')

        reviews = doctorInfo.find_elements(By.CSS_SELECTOR,
                                           '#rating-overview .webmd-card__body ' + '.card-content .reviews-section '
                                           + '.reviews-content')

        showAll = doctorInfo.find_elements(By.CSS_SELECTOR,
                                           '#location-card-holder .webmd-card__body .card-action .webmd-button')
        if len(showAll) > 0:
            driver.execute_script("arguments[0].click();", showAll[0])

        driver.implicitly_wait(1)
        providerLocations = doctorInfo.find_elements(By.CSS_SELECTOR,
                                                     '#location-card-holder .webmd-card__body .location-map .location-lines')

        doctor_name = nameElement.text

        print('Doctor - ' + nameElement.text)

        print(' Provider Locations - ' + str(len(providerLocations)))
        print(' ')
        locations_list = []
        for location in providerLocations:

            practice = location.find_element(By.CSS_SELECTOR, '.location-line .title ').text
            address = location.find_element(By.CSS_SELECTOR, '.location-line .address').text
            mobile_list = location.find_elements(By.CSS_SELECTOR, '.location-line .phone .loc-vl-telep')
            loc = []
            loc.append(doctor_name)

            loc.append(practice)
            loc.append(address.replace('\n', ', '))

            print('-------------------------------')
            print(' ')
            print(practice)
            print(address)

            if len(mobile_list) > 0:
                for mobile in mobile_list:
                    print(mobile.text)
                    loc.append(mobile.text)
            else:
                loc.append('-')
            locations_list.append(loc)

        print('---------------------------------')
        reviews_list = []
        for review in reviews:
            review_text = review.find_element(By.CSS_SELECTOR, '.review-text .lov-vr-irrttxt')
            review_date = review.find_element(By.CSS_SELECTOR, '.headline')
            reviews_list.append([doctor_name, site, review_date.text, review_text.text])
            print(review_date.text + " - " + review_text.text)

        print('Ratings Summary')
        ratings_list = []
        ratings = doctorInfo.find_elements(By.CSS_SELECTOR,
                                           '#rating-overview .webmd-card__header .persp-callsout .persp-callsout-item '
                                           )
        for rating in ratings:
            rating_text = rating.find_element(By.CSS_SELECTOR, 'dd')
            rating_score = rating.find_element(By.CSS_SELECTOR, 'dt')
            if rating_text.get_attribute('data-qa-waittm') is not None:
                continue
            ratings_list.append([doctor_name, site, rating_text.text, rating_score.text])
            print(rating_text.text + ' - ' + rating_score.text)

        with file_lock:
            print('acquired lock')
            write_to_file(locations_list, reviews_list, ratings_list)




    except Exception as ex:
        print(str(ex))
        with file_lock:
            write_failed([doctor_name, 'retryable error like timeout'])

    driver.close()

def write_failed(row):
    with open('failed_scrape.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(row)
def write_to_file(locations_list, reviews_list, ratings_list):
    with open('ratings.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerows(ratings_list)

    with open('reviews.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerows(reviews_list)

    with open('locations.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerows(locations_list)


def find_doctor(web_driver, doctor):
    driver = None
    if not web_driver:
        options = webdriver.ChromeOptions()
        options.accept_insecure_certs = True
        driver = webdriver.Chrome(options)
    else:
        driver = web_driver

    # name = 'Dr. Bobby Brice Niemann'
    name = 'Bobby Brice Niemann'
    #name = 'James V Stonecipher'
    name_split = doctor.split(' ')
    specialisation = 'neurology'
    try:

        driver.get(
            "https://www.vitals.com/")
        searchBox = driver.find_element(By.CSS_SELECTOR,
                                        '#app #app .top-wrapper .vitals-top-container .search-wrapper .search-bar .search-bar-wrapper')
        search = searchBox.find_element(By.CSS_SELECTOR,
                                        '.search-input .webmd-input__div .webmd-input__inner')
        search.send_keys(doctor)
        search.submit()
        driver.implicitly_wait(3)
        search_result = driver.find_elements(By.CSS_SELECTOR,
                                             '#app .webmd-col .webmd-container .webmd-main .webmd-row .webmd-col .infinite-loader .result-page .provider-card  ')

        profile_cards = []
        doctors_list = []
        print('search results count - ' + str(len(search_result)))
        for row in search_result:

            summary = row.text.splitlines()
            print(summary[0])
            # and specialisation.lower() in summary[2].lower()
            if name_split[0].lower() in summary[0].lower() and name_split[-1].lower() in summary[0].lower():
                profile = row.find_elements(By.CSS_SELECTOR,
                                            '.webmd-card .webmd-card__body .card-content')
                profile_cards.append(profile[0])
                break

        print('search results count - ' + str(len(profile_cards)))
        print(doctors_list)
        #print([row.find_element(By.CSS_SELECTOR, '.prov-name-wrap .prov-name').text for row in profile_cards if len(row)> 0])

        doctor_link = profile_cards[0].find_element(By.CSS_SELECTOR, '.card-info .overlay-card-link')
        driver.execute_script("arguments[0].click();", doctor_link)
        return True

    except Exception as ex:
        print(str(ex))
        return False
