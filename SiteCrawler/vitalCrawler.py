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


def vital_crawler(doctor_row):
    print(doctor_row.replace('\"',''))
    attributes = doctor_row.replace('\"','').split(',')
    npi_id = attributes[0]
    doctor_name = attributes[1]
    specialisation = attributes[2]
    cities = attributes[3].split('|')
    options = webdriver.ChromeOptions()
    options.accept_insecure_certs = True
    driver = webdriver.Chrome(options)
    site = 'vital'
    try:
        print('doctor name '+ str(attributes[1])+' , cities - '+str(cities))

        if len(attributes) > 4 and (site != attributes[-2].strip().lower() or attributes[-1] != 'retryable'):
            return

        if len(attributes[3]) <= 0:
            with file_lock:
                write_failed([npi_id,doctor_name, specialisation, attributes[3], site, 'Not located in texas'])
            return

        status, stat_code = find_doctor(driver, attributes)
        if not status:
            print('couldn\'t find doctor : {} in {} site '.format(doctor_name, site))
            stat_reason = 'couldn\'t find doctor : {} in {}'.format(doctor_name,
                                                                    site) if stat_code == 0 else 'retryable'
            with file_lock:
                write_failed([npi_id, doctor_name, specialisation, attributes[3], site, stat_reason])
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
        print('main function -' + str(ex))
        print(type(ex))
        with file_lock:
            if 'disconnected:' in str(ex):
                write_failed([npi_id, doctor_name, specialisation, attributes[3], site, 'retryable'])
            else:
                write_failed([npi_id, doctor_name, specialisation, attributes[3], site, str(ex)[:200]])

    driver.close()


def write_failed(row):
    with open('failed_scrape_update.csv', 'a', newline='') as csvfile:
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


def find_doctor(driver, doctor):
    # name = 'Dr. Bobby Brice Niemann'
    #name = 'Bobby Brice Niemann'
    #name = 'James V Stonecipher'
    name_split = doctor[1].split(' ')

    cities = doctor[3].split('|')

    specialisation = set([spec.lower().strip() for spec in doctor[2].split('|')])
    try:

        driver.get(
            "https://www.vitals.com/")
        searchBox = driver.find_element(By.CSS_SELECTOR,
                                        '#app #app .top-wrapper .vitals-top-container .search-wrapper .search-bar .search-bar-wrapper')
        search = searchBox.find_element(By.CSS_SELECTOR,
                                        '.name .webmd-input__div .webmd-input__inner')
        #sending doctor name to the search box
        search.send_keys(doctor[1])

        #search.submit()

        if len(cities) > 0:
            city_search = searchBox.find_element(By.CSS_SELECTOR,
                                                 '.location .webmd-input__div .webmd-input__inner')
            city = cities[0].strip() + ", TX"
            city_search.clear()
            city_search.send_keys(city)
            city_search.submit()
        else:
            search.submit()


        driver.implicitly_wait(3)
        search_result = driver.find_elements(By.CSS_SELECTOR,
                                             '#app .webmd-col .webmd-container .webmd-main .webmd-row .webmd-col .infinite-loader .result-page .provider-card  ')



        profile_cards = []
        doctors_list = []
        print('search results count - ' + str(len(search_result)))
        profile_dict = {}
        for row in search_result:
            profile_dict[row.id] = driver.execute_script("return arguments[0].textContent;", row)

        for row in search_result:
            driver.execute_script("arguments[0].scrollIntoView();", row)
            summary = row.text.splitlines()
            #state = driver.execute_script("return arguments[0].textContent;", row)
            if len(summary) == 0:
                continue
            print(summary[0])
            print('hsas - ' + summary[1])
            print('hsas2 - ' + summary[2])
            # and specialisation.lower() in summary[2].lower()
            if (name_split[0].lower() in summary[0].lower() and name_split[-1].lower() in summary[0].lower()
                    and match_specs(summary[1], specialisation)):
                profile = row.find_elements(By.CSS_SELECTOR,
                                            '.webmd-card .webmd-card__body .card-content')
                profile_cards.append(profile[0])
                break

        print('search results count - ' + str(len(profile_cards)))
        print(doctors_list)
        #print([row.find_element(By.CSS_SELECTOR, '.prov-name-wrap .prov-name').text for row in profile_cards if len(row)> 0])

        doctor_link = profile_cards[0].find_element(By.CSS_SELECTOR, '.card-info .overlay-card-link')
        driver.execute_script("arguments[0].click();", doctor_link)
        return True, 1

    except Exception as ex:
        print(str(ex))
        print(type(ex))
        stat_code = 2 if 'disconnected:' in str(ex) else 0
        return False, stat_code


def match_specs(summary, specs_set):
    profile_specs = summary.split(',')
    for spec in profile_specs:
        if spec.strip().lower() in specs_set:
            return True

    return False
