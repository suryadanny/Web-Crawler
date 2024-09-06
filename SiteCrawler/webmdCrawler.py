import csv

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from threading import Lock
from selenium.webdriver.common.keys import Keys
import time
file_lock = Lock()

def webmd_crawler(doctor_row):
    attributes = doctor_row.replace('\"','').split(',')
    npi_id = attributes[0]
    doctor_name = attributes[1]
    specialisation = attributes[2]
    cities = attributes[3].split('|')

    options = webdriver.ChromeOptions()
    options.accept_insecure_certs = True
    driver = webdriver.Chrome(options)
    site = 'webmd'
    try:

        if len(attributes[3]) <= 0:
            with file_lock:
                write_failed([npi_id,doctor_name, specialisation, attributes[3], site, 'Not located in texas'])
            return

        if len(attributes) > 4 and site != attributes[-2].strip().lower():
            return

        status, stat_code = find_doctor(driver, attributes)
        if not status:
            print('couldn\'t find doctor : {} in {} site '.format(doctor_name,site))
            stat_reason = 'couldn\'t find doctor : {} in {}'.format(doctor_name, site) if stat_code == 0 else 'retryable'
            with file_lock:
                write_failed([npi_id,doctor_name, specialisation, attributes[3], site, stat_reason])
            return

        driver.implicitly_wait(3)

        nameElement = driver.find_element(By.CSS_SELECTOR, '#app .profile-main .profile-topcard-wrap ' +
                                          '.profile-topcard .prov-Basic ' +
                                          '.prov-name-txt ' + '.provider-full-name')

        doctorInfo = driver.find_element(By.CSS_SELECTOR, '#app .center-well.center-wrapper ' +
                                         '.profile-page-topcard-sidebar ' +
                                         '.bottom-part ' + '.basic-cards-container ')

        reviews = doctorInfo.find_elements(By.CSS_SELECTOR,
                                           '.profile-basecard ' + '.card .basecard-container .basecard-content '
                                           + '.overall .overall-rating .rating-subheader')

        showAll = doctorInfo.find_elements(By.CSS_SELECTOR,
                                           '.provider-location-info .provider-office-info .card .basecard-container .basecard-content .provider-location-url')

        if len(showAll) > 0:
            driver.execute_script("arguments[0].click();", showAll[0])

        driver.implicitly_wait(1)
        providerLocations = doctorInfo.find_elements(By.CSS_SELECTOR,
                                                     '.provider-location-info .provider-office-info .card .basecard-container .basecard-content .location ')

        doctor_name = nameElement.text
        print('Doctor - ' + nameElement.text)

        print(' Provider Locations - ' + str(len(providerLocations)))
        print(' ')
        for location in providerLocations:

            practice_element = location.find_elements(By.CSS_SELECTOR, '.location-practice-name ')
            practice = ''
            if len(practice_element) > 0:
                practice = practice_element[0].text
            address_element = location.find_elements(By.CSS_SELECTOR, '.location-address')
            geo_element = location.find_elements(By.CSS_SELECTOR, '.location-geo')
            mobile_list = location.find_elements(By.CSS_SELECTOR, '.location-phone .loc-coi-telep')
            address = ''
            if len(address_element)>0:
                address += address_element[0].text

            if len(geo_element) > 0:
                address += ', ' + geo_element[0].text

            print('-------------------------------')
            print(' ')
            print(practice)
            print(address)

            if len(mobile_list) > 0:
                for mobile in mobile_list:
                    print(mobile.text)

        print('---------------------------------')
        ratings_list = []
        for review in reviews:
            ratingName = review.find_element(By.CSS_SELECTOR, '.rating-name')
            rating = review.find_element(By.CSS_SELECTOR, '.rating-value')
            entry = [doctor_name, site, ratingName.text, rating.text]
            ratings_list.append(entry)
            print(ratingName.text + " - " + str(rating.text))

        with file_lock:
            write_to_file(ratings_list)


    except Exception as ex:
        print(str(ex))
        with file_lock:
            write_failed([npi_id,doctor_name,specialisation, attributes[3], site, 'retryable error like timeout'])

    driver.close()



def write_failed(row):
    with open('failed_scrape.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(row)


def write_to_file(ratings_list):
    with open('ratings.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerows(ratings_list)



def find_doctor(driver, doctor):
    # name = 'Dr. Bobby Brice Niemann'
    name = 'Bobby Brice Niemann'
    #name = 'James V Stonecipher'
    name_split = doctor[1].split(' ')
    cities = doctor[3].split('|')

    specialisation = set([spec.lower().strip() for spec in doctor[2].split('|')])
    try:

        driver.get(
            "https://doctor.webmd.com/")
        searchBox = driver.find_element(By.CSS_SELECTOR,
                                        '#app #app .webmd-header .header-paddingbottom .search-wrapper .search-form .search-on-desktop')
        search = searchBox.find_element(By.CSS_SELECTOR,
                                        '.search-input .webmd-typeahead2 .webmd-input__div .webmd-input__inner')
        search.send_keys(doctor[1])
        #search.submit()

        if len(cities) > 0:
            city_search= searchBox.find_element(By.CSS_SELECTOR,
                                        '.location-input .webmd-typeahead2 .webmd-input__div .webmd-input__inner')
            city = cities[0] + ", TX"
            city_search.send_keys(Keys.CONTROL + "a")
            city_search.send_keys(Keys.DELETE)
            city_search.send_keys(city)
            time.sleep(1)



        search.submit()

        driver.implicitly_wait(3)
        search_result = driver.find_elements(By.CSS_SELECTOR,
                                             '#app .webmd-container .page-mb .webmd-row .webmd-col .page-skwebmdton .serp-srl-layout .webmd-row .result-column .infinite-loader .resultslist-content .basic  ')

        profile_cards = []
        doctors_list = []
        print('search results count - ' + str(len(search_result)))
        for row in search_result:
            driver.execute_script("arguments[0].scrollIntoView();", row)
            summary = row.text.splitlines()
            print(summary[1])
            print(summary[2])
            #profile_card_specs.lower() in summary[2].lower()
            if (name_split[0].lower() in summary[1].lower() and name_split[-1].lower() in summary[1].lower()
                    and match_specs(summary[2], specialisation)):
                profile = row.find_elements(By.CSS_SELECTOR,
                                            '.results-card-wrap .webmd-card .webmd-card__body .webmd-row .nocardheight .webmd-col .card-info-wrap .card-content ')
                profile_cards.append(profile[0])
                print('found the card')
                break

        print('search results count - ' + str(len(profile_cards)))
        print(doctors_list)
        #print([row.find_element(By.CSS_SELECTOR, '.prov-name-wrap .prov-name').text for row in profile_cards if len(row)> 0])

        doctor_link = profile_cards[0].find_element(By.CSS_SELECTOR, '.prov-name-wrap .prov-name')
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
