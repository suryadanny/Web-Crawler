import csv

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By

def webMdCrawler():
    options = webdriver.ChromeOptions()
    options.accept_insecure_certs = True
    driver = webdriver.Chrome(options)
    site ='webmd'
    try:
        find_doctor(driver)

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

        print(' Provider Locations - '+str(len(providerLocations)))
        print(' ')
        for location in providerLocations:

            practice_element = location.find_elements(By.CSS_SELECTOR, '.location-practice-name ')
            practice = ''
            if len(practice_element) > 0:
                practice = practice_element[0].text
            address = location.find_element(By.CSS_SELECTOR, '.location-address').text + ', ' + location.find_element(
                By.CSS_SELECTOR, '.location-geo').text
            mobile_list = location.find_elements(By.CSS_SELECTOR, '.location-phone .loc-coi-telep')

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
            entry = [doctor_name,site, ratingName.text, rating.text]
            ratings_list.append(entry)
            print(ratingName.text + " - " + str(rating.text))

        with open('ratings.csv', 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile,
                                    quotechar='"', quoting=csv.QUOTE_ALL)
            csv_writer.writerows(ratings_list)

    except Exception as ex:
        print(str(ex))

    driver.close()
def find_doctor(driver):

    # name = 'Dr. Bobby Brice Niemann'
    name = 'Bobby Brice Niemann'
    #name = 'James V Stonecipher'
    name_split = name.split(' ')
    specialisation = 'neurology'
    try:

        driver.get(
            "https://doctor.webmd.com/")
        searchBox = driver.find_element(By.CSS_SELECTOR,
                                        '#app #app .webmd-header .header-paddingbottom .search-wrapper .search-form .search-on-desktop')
        search = searchBox.find_element(By.CSS_SELECTOR,
                                        '.search-input .webmd-typeahead2 .webmd-input__div .webmd-input__inner')
        search.send_keys(name)
        search.submit()
        driver.implicitly_wait(3)
        search_result = driver.find_elements(By.CSS_SELECTOR,
                                             '#app .webmd-container .page-mb .webmd-row .webmd-col .page-skwebmdton .serp-srl-layout .webmd-row .result-column .infinite-loader .resultslist-content .basic  ')

        profile_cards = []
        doctors_list= []
        print('search results count - ' + str(len(search_result)))
        for row in search_result:

            summary = row.text.splitlines()
            print(summary[1])
            # and specialisation.lower() in summary[2].lower()
            if name_split[0].lower() in summary[1].lower() and name_split[-1].lower() in summary[1].lower():
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


    except Exception as ex:
        print(str(ex))

