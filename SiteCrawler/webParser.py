import json

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By


def connect_site():
    options = webdriver.ChromeOptions()
    options.accept_insecure_certs = True
    driver = webdriver.Chrome(options)
    try:
        driver.get()
        driver.get(
            "https://doctor.webmd.com/doctor/bobby-niemann-3c2084cd-672b-40fc-88e3-282fb7ba2303-overview?lid=5537118")
        driver.implicitly_wait(1)

        nameElement = driver.find_element(By.CSS_SELECTOR, '#app .profile-main .profile-topcard-wrap ' +
                                          '.profile-topcard .prov-Basic ' +
                                          '.prov-name-txt ' + '.provider-full-name')

        doctorInfo = driver.find_element(By.CSS_SELECTOR, '#app .center-well.center-wrapper ' +
                                         '.profile-page-topcard-sidebar ' +
                                         '.bottom-part ' + '.basic-cards-container ')

        reviews = doctorInfo.find_elements(By.CSS_SELECTOR,
                                           '.profile-basecard ' + '.card .basecard-container .basecard-content '
                                           + '.overall .overall-rating .rating-subheader')

        showAll = doctorInfo.find_element(By.CSS_SELECTOR,
                                 '.provider-location-info .provider-office-info .card .basecard-container .basecard-content .provider-location-url')
        driver.execute_script("arguments[0].click();", showAll)
        #ActionChains(driver).click(showAll).perform()
        driver.implicitly_wait(1)
        providerLocations = doctorInfo.find_elements(By.CSS_SELECTOR,
                                                     '.provider-location-info .provider-office-info .card .basecard-container .basecard-content .location ')
        #.provider-office-info .card .basercard-container .basecard-content
        print('Doctor - ' + nameElement.text)

        print(' Provider Locations - '+str(len(providerLocations)))
        print(' ')
        for location in providerLocations:

            practice = location.find_element(By.CSS_SELECTOR, '.location-practice-name ').text
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
        for review in reviews:
            ratingName = review.find_element(By.CSS_SELECTOR, '.rating-name')
            rating = review.find_element(By.CSS_SELECTOR, '.rating-value')
            print(ratingName.text + " - " + str(rating.text))

    except Exception as ex:
        print(str(ex))

    driver.close()
