import csv

def prepare_failed_headers():
    with open('failed_scrape_update.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(['npi id','Doctor', 'specialisation','city','Website','reason' ])
def prepare_file_headers():
    with open('ratings.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(['Doctor', 'Website','rating_text' , 'rating'])

    with open('locations.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(['Doctor', 'Practice','Address' , 'mobile'])

    with open('reviews.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(['Doctor', 'Website','Date' , 'review'])

