import os
import logging
import pandas as pd
from glob import glob
from datetime import datetime
from bs4 import BeautifulSoup

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

max_offe = 20

def scraping_pages(url):
    pass

def get_source_page():
    pass

def scrape_one_offer():
    pass

def scrap_one_page(page_url: str, logger: logging.Logger):
    """
    Scrapes data for job offers from a single page URL.

    Args:
        page_url (str): The URL of the page containing job offers.
        logger (logging.Logger): Logger object for logging messages.

    Yields:
        dict: A dictionary containing job offer information scraped from each offer on the page.

    """
    try:
        html_source = get_source_page()
        if not html_source:
            raise "can't scrape data"
        soup = BeautifulSoup(html_source, 'html.parser')
        offer_boxes = soup.find_all("div", {"class": "offer-box"})
        logger.info(f"Extract {len(offer_boxes)} offer boxes")
    except Exception as e:
        logger.error(f"We could not fetch offers from this page {page_url}. Error: {e}")
        return
    try:
        if not offer_boxes:
            raise Exception("There are no divs with class name [offer-box]. Cannot get offers from the page.")
        logger.info(f"Start scraping {len(offer_boxes)} offers on {page_url}")
        for offer_box in offer_boxes:
            offer_url = offer_box.find('h3', class_='offer-title').a['href']
            if not offer_url:
                raise Exception("There is no URL in the offers section. Cannot get the offer link page.")
            logger.info(f"Extract the link of an offer: {offer_url}")
            data_offer =  scrape_one_offer(offer_url, logger)
            yield data_offer
            if is_the_last_offer(data_offer):
                break
    except Exception as e:
        logger.warning(f"Error occurred in scrap_one_page: {e}")

def data_scraper():
    df1 = pd.read_csv(current, encoding='utf-8-sig')

    try:
        current_working_dir = '/root/python_scripts/data_web_scrap/'
        list_of_files = glob(current_working_dir+'MJOB/*.csv')
        # * means all if need specific format then *.csv
        if not list_of_files:
            current == None
        current = max(list_of_files, key=os.path.getctime)

        # Read CSV file into a DataFrame
        df = spark.read.csv(current, header=True)

        # Get the last row
        last_row = df.collect()[-1]

        # Convert the last row to a dictionary
        last_row_dict = last_row.asDict()
    except:
        pass

    # Show the dictionary
    print(last_row_dict)

    print(current)
    url_page = "http://www.m-job.ma/recherche?page="
    page_index = 1
    while True:
        for data in scrap_one_page(url_page + str(page_index)):
            if data == last_row_dict or max_offer >= i or data == None:
                return None
            yield data
        i += 1

def get_current_date_as_string(format='%Y-%m-%d'):
    """
    Get the current date as a string.

    Args:
        format (str, optional): The format string to represent the date. Defaults to '%Y-%m-%d'.

    Returns:
        str: The current date as a string.
    """
    return datetime.now().strftime(format)

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Mjob_scraper") \
        .getOrCreate()

    # Define schema
    schema = StructType([
        StructField("", StringType(), True),
        StructField("Postes", StringType(), True),
        StructField("Secteur d'activité", StringType(), True),
        StructField("Locations", StringType(), True),
        StructField("Dates", StringType(), True),
        StructField("Societes", StringType(), True),
        StructField("Contrats", StringType(), True),
        StructField("Profils", StringType(), True),
        StructField("Missions", StringType(), True),
        StructField("Details entreprises", StringType(), True),
        StructField("Salaire", StringType(), True),
        StructField("Metiers", StringType(), True),
        StructField("Niveau d'expérience", StringType(), True),
        StructField("Niveau d'études", StringType(), True),
        StructField("Langues exigées", StringType(), True),
    ])

    # Create an empty DataFrame
    df = spark.createDataFrame([], schema=schema)

    # Append data to DataFrame directly
    for index, offer in enumerate(data_scraper()):
        current_df = spark.createDataFrame([(index, offer)], schema=schema)
        df = df.union(current_df)

    # Save DataFrame as CSV
    df.write.csv(f"Mjobparquet-{get_current_date_as_string()}.csv", header=True)

    # Convert PySpark DataFrame to pandas DataFrame
    pandas_df = df.toPandas()

    # Save pandas DataFrame to CSV
    pandas_df.to_csv(f"{get_current_date_as_string()}.csv", index=False)

    # Show DataFrame
    df.show()

    # Stop SparkSession
    spark.stop()

if __name__ == '__main__':
    main()
