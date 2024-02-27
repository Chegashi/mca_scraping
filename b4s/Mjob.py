import logging
import requests
import time
import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import monotonically_increasing_id

missing_value = "non spécifié"

def current_date_as_string() -> str:
    """
    Returns the current date as a string in the format "YYYY-MM-DD".

    Returns:
        str: The current date in the format "YYYY-MM-DD".

    Example:
        current_date = current_date_as_string()
        print(current_date)  # Output: '2024-02-16'
    """
    return datetime.now().strftime('%Y-%m-%d')

def set_logger() -> logging.Logger:
    """
    Configures and returns a logger for a web scraping job.

    Returns:
        logging.Logger: A configured logger object for logging scraping activities.
        
    Notes:
        This function sets up logging for a web scraping job, providing both a file handler and a console handler.
        It configures the logging level to DEBUG, allowing logging of all messages.
        The log messages include the timestamp, logging level, and the message itself.

    """

    logger = logging.getLogger('scraping_mjob')
    logger.setLevel(logging.DEBUG)  # Set the logging level for the logger

    # Create file handler which logs even debug messages
    fh = logging.FileHandler('mjob_scraper.log')
    fh.setLevel(logging.DEBUG)  # Set logging level for the file handler

    # Create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)  # Set logging level for the console handler

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def check_value_data(data: str, field: str, logger: logging.Logger) -> str:
    """
    Checks the validity of a data value, removes non-ASCII characters, and logs a warning if it's empty.

    Args:
        data (str): The data value to be checked.
        field (str): The name of the field associated with the data value.
        logger (logging.Logger): A configured logger object for logging warnings.

    Returns:
        str: The cleaned data value after removing non-ASCII characters and multiple spaces, tabs, and newline characters.
    """
    # Check if the data value is empty
    if not data:
        logger.warning(f"Empty data found in {field}")
        return missing_value

    # Remove non-ASCII characters
    data = re.sub(r'[^\x00-\x7F]+', '', data)

    # Remove multiple spaces, tabs, and newline characters
    cleaned_data = re.sub(r'\s+', ' ', data)
    return cleaned_data

def get_locations(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the location from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted location, or an empty string if not found or unable to extract.

    Example:
        location_tag = tag.find('div', class_='header-info').find('div', class_='location')
        logger = set_logger()
        location = get_locations(location_tag, logger)
        print(location)
    """
    try:
        location = tag.find('div', class_='header-info').find('div', class_='location').text.strip()
        return check_value_data(location, 'location', logger)
    except Exception as e:
        logger.warning(f"Location of the offer not found. Error: {e}")
        return missing_value

def get_poste(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the job title from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted job title, or "non spécifié" if not found or unable to extract.

    """
    try:
        job_title = tag.find('h1', class_='offer-title').text.strip()
        return check_value_data(job_title, 'poste', logger)
    except Exception as e:
        logger.warning(f"Title of the job post not found. Error: {e}")
        return missing_value

def get_company(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the company name from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted company name, or an empty string if not found or unable to extract.

    """
    try:
        company = tag.find('ul', class_='list-details').find_all('li')[0].find('h3').text.strip()
        return check_value_data(company, 'company', logger)
    except Exception as e:
        logger.warning(f"Company name of the offer not found. Error: {e}")
        return missing_value
 
def get_contrat(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the contract type from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted contract type, or an empty string if not found or unable to extract.

    """
    try:
        contrat = tag.find('ul', class_='list-details').find_all('li')[1].find('h3').text.strip()
        return check_value_data(contrat, 'contract type', logger)
    except Exception as e:
        logger.warning(f"Contract type of the offer not found. Error: {e}")
        return missing_value

def get_salary(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the salary information from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted salary information, or an empty string if not found or unable to extract.

    """
    try:
        salary = tag.find('ul', class_='list-details').find_all('li')[2].find('h3').text.strip()
        return check_value_data(salary, 'salary', logger)
    except Exception as e:
        logger.warning(f"Salary information of the offer not found. Error: {e}")
        return missing_value

def get_recruiter(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the recruiter information from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted recruiter information, or an empty string if not found or unable to extract.
    """
    try:
        heading_div = tag.find("h3", class_="heading", string="Le recruteur :")
        recruiter = heading_div.find_next_sibling("div").text.strip()
        return check_value_data(recruiter, 'Le recruteur :', logger)
    except Exception as e:
        logger.warning(f"Recruiter information of the offer not found. Error: {e}")
        return missing_value

def get_date(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the date information from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted date information, or an empty string if not found or unable to extract.
    """
    try:
        prefix = "L'offre a été publiée il y a"
        sufix = "sur le site."
        date_div = tag.find("div", class_="bottom-content").find('span')
        date = date_div.text.strip().replace(prefix, "").replace(sufix, "")
        return check_value_data(date, 'date', logger)
    except Exception as e:
        logger.warning(f"Date of the offer not found. Error: {e}")
        return missing_value

def get_profile(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the profile information from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted profile information, or an empty string if not found or unable to extract.
    """
    try:
        heading_div = tag.find("h3", class_="heading", string="Poste à occuper :")
        profile = heading_div.find_next_sibling("div").text.strip()
        return check_value_data(profile, 'Poste à occuper :', logger)
    except Exception as e:
        logger.warning(f"Profile of the offer not found. Error: {e}")
        return missing_value

def get_missions(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the missions information from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted missions information, or an empty string if not found or unable to extract.
    """
    try:
        heading_div = tag.find("h3", class_="heading", string="Poste à occuper :")
        missions = heading_div.find_next_sibling("div").text.strip()
        return check_value_data(missions, 'Missions :', logger)
    except Exception as e:
        logger.warning(f"Missions of the offer not found. Error: {e}")
        return missing_value

def get_business_sector(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the business sector information from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted business sector information, or an empty string if not found or unable to extract.
    """
    try:
        heading_div = tag.find("h3", class_="heading", string="Secteur(s) d'activité :")
        business_sector = heading_div.find_next_sibling("div").text.strip()
        return check_value_data(business_sector, "Secteur(s) d'activité :", logger)
    except Exception as e:
        logger.warning(f"Business sector of the offer not found. Error: {e}")
        return missing_value

def get_education_level(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the education level information from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted education level information, or an empty string if not found or unable to extract.
    """
    try:
        heading_div = tag.find("h3", class_="heading", string="Niveau d'études exigé :")
        education_level = heading_div.find_next_sibling("div").text.strip()
        return check_value_data(education_level, "Niveau d'études exigé :", logger)
    except Exception as e:
        logger.warning(f"Education level of the offer not found. Error: {e}")
        return missing_value

def get_job(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the job information from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted job information, or an empty string if not found or unable to extract.
    """
    try:
        heading_div = tag.find("h3", class_="heading", string="Métier(s) :")
        job = heading_div.find_next_sibling("div").text.strip()
        return check_value_data(job, "Métier(s) :", logger)
    except Exception as e:
        logger.warning(f"Job of the offer not found. Error: {e}")
        return missing_value

def get_experience_level(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the experience level information from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted experience level information, or an empty string if not found or unable to extract.
    """
    try:
        heading_div = tag.find("h3", class_="heading", string="Niveau d'expériences requis :")
        experience_level = heading_div.find_next_sibling("div").text.strip()
        return check_value_data(experience_level, "Niveau d'expériences requis :", logger)
    except Exception as e:
        logger.warning(f"Experience level of the offer not found. Error: {e}")
        return missing_value

def get_languages_level(tag: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extracts the languages level information from a BeautifulSoup tag and returns it after validation.

    Args:
        tag (BeautifulSoup): The BeautifulSoup tag containing job information.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        str: The extracted languages level information, or an empty string if not found or unable to extract.
    """
    try:
        heading_div = tag.find("h3", class_="heading", string="Langue(s) exigée(s) :")
        languages_level = heading_div.find_next_sibling("div").text.strip()
        return check_value_data(languages_level, "Langue(s) exigée(s) :", logger)
    except Exception as e:
        logger.warning(f"Languages level of the offer not found. Error: {e}")
        return missing_value

def scrape_one_offer(offer_url: str, logger: logging.Logger) -> dict:
    """
    Scrapes job offer information from a given URL.

    Args:
        offer_url (str): The URL of the job offer page.
        logger (logging.Logger): Logger object for logging messages.

    Returns:
        dict: A dictionary containing various job offer information.
    """
    try:
        response = requests.get(offer_url)
    except Exception as e:
        logger.info(f"The server {offer_url} could not be found. Error: {e}")
        return {}

    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    return {
        "Postes": get_poste(soup, logger),
        "Secteur d'activité": get_business_sector(soup, logger),
        "Locations": get_locations(soup, logger),
        "Dates": get_date(soup, logger),
        "Societes": get_company(soup, logger),
        "Contrats": get_contrat(soup, logger),
        "Profils": get_profile(soup, logger),
        "Missions": get_missions(soup, logger),
        "Details entreprises": get_recruiter(soup, logger),
        "Salaire": get_salary(soup, logger),
        "Metiers": get_job(soup, logger),
        "Niveau d'expérience": get_experience_level(soup, logger),
        "Niveau d'études": get_education_level(soup, logger),
        "Langues exigées": get_languages_level(soup, logger)
    }

def scrap_one_page(page_url: str, logger: logging.Logger):
    """
    Scrapes data for job offers from a single page URL.

    Args:
        page_url (str): The URL of the page containing job offers.
        logger (logging.Logger): Logger object for logging messages.

    Yields:
        dict: A dictionary containing job offer information scraped from each offer on the page.

    """
    logger.info(f"Start scraping data for page_offers with URL: {page_url}")
    try:
        response = requests.get(page_url)
        logger.info(f"The page was fetched successfully from: {page_url}")
    except Exception as e:
        logger.warning(f"The server {page_url} could not be reached. Error: {e}")
        return

    logger.info(f"The data was fetched from: {page_url}")
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    try:
        offer_boxes = soup.find_all('div', class_='offer-box')
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
            yield scrape_one_offer(offer_url, logger)
    except Exception as e:
        logger.warning(f"Error occurred in scrap_one_page: {e}")

def main():
    # Set up logger
    logger = set_logger()
    logger.info("Start scraping script")
    
    # Define the URL to scrape
    url = 'http://www.m-job.ma/recherche?page=1'

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Scrape and Store") \
        .getOrCreate()

    # Create an empty list to hold rows
    rows = []

    # Iterate through the offers returned by scrap_one_page
    for i, offer_data in enumerate(scrap_one_page(url, logger)):
        logger.info(f"Scraped data for page {i + 1} with URL: {url} finished")

        # Add index to offer_data
        offer_data[""] = i 
        logger.debug(f"the data wich scraped from {url} is => {offer_data} ")
        
        # Append each offer data to the list
        rows.append(offer_data)

    # Create Spark DataFrame from the list of rows
    df = spark.createDataFrame(rows)

    # # Move index column to the first position
    # columns = [col for col in df.columns if col != "index"]
    # df = df.select(columns)

    # Write DataFrame to CSV file using Spark
    output_path_spark = f"/tmp/monitoring/Mjob-{current_date_as_string()}-spark.csv"
    df.write.csv(output_path_spark, header=True, mode="overwrite")

    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Write DataFrame to CSV file using Pandas
    output_path_pandas = f"Mjob-{current_date_as_string()}.csv"
    pandas_df.to_csv(output_path_pandas, index=False)

    # Log completion message
    logger.info(f"Data scraped from {url}.")
    logger.info(f"Data stored in CSV file (Spark): {output_path_spark}")
    logger.info(f"Data stored in CSV file (Pandas): {output_path_pandas}")

    # Stop Spark session
    spark.stop()



# def main():
#     # Set up logger
#     logger = set_logger()
#     logger.info("Start scraping script")
    
#     # Define the URL to scrape
#     url = 'http://www.m-job.ma/recherche?page=1'

#     # Initialize Spark session
#     spark = SparkSession.builder \
#         .appName("Scrape and Store") \
#         .getOrCreate()

#     # Create an empty list to hold rows
#     rows = []

#     # Iterate through the offers returned by scrap_one_page
#     for i, offer_data in enumerate(scrap_one_page(url, logger)):
#         logger.info(f"Scraped data for page {i + 1} with URL: {url} finished")

#         # Add index to offer_data
#         offer_data["index"] = i + 1
        
#         # Append each offer data to the list
#         rows.append(offer_data)

#     # Create Spark DataFrame from the list of rows
#     df = spark.createDataFrame(rows)

#     # Move index column to the first position
#     columns = ["index"] + [col for col in df.columns if col != "index"]
#     df = df.select(columns)

#     # Write DataFrame to CSV file
#     output_path = f"./Mjob-{current_date_as_string}.csv"
#     df.write.csv(output_path, header=True, mode="overwrite")

#     # Log completion message
#     logger.info(f"Data scraped from {url} and stored in {output_path}")

#     # Stop Spark session
#     spark.stop()

# def main():
#     logger = set_logger()
#     logger.info("start scraping script")
#     url = 'http://www.m-job.ma/recherche?page=1'

#     for i, offer_data in enumerate(scrap_one_page(url, logger)):
#         logging.info(f"scrape data for {i} pages with url: {url} finished")
#         for key, value in offer_data.items():
#             print(f"[{key}] : \n[{value}]\n\n")
#         break

    # spark = SparkSession.builder.appName("JobScraping").getOrCreate()

    # Define schema for your data
    # schema = StructType([
    #     StructField('Postes', StringType(), True),
    #     StructField('Secteur d\'activité', StringType(), True),
    #     StructField('Locations', StringType(), True),
    #     StructField('Dates', StringType(), True),
    #     StructField('Societes', StringType(), True),
    #     StructField('Contrats', StringType(), True),
    #     StructField('Profils', StringType(), True),
    #     StructField('Missions', StringType(), True),
    #     StructField('Details entreprises', StringType(), True),
    #     StructField('Salaire', StringType(), True),
    #     StructField('Metiers', StringType(), True),
    #     StructField('Niveau d\'expérience', StringType(), True),
    #     StructField('Niveau d\'études', StringType(), True),
    #     StructField('Langues exigées', StringType(), True),
    #     StructField('url', StringType(), True)
    # ])

    # URL to scrape
    

    # Use a list to collect data rows
    # data_rows = []

    # Loop through scraped data and append to data_rows
    # for data in scrap_one_page(url, logging):
    # #     # You may need to adjust the data structure to match the schema
    #     data_rows.append(data)
    
    # print(data_rows)
    #     return ''

    # Convert list to Spark DataFrame
    # df = spark.createDataFrame(data_rows, schema=schema)

    # Write DataFrame to CSV
    # df.write.csv('mjob_spark.csv', mode='overwrite', header=True)

    # Stop the Spark session
    # spark.stop()

if __name__ == '__main__':
    main()

        # for offer_box in offer_boxes:
        #     offer_title_tag = offer_box.find('h3', class_='offer-title')
        #     url = offer_title_tag.a['href']
        #     one_offer_data = scrape_one_offer(url)
        #     one_offer_data["Postes"] = offer_title_tag.text.strip()
        #     one_offer_data["url"] = url
            
        #     date_span = offer_box.find('div', class_='date-buttons').span
        #     one_offer_data["Dates"] = date_span.text.strip()
            
        #     location_div = offer_box.find('div', class_='location')
        #     location = ' '.join(location_div.text.replace('\n', '').strip().split(' '))
        #     one_offer_data["Locations"] = location
            
            # yield match_data(one_offer_data)
    # except Exception as e:
    #     logging.error(f"Failed to scrape page from {url}: {e}")