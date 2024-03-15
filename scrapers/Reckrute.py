import csv
import numpy as np
from utility import *
from functools import reduce
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row

logger = None
web_site = "Reckrute"
nbr_of_offer = -1


def get_company(tag: BeautifulSoup) -> str:
    try:
        title_tag = tag.find('title')
        title_text = title_tag.string
        company_name = title_text.split('-')[-1].strip()
        logger.info(f"Company name of the offer: {company_name}")
        return remove_duplicate_whitespace(company_name)
    except Exception as e:
        logger.warning(f"Company name of the offer not found. Error: {e}")
        return None

def get_poste(tag: BeautifulSoup) -> str:
    try:
        title_text = tag.find('title').string
        job_title = title_text.split('-')[0].strip()
        job_title = job_title.replace("Offre  d'emploi ", "")
        logger.info(f"job title of the offer: {job_title}")
        return remove_duplicate_whitespace(job_title)
    except Exception as e:
        logger.warning(f"job title of the offer not found. Error: {e}")
        return None

def get_adresses(tag: BeautifulSoup) -> str:
    try:
        location = tag.find('span', id='address').text.strip()
        logger.info(f"Adresses of the offer: {location}")
        return remove_duplicate_whitespace(location)
    except Exception as e:
        logger.warning(f"Adresses of the offer not found. Error: {e}")
        return None

def get_missions(tag: BeautifulSoup) -> str:
    try:
        h2_tag = tag.find('h2', string='Poste :')
        missions = h2_tag.parent.get_text(strip=True)
        logger.info(f"Details de poste: {missions}")
        return remove_duplicate_whitespace(missions)
    except Exception as e:
        logger.warning(f"Details de post of the offer not found. Error: {e}")
        return None

def get_date(tag: BeautifulSoup) -> str:
    try:
        date = tag.find("em", class_='date').text
        date_formarted = remove_duplicate_whitespace(date).split("|")[0]
        logger.info(f"date de poste: {date_formarted}")
        return remove_duplicate_whitespace(date_formarted)
    except Exception as e:
        logger.warning(f"Date of the offer not found. Error: {e}")
        return None

def get_locations(tag: BeautifulSoup) -> str:
    try:
        titreJob = tag.find("a", class_='titreJob').text
        locations = remove_duplicate_whitespace(titreJob).split("|")[1]
        logger.info(f"Location de poste: {locations}")
        return remove_duplicate_whitespace(locations)
    except Exception as e:
        logger.warning(f"Location of the offer not found. Error: {e}")
        return None

def get_business_sector(tag: BeautifulSoup) -> str:
    try:
        business_sector = tag.find("h2", class_='h2italic').text
        business_sector_formated = remove_duplicate_whitespace(business_sector)
        logger.info(f"Secteur d'activité: {business_sector_formated}")
        return remove_duplicate_whitespace(business_sector_formated)
    except Exception as e:
        logger.warning(f"Secteur d'activité of the offer not found. Error: {e}")
        return None

def get_positions_offered(tag: BeautifulSoup) -> str:
    try:
        date_tag = tag.find('em', class_='date')
        if date_tag:
            date_text = date_tag.get_text(strip=True)
            match = re.search(r'Postes proposés:\s*(\d+)', date_text)
            if match:
                postes_proposes = match.group(1)  # This captures the number of "Postes proposés"
                logger.info(f"Postes proposés de poste: {postes_proposes}")
                return remove_duplicate_whitespace(postes_proposes)
            else:
                raise Exception("The 'Postes proposés' value was not found.")
        else:
            raise Exception("The date tag was not found.")
    except Exception as e:
        logger.warning(f"postes_proposes of the offer not found. Error: {e}")
        return None

def get_function(soup: BeautifulSoup) -> str:
    try:
        fonction_li = None
        for li in soup.find_all('li'):
            if 'Fonction :' in li.text:
                fonction_li = li
                break  # Found the li, no need to continue
        if not fonction_li:
            raise Exception("fonction of the offer not found")
        function_offer = fonction_li.a.text
        logger.info(f"fonction_li {function_offer}")
        return remove_duplicate_whitespace(function_offer)
        
    except Exception as e:
        logger.warning(f"function of the offer not found. Error: {e}")
        return None

def get_experience_level(tag: BeautifulSoup) -> str:
    try:
        experience_required_li = tag.find('li', title="Expérience requise")
        if experience_required_li:
            experience_text = " ".join(experience_required_li.stripped_strings)
            logger.info(f"Expérience requise {experience_text}")
            return remove_duplicate_whitespace(experience_text)
        else:
            raise Exception("The tag with title 'Expérience requise' was not found.")
    except AttributeError:
        logger.warning(f"Experience level of the offer not found")
    except Exception as e:
        logger.warning(f"Experience level of the offer not found. Error: {e}")
        return None

def get_education_level(tag: BeautifulSoup) -> str:
    try:
        education_level = tag.find("li", title="Niveau d'étude et formation").text
        education_level_formated = remove_duplicate_whitespace(education_level)
        logger.info(f"Niveau d’étude demandé {education_level_formated}")
        return education_level_formated
    except AttributeError:
        logger.warning(f"Education level of the offer not found")
    except Exception as e:
        logger.warning(f"Education level information cant scraped. Error: {e}")
        return None

def get_contrat(tag: BeautifulSoup) -> str:
    try:
        contrat = tag.find('span', title='Type de contrat').text
        formated_contract = remove_duplicate_whitespace(contrat)
        logger.info(f"Type de contrat: {formated_contract}")
        return remove_duplicate_whitespace(contrat)
    except Exception as e:
        logger.warning(f"Contract type of the offer not found. Error: {e}")
        return None

def get_profile(tag: BeautifulSoup) -> str:
    try:
        h2_tag = tag.find('h2', string='Profil recherché :')
        profil = remove_duplicate_whitespace(h2_tag.parent.get_text(strip=True))
        logger.info(f"Profil recherché : {profil}")
        return remove_duplicate_whitespace(profil)
    except Exception as e:
        logger.warning(f"Profil recherché of the offer not found. Error: {e}")
        return None
    
def get_recruiter(tag: BeautifulSoup) -> str:
    try:
        h2_tag = tag.find('h2', string='Entreprise :')
        recruiter = remove_duplicate_whitespace(h2_tag.parent.get_text(strip=True))
        logger.info(f"Entreprise : : {recruiter}")
        return remove_duplicate_whitespace(recruiter)
    except Exception as e:
        logger.warning(f"Entreprise of the offer not found. Error: {e}")
        return None

def get_Personality_traits(tag: BeautifulSoup) -> str:
    try:
        h2_tag = tag.find('h2', string='Traits de personnalité souhaités :')
        spans = h2_tag.parent.find_all('span')
        personality_traits = remove_duplicate_whitespace(" " .join([i.text for i in spans]))
        logger.info(f"Traits de personnalité souhaités : : : {personality_traits}")
        return personality_traits
    except Exception as e:
        logger.warning(f"Traits de personnalité souhaités of the offer not found. Error: {e}")
        return None
    
def scrape_one_offer(offer_tag: BeautifulSoup) -> dict:
    global nbr_of_offer
    nbr_of_offer += 1
    try:
        titreJob_tag = offer_tag.find('a', class_='titreJob')
        if not titreJob_tag:
            raise Exception("There are no divs with a tag name [titreJob]. Cannot get offers from the page.")
        job_url = f"https://www.rekrute.com{titreJob_tag['href']}"
        if check_url_in_cache(job_url, web_site, logger):
            return None
        html_content = get_html_content(job_url, logger)
        if not html_content:
            raise Exception("could not get the source page pffers pages")
        logger.info(f"The offer page number [{nbr_of_offer}] was fetched successfully")
        soup = BeautifulSoup(html_content, 'html.parser')
        offer_data = {
            "": nbr_of_offer,
            "Societés": get_company(soup),
            "Postes": get_poste(soup),
            "Adresses": get_adresses(soup),
            "Details de poste" : get_missions(soup),
            "Date de publication": get_date(offer_tag),
            "Location": get_locations(offer_tag),
            "Postes proposés": get_positions_offered(offer_tag),
            "Secteur d'activité": get_business_sector(soup),
            "Fonctions": get_function(offer_tag),
            "Expérience requise": get_experience_level(soup),
            "Niveau d’étude demandé": get_education_level(soup),
            "Type de contrat proposé": get_contrat(soup),
            "Profil recherché": get_profile(soup),
            "Decsription d'entreprise": get_recruiter(soup),
            "Traits de personnalité": get_Personality_traits(soup),   
        }
        if not all(value is None for value in offer_data.values()):
            write_to_cache(job_url, web_site, logger)
        return offer_data
    except Exception as e:
        logger.error(f"The server {job_url} could not be reached. Error: {e}")
        return None

def scrape_page_offers(page_url):
    offers = []
    logger.info(f"start scraping the page {page_url}")
    try:
        html_content = get_html_content(page_url, logger)
        if html_content is None:
            raise Exception(f"Could not get the source page offers from: {page_url}")
        logger.info("The page was fetched successfully.")
        soup = BeautifulSoup(html_content, 'html.parser')
        job_tags = soup.find_all('li', class_='post-id')
        logger.info(f"Found {len(job_tags)} offer(s) to process.")
        if job_tags:
            for offer_tag in job_tags:
                offer_data = scrape_one_offer(offer_tag)
                offers.append(offer_data)
                if not offer_data:
                    break
            return offers
        else:
            raise Exception(f"There are no offer tags on the page: {page_url}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing the page: {page_url}. Error: {e}")
        return None

def get_pages() -> list:
    return [f"https://www.rekrute.com/offres.html?p={str(i)}" for i in range(1,1000)]

def fetch_and_yield_offers():
    try:
        stop_iteration = False
        logger.info(f"start scraping all the pages")
        for page_url in get_pages():
            if stop_iteration:
                break  # Exit the outer loop
            # logger.debug(f"start scraping the page: {page_url}")
            for offer_data in scrape_page_offers(page_url):
                if offer_data:
                    yield offer_data
                else:
                    stop_iteration = True
                    break  # Exit the inner loop
            logger.info(f"the page was scraped")
    except Exception as e:
        logger.error(f"Error occurred in scraping pages: {e}")

def run_scraper(frequency, schema):
    folder_raw_path = os.path.abspath(f"../data/raw/{frequency}/{web_site}/")
    folder_parquet_path = os.path.abspath(f"../data/parquet/{frequency}/{web_site}/")
    
    file__raw_path = f"{folder_raw_path}/{web_site}-{current_date_as_string()}.csv"
    file__parquet_path = f"{folder_parquet_path}/{web_site}-{current_date_as_string()}.csv"
    
    if not os.path.exists(folder_raw_path):
        os.makedirs(folder_raw_path)
        logger.info(f"creat the folder: {folder_raw_path}")
    
    if not os.path.exists(folder_parquet_path):
        os.makedirs(folder_parquet_path)
        logger.info(f"creat the folder: {folder_parquet_path}")
        
    
    if os.path.exists(file__raw_path) or os.path.exists(file__parquet_path):
        logger.error(f"make sure you delete {file__raw_path} and {file__parquet_path}")
        exit(0)

    spark = SparkSession.builder \
        .master("local")\
        .config("spark.driver.memory", "8g") \
        .appName(f"{web_site} scraper") \
        .getOrCreate()

    num_partitions = 8
    df = spark.createDataFrame([], schema=schema).repartition(num_partitions)
    for offer_data in fetch_and_yield_offers():
        if offer_data:
            current_df = spark.createDataFrame([offer_data], schema=schema)
            df = df.union(current_df)
        else:
            break
    logger.info(f"The End of scraping prossece")
    
    if not df.count():
        new_row_values = [0] + [None] * (len(df.columns) - 1)
        new_row = Row(*new_row_values)

        # Convert the Row to a DataFrame with the same schema as df
        new_row_df = spark.createDataFrame([new_row], schema=df.schema)

        # Append the new row DataFrame to the existing DataFrame df
        df = df.union(new_row_df)
            
    calculate_non_null_percentage(df, frequency, web_site)
    logger.info(f"Strat history saving")
    df = df.fillna('non spécifié')
    logger.info(f"END of history saving")
    
    pandas_df = df.toPandas()
    logger.info(f"start saving data to {file__raw_path}")
    for chunk in np.array_split(pandas_df, num_partitions):
        chunk.to_csv(file__raw_path, mode='a', index=False, header=True)

    logger.info(f"THE END of saving data to {file__raw_path}")
    logger.info(f"start saving data to {file__parquet_path}")
    df.write.mode("overwrite").parquet(file__parquet_path)
    logger.info(f"THE END of saving data to {file__parquet_path}")
    

    df.show()
    spark.stop()

def Reckrute_scraper(frequency: str):
    schema = StructType([
        StructField("", StringType(), True),
        StructField("Societés", StringType(), True),
        StructField("Postes", StringType(), True),
        StructField("Adresses", StringType(), True),
        StructField("Details de poste", StringType(), True),
        StructField("Date de publication", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Postes proposés", StringType(), True),
        StructField("Secteur d'activité", StringType(), True),
        StructField("Fonctions", StringType(), True),
        StructField("Expérience requise", StringType(), True),
        StructField("Niveau d’étude demandé", StringType(), True),
        StructField("Type de contrat proposé", StringType(), True),
        StructField("Profil recherché", StringType(), True),
        StructField("Decsription d'entreprise", StringType(), True),
        StructField("Traits de personnalité", StringType(), True),
    ])
    try:
        logger.info("start scraping")
        run_scraper(frequency, schema)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return

def main(frequency):
    Reckrute_scraper(frequency)

if __name__ == '__main__':
    frequency = "daily"
    logger = set_logger(web_site, frequency)
    main(frequency)
