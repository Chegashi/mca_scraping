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
web_site = "Anapec"
nbr_of_offer = -1

def get_poste(tag: BeautifulSoup) -> str:
    try:
        poste_tag = tag.find_all('td')[3]
        poste_title = remove_duplicate_whitespace(poste_tag.text)
        logger.info(f"poste title of the offer: {poste_title}")
        return poste_title
    except Exception as e:
        logger.warning(f"poste title of the offer not found. Error: {e}")
        return None

def get_business_sector(tag: BeautifulSoup) -> str:
    try:
        business_sector = tag.find("span", text="Secteur d’activité  :").find_next_sibling("span")
        business_sector_formated = remove_duplicate_whitespace(business_sector.text)
        logger.info(f"Secteur d'activité: {business_sector_formated}")
        return business_sector_formated
    except Exception as e:
        logger.warning(f"Secteur d'activité of the offer not found. Error: {e}")
        return None

def get_number_of_offers(tag: BeautifulSoup) -> str:
    try:
        nbr_poste_tag = tag.find_all('td')[4]
        nbr_poste = remove_duplicate_whitespace(nbr_poste_tag.text)
        logger.info(f"number of poste of the offer: {nbr_poste}")
        return nbr_poste
    except Exception as e:
        logger.warning(f"poste title of the offer not found. Error: {e}")
        return None

def get_date(tag: BeautifulSoup) -> str:
    try:
        date_div = tag.find('strong', text='Date : ').find_next_sibling(text=True)
        date = remove_duplicate_whitespace(date_div.text)
        logger.info(f"Date of the offer : {date}")
        return date
    except Exception as e:
        logger.warning(f"Date of the offer not found. Error: {e}")
        return None

def get_start_date(tag: BeautifulSoup) -> str:
    try:
        start_date = tag.find("span", text="Date de début  : ").find_next_sibling("span")
        business_sector_formated = remove_duplicate_whitespace(start_date.text)
        logger.info(f"Date de début  :  {business_sector_formated}")
        return business_sector_formated
    except Exception as e:
        logger.warning(f"Date de début  :  of the offer not found. Error: {e}")
        return None

def get_locations(tag: BeautifulSoup) -> str:
    try:
        location = tag.find("span", text="Lieu de travail :").find_next_sibling("span")
        location_formated = remove_duplicate_whitespace(location.text)
        logger.info(f"Lieu de travail :  {location_formated}")
        return location_formated
    except Exception as e:
        logger.warning(f"Lieu de travail : of the offer not found. Error: {e}")
        return None

def get_contrat(tag: BeautifulSoup) -> str:
    try:
        contract = tag.find("span", text="Type de contrat : ").find_next_sibling("span")
        contract_formated = remove_duplicate_whitespace(contract.text)
        logger.info(f"type de contrat   {contract_formated}")
        return contract_formated
    except Exception as e:
        logger.warning(f"type de contrat  of the offer not found. Error: {e}")
        return None

def get_profile(tag: BeautifulSoup) -> str:
    try:
        profil = tag.find("span", text="Caractéristiques du poste : ").find_next_sibling("span")
        profil_formated = remove_duplicate_whitespace(profil.text)
        logger.info(f"Description du profil :  {profil_formated}")
        return profil_formated
    
    except Exception as e:
        logger.warning(f"Description du profil :  of the offer not found. Error: {e}")
        return None

def get_education_level(tag: BeautifulSoup) -> str:
    try:
        
        education_level = tag.find("span", text="Formation : ").find_next_sibling()
        education_level_formated = remove_duplicate_whitespace(education_level.text)
        logger.info(f"Formation :   {education_level_formated}")
        return education_level_formated
    
    except Exception as e:
        logger.warning(f"Formation :   of the offer not found. Error: {e}")
        return None

def get_experience_level(tag: BeautifulSoup) -> str:
    try:
        experience = tag.find("span", text=" Expérience professionnelle  : ").find_next_sibling("span")
        experience_formated = remove_duplicate_whitespace(experience.text)
        logger.info(f" Expérience professionnelle:  {experience_formated}")
        return experience_formated
    
    except Exception as e:
        logger.warning(f" Expérience professionnelle  :   of the offer not found. Error: {e}")
        return None

def get_languages_level(tag: BeautifulSoup) -> str:
    try:
        # Find the <p> tag containing the string "Langues :"
        langues_tag = tag.find('p', string="Langues :")

        # If the tag is found, find the next <p> tag to extract the values
        next_p_tag = langues_tag.find_next_sibling('p')
        if next_p_tag:
            # Extract the text from the next <p> tag
            langues_text = next_p_tag.get_text(separator=', ').replace('<br>', ', ')
            languages_level = remove_duplicate_whitespace(langues_text)
            logger.info(f"Langues :  {languages_level}")
            return languages_level
    except Exception as e:
        logger.warning(f"Langues:   of the offer not found. Error: {e}")
        return None

def get_Personality_traits(tag: BeautifulSoup) -> str:
    try:
        Personality_traits = tag.find("span", text=" Compétences spécifiques : ").find_next_sibling("span")
        Personality_traits_formated = remove_duplicate_whitespace(Personality_traits.text)
        logger.info(f"Compétences spécifiques : {Personality_traits_formated}")
        return Personality_traits_formated
    
    except Exception as e:
        logger.warning(f"Compétences spécifiques ::  of the offer not found. Error: {e}")
        return None
    
def scrape_one_offer(offer_tag: BeautifulSoup) -> dict:
    global nbr_of_offer
    nbr_of_offer += 1
    try:
        titreJob_tag = offer_tag.find_all('td')[1].a
        if not titreJob_tag:
            raise Exception("There are no td tag that have offer in the first column. Cannot get offers from the page.")
        job_url = f"http://www.anapec.org{titreJob_tag['href']}"
        if check_url_in_cache(job_url, web_site, logger):
            return None
        html_content = get_html_content(job_url, logger)
        if not html_content:
            raise Exception("could not get the source page pffers pages")
        logger.info(f"The offer page number [{nbr_of_offer}] was fetched successfully")
        soup = BeautifulSoup(html_content, 'html.parser')
        offer_data = {
            "": nbr_of_offer,
            "Postes": get_poste(offer_tag), 
            "Secteurs": get_business_sector(soup),
            "nombre de postes": get_number_of_offers(offer_tag),
            "date publication": get_date(soup),
            "date de debut": get_start_date(soup),
            "lieux": get_locations(soup),
            "contrats": get_contrat(soup),
            "description de profil" : get_profile(soup),
            "formations": get_education_level(soup),
            "Expérience professionnelle": get_experience_level(soup),
            "langues": get_languages_level(soup) ,
            "competences spécifiques": get_Personality_traits(soup),
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
        job_tags = soup.find('table', id='myTable').tbody.find_all('tr')
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
    return [f"http://anapec.org/sigec-app-rv/chercheurs/resultat_recherche/page:{str(i)}/tout:all/language:fr" for i in range(1,1000)]

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
            
    logger.info(f"Strat history saving")
    calculate_non_null_percentage(df, frequency, web_site)
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

def Anapec_scraper(frequency: str):
    schema = StructType([
        StructField("", StringType(), True),
        StructField("Postes", StringType(), True),
        StructField("Secteurs", StringType(), True),
        StructField("nombre de postes", StringType(), True),
        StructField("date publication", StringType(), True),
        StructField("date de debut", StringType(), True),
        StructField("lieux", StringType(), True),
        StructField("contrats", StringType(), True),
        StructField("description de profil", StringType(), True),
        StructField("formations", StringType(), True),
        StructField("Expérience professionnelle", StringType(), True),
        StructField("langues", StringType(), True),
        StructField("competences spécifiques", StringType(), True),
    ])
    try:
        logger.info("start scraping")
        run_scraper(frequency, schema)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return

def main(frequency):
    Anapec_scraper(frequency)

if __name__ == '__main__':
    frequency = "daily"
    logger = set_logger(web_site, frequency)
    main(frequency)
