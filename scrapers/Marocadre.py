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
web_site = "Marocadre"
nbr_of_offer = -1

def get_poste(tag: BeautifulSoup) -> str:
    try:
        poste_tag = tag.find('h4').a.text
        poste_title = remove_duplicate_whitespace(poste_tag)
        logger.info(f"poste title of the offer: {poste_title}")
        return poste_title
    except Exception as e:
        logger.warning(f"poste title of the offer not found. Error: {e}")
        return None

def get_business_sector(tag: BeautifulSoup) -> str:
    try:
        business_sector = tag.find("th", text="Secteur d'activités :")
        if not business_sector:
            business_sector = tag.find("th", text="Secteur d'activitÃ©s :")
        business_sector = business_sector.find_next_sibling("td")
        business_sector_formated = remove_duplicate_whitespace(business_sector.text)
        logger.info(f"Secteur d'activité: {business_sector_formated}")
        return business_sector_formated
    except Exception as e:
        logger.warning(f"Secteur d'activité of the offer not found. Error: {e}")
        return None

def get_pays(tag: BeautifulSoup) -> str:
    try:
        contry_tag = tag.find("th", text="Pays concerné :")
        if not contry_tag:
            contry_tag = tag.find("th", text="Pays concernÃ© :")
        contry_tag = contry_tag.find_next_sibling("td")
        contry_formated = remove_duplicate_whitespace(contry_tag.text)
        logger.info(f"pays concernés: {contry_formated}")
        return contry_formated
    except Exception as e:
        logger.warning(f"pays concernés of the offer not found. Error: {e}")
        return None

def get_company(tag: BeautifulSoup) -> str:
    try:
        title_tag = tag.find('p').a
        company_name = remove_duplicate_whitespace(title_tag.text)
        logger.info(f"Company name of the offer: {company_name}")
        return company_name
    except Exception as e:
        logger.warning(f"Company name of the offer not found. Error: {e}")
        return None

def get_date(tag: BeautifulSoup) -> str:
    try:
        date_div = tag.find('div', class_='liste-annonce-text-date')
        date_div_text = date_div.text.replace("Le : ", "")
        date = remove_duplicate_whitespace(date_div_text)
        logger.info(f"Date of the offer : {date}")
        return date
    except Exception as e:
        logger.warning(f"Date of the offer not found. Error: {e}")
        return None

def get_locations(tag: BeautifulSoup) -> str:
    try:
        location_tag = tag.p.find_all('span')[1]
        location = remove_duplicate_whitespace(location_tag.text)
        logger.info(f"Localité: {location}")
        return location
    except Exception as e:
        logger.warning(f"Localité: : of the offer not found. Error: {e}")
        return None

def get_categories(tag: BeautifulSoup) -> str:
    try:
        categories_tag = tag.find("th", text="Catégorie :")
        if not categories_tag:
            categories_tag = tag.find("th", text="CatÃ©gorie :")
        categories = categories_tag.find_next_sibling("td")
        business_sector_formated = remove_duplicate_whitespace(categories.text)
        logger.info(f"Catégorie : {business_sector_formated}")
        return business_sector_formated
    except Exception as e:
        logger.warning(f"Catégorie : of the offer not found. Error: {e}")
        return None

def get_regions(tag: BeautifulSoup) -> str:
    try:
        regions_tag = tag.find("th", text="Région :")
        if not regions_tag:
            regions_tag = tag.find("th", text="RÃ©gion :")
        regions = regions_tag.find_next_sibling("td")
        regions_formated = remove_duplicate_whitespace(regions.text)
        logger.info(f"Région : {regions_formated}")
        return regions_formated
    except Exception as e:
        logger.warning(f"Région : of the offer not found. Error: {e}")
        return None

def get_contrat(tag: BeautifulSoup) -> str:
    try:
        contrat = tag.find("th", text="Contrat :").find_next_sibling("td")
        contrat_formated = remove_duplicate_whitespace(contrat.text)
        logger.info(f"type de contrat   {contrat_formated}")
        return contrat_formated
    except Exception as e:
        logger.warning(f"type de contrat  of the offer not found. Error: {e}")
        return None

def get_experience_level(tag: BeautifulSoup) -> str:
    try:
        experience_tag = tag.find("th", text="Experience :")
        cexperience_formated = remove_duplicate_whitespace(experience_tag.text)
        logger.info(f"Experience : {cexperience_formated}")
        return cexperience_formated
    except Exception as e:
        logger.warning(f"Experience : of the offer not found. Error: {e}")
        return None

def get_fonction(tag: BeautifulSoup) -> str:
    try:
        fonction = tag.find("th", text="fonction").find_next_sibling("td")
        fonction_formated = remove_duplicate_whitespace(fonction.text)
        logger.info(f"type de contrat   {fonction_formated}")
        return fonction_formated
    except Exception as e:
        logger.warning(f"type de contrat  of the offer not found. Error: {e}")
        return None

def get_education_level(tag: BeautifulSoup) -> str:
    try:
        education_level = tag.find("th", text="DiplÃ´me :").find_next_sibling("td")
        if not education_level:
            education_level = tag.find("th", text="Diplôme :").find_next_sibling("td")
        education_level_formated = remove_duplicate_whitespace(education_level.text)
        logger.info(f"Formation :   {education_level_formated}")
        return education_level_formated
    except Exception as e:
        logger.warning(f"Formation :   of the offer not found. Error: {e}")
        return None

def get_languages_level(tag: BeautifulSoup) -> str:
    try:
        languages_level = tag.find("th", text="Langues :").find_next_sibling("td")
        languages_level = remove_duplicate_whitespace(languages_level.text)
        logger.info(f"Langues :  {languages_level}")
        return languages_level
    except Exception as e:
        logger.warning(f"Langues:   of the offer not found. Error: {e}")
        return None

def get_salary(tag: BeautifulSoup) -> str:
    try:
        Salaire = tag.find("th", text="Salaire :").find_next_sibling("td")
        Salaire = remove_duplicate_whitespace(Salaire.text)
        logger.info(f"Salaire :  {Salaire}")
        return Salaire
    except Exception as e:
        logger.warning(f"Salaire:   of the offer not found. Error: {e}")
        return None

def get_missions(tag: BeautifulSoup) -> str:
    try:
        missions_tag = tag.find('div', id="liste").find_all('ul')[0]
        missions = " ".join([m.text for m in missions_tag])
        missions = remove_duplicate_whitespace(missions)
        logger.info(f"missions :  {missions}")
        return missions
    except Exception as e:
        logger.warning(f"missions:   of the offer not found. Error: {e}")
        return None

def scrape_one_offer(offer_tag: BeautifulSoup) -> dict:
    global nbr_of_offer
    nbr_of_offer += 1
    try:
        titreJob_tag = offer_tag.find(class_ = "liste-annonce-text-desc").a
        if not titreJob_tag:
            raise Exception("There are no offer. Cannot get offers from the page.")
        job_url = f"https://marocadres.com/{titreJob_tag['href']}"
        if check_url_in_cache(job_url, web_site, logger):
            return None
        html_content = get_html_content(job_url, logger)
        if not html_content:
            raise Exception("could not get the source page pffers pages")
        logger.info(f"The offer page number [{nbr_of_offer}] was fetched successfully")
        soup = BeautifulSoup(html_content, 'html.parser')
        offer_data = {
            "": nbr_of_offer,
            "postes": get_poste(offer_tag), 
            "entreprises" : get_company(offer_tag),
            "date de publication": get_date(offer_tag),
            "localités": get_locations(offer_tag),
            "pays concernés" : get_pays(soup),
            "categories": get_categories(soup),
            "regions" : get_regions(soup),
            "contrats": get_contrat(soup),
            "experiences": get_experience_level(soup),
            "diplomes": get_education_level(soup),
            "langues": get_languages_level(soup) ,
            "salaires": get_salary(soup),
            "fonction" : get_fonction(soup),
            "Missions": get_missions(soup),
        }
        if not all(value is None for value in offer_data.values()):
            write_to_cache(job_url, web_site, logger)
        return offer_data
    except Exception as e:
        logger.error(f"The server {job_url} could not be reached. Error: {e}")
        return None
    
def get_pages() -> list:
    return ["https://marocadres.com/"]

def fetch_and_yield_offers():
    try:
        stop_iteration = False
        logger.info(f"start scraping all the pages")
        for page_url in get_pages():
            if stop_iteration:
                break  # Exit the outer loop
            logger.debug(f"start scraping the page: {page_url}")
            for offer_data in scrape_page_offers(page_url):
                if offer_data:
                    yield offer_data
                else:
                    stop_iteration = True
                    break  # Exit the inner loop
            logger.info(f"the page was scraped")
    except Exception as e:
        logger.error(f"Error occurred in scraping pages: {e}")
        



def scrape_page_offers(page_url):
    offers = []
    logger.info(f"start scraping the page {page_url}")
    try:
        html_content = get_html_content(page_url, logger)
        if html_content is None:
            raise Exception(f"Could not get the source page offers from: {page_url}")
        logger.info("The page was fetched successfully.")
        soup = BeautifulSoup(html_content, 'html.parser')
        job_tags = soup.find_all('div', class_='liste-annonce-text clearfix')
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
    # calculate_non_null_percentage(df, frequency, web_site)
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

def Marocadre_scraper(frequency: str):
    schema = StructType([
        StructField("", StringType(), True),
        StructField("postes", StringType(), True),
        StructField("entreprises", StringType(), True),
        StructField("date de publication", StringType(), True),
        StructField("localités", StringType(), True),
        StructField("pays concernés", StringType(), True),
        StructField("categoriess", StringType(), True),
        StructField("regions", StringType(), True),
        StructField("contrats", StringType(), True),
        StructField("experiences", StringType(), True),
        StructField("diplomes", StringType(), True),
        StructField("langues", StringType(), True),
        StructField("salaires", StringType(), True),
        StructField("fonction", StringType(), True),
        StructField("Missions", StringType(), True),
    ])
    try:
        logger.info("start scraping")
        run_scraper(frequency, schema)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return

def main(frequency):
    Marocadre_scraper(frequency)

if __name__ == '__main__':
    frequency = "daily"
    logger = set_logger(web_site, frequency)
    main(frequency)
