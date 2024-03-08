from utility import *
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

current_working_dir = './'
logger = None

def get_adresses(tag: BeautifulSoup) -> str:
    try:
        location = tag.find('span', id='address').text.strip()
        logger.info(f"Adresses of the offer: {location}")
        return check_value_data(location, 'location', logger)
    except Exception as e:
        logger.warning(f"Location of the offer not found. Error: {e}")
        return missing_value

def get_poste(tag: str) -> str:
    try:
        title_text = tag.find('title').string
        job_title = title_text.split('-')[0].strip()
        job_title = job_title.replace("Offre  d'emploi ", "")
        logger.info(f"job title of the offer: {job_title}")
        return check_value_data(job_title, 'job title', logger)
    except Exception as e:
        logger.warning(f"job title of the offer not found. Error: {e}")
        return None


def get_company(tag: BeautifulSoup) -> str:
    try:
        title_tag = tag.find('title')
        title_text = title_tag.string
        company_name = title_text.split('-')[-1].strip()
        logger.info(f"Company name of the offer: {company_name}")
        return check_value_data(company_name, 'company', logger)
    except Exception as e:
        logger.warning(f"Company name of the offer not found. Error: {e}")
        return None

def get_contrat(tag: BeautifulSoup) -> str:
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

def get_salary(tag: BeautifulSoup) -> str:
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

def get_recruiter(tag: BeautifulSoup) -> str:
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
    except AttributeError:
        logger.warning(f"Recruiter information of the offer not found. Error: {e}")
    except Exception as e:
        logger.warning(f"Recruiter information cant scraped. Error: {e}")
        return missing_value

def get_date(tag: BeautifulSoup) -> str:
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

def get_profile(tag: BeautifulSoup) -> str:
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
    except AttributeError:
        logger.warning(f"Profile of the offer not found")
    except Exception as e:
        logger.warning(f"Profile information cant scraped. Error: {e}")
        return missing_value

def get_missions(tag: BeautifulSoup) -> str:
    try:
        h2_tag = tag.find('h2', string='Poste :')
        missions = h2_tag.parent.get_text(strip=True)
        logger.info(f"Details de poste: {missions}")
        return check_value_data(missions, 'Details de poste:', logger)
    except Exception as e:
        logger.warning(f"Details de post of the offer not found. Error: {e}")
        return None
        

def get_business_sector(tag: BeautifulSoup) -> str:
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
    except AttributeError:
        logger.warning(f"Business of the offer not found")
    except Exception as e:
        logger.warning(f"Business information cant scraped. Error: {e}")
        return missing_value

def get_education_level(tag: BeautifulSoup) -> str:
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
    except AttributeError:
        logger.warning(f"Education level of the offer not found")
    except Exception as e:
        logger.warning(f"Education level information cant scraped. Error: {e}")
        return missing_value

def get_job(tag: BeautifulSoup) -> str:
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
    except AttributeError:
        logger.warning(f"Job level of the offer not found.")
    except Exception as e:
        logger.warning(f"Job level information cant scraped. Error: {e}")
        return missing_value

def get_experience_level(tag: BeautifulSoup) -> str:
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
    except AttributeError:
        logger.warning(f"Experience level of the offer not found")
    except Exception as e:
        logger.warning(f"Experience level of the offer not found. Error: {e}")
        return missing_value

def get_languages_level(tag: BeautifulSoup) -> str:
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
    except AttributeError:
        logger.warning(f"Languages level of the offer not found")
    except Exception as e:
        logger.warning(f"Languages level of the offer not found. Error: {e}")
        return missing_value

def scrape_one_offer(offer_url: str) -> dict:
    """
    Scrapes job offer information from a given URL.

    Args:
        offer_url (str): The URL of the job offer page.
        logger (logging.Logger): Logger object for logging messages.

    Returns:
        dict: A dictionary containing various job offer information.
    """
    try:
        html_content = get_html_content(offer_url, logger)
        logger.info(f"The server could get the offer source page succesfuly")
    except Exception as e:
        logger.error(f"The server {offer_url} could not be found. Error: {e}")
        return

    # html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    # print(soup)
    return {
        # ,Societés,Postes,Adresses,Details de poste,Date de publication,Location,Postes proposés,Secteur d'activité,Fonctions,Expérience requise,Niveau d’étude demandé,Type de contrat proposé,Profil recherché,Decsription d'entreprise,Traits de personnalité
        "Societés": get_company(soup),
        "Postes": get_poste(soup),
        "Adresses": get_adresses(soup),
        "Details de poste" : get_missions(soup),
        # "Secteur d'activité": get_business_sector(soup),
        # "Locations": get_locations(soup),
        # "Dates": get_date(soup),
        # "Contrats": get_contrat(soup),
        # "Profils": get_profile(soup),
        # "Details entreprises": get_recruiter(soup),
        # "Salaire": get_salary(soup),
        # "Metiers": get_job(soup),
        # "Niveau d'expérience": get_experience_level(soup),
        # "Niveau d'études": get_education_level(soup),
        # "Langues exigées": get_languages_level(soup)
    }

def extract_offer_links(page_url):
    try:
        html_content = get_html_content(page_url, logger)
        # logger.debug(f"html_content: {html_content}")÷
        # if not html_content:
        #     return None
        logger.info(f"The page was fetched successfully")
    except Exception as e:
        logger.error(f"The server {page_url} could not be reached. Error: {e}")
        return None
    soup = BeautifulSoup(html_content, 'html.parser')
    try:
        titreJobs = soup.find_all('a', class_='titreJob')
        logger.info(f"Extract {len(titreJobs)} titreJob from the page")
        if not titreJobs:
            raise Exception("There are no divs with a tag name [titreJob]. Cannot get offers from the page.")
    except Exception as e:
        logger.error(f"We could not fetch offers from this page {page_url}. Error: {e}")
        return None
    
    try:
        for offer_box in titreJobs:
            offer_url = f"https://www.rekrute.com{offer_box['href']}"
            if not offer_url:
                raise Exception("There is no URL in the offers section. Cannot get the offer link page.")
            yield offer_url
    except Exception as e:
        logger.error(f"Error while getting url offers: {e}")
    logger.info(f"Extract the link of an offer: {offer_url}")

def get_pages():
    # return [f"http://www.m-job.ma/recherche?page=" + str(i) for i in range(1000)]
    # return [f"https://www.rekrute.com/offres.html?s=2&p={str(i)}&o=1" for i in range(1000)]
    # return [f"https://www.rekrute.com/offres.html?s=2&p={str(i)}&o=1" for i in range(1)]
    return [f"https://www.rekrute.com/offres.html?p={str(i)}" for i in range(1,2)]


def fetch_and_yield_offers(site_name):
    """
    Scrapes data for job offers from a single page URL.
    Args:
        page_url (str): The URL of the page containing job offers.
        logger (logging.Logger): Logger object for logging messages.
    Yields:
        dict: A dictionary containing job offer information scraped from each offer on the page.
    """

    nbr_of_offer = 0
    try:
        for page_url in get_pages():
            logger.debug(f"start scraping the page: {page_url}")
            for url_offer in extract_offer_links(page_url):
                logger.debug(f"start fitching data for this offer: {url_offer}")
                # if not url_offer or already_scraperd(url_offer, site_name, logger):
                #     return None
                offer_data = scrape_one_offer(url_offer)
                print(f"offer_data: {offer_data}")
                indexed_data = {"": nbr_of_offer}
                indexed_data.update(offer_data)
                yield indexed_data
                exit(0)
                nbr_of_offer += 1
    except Exception as e:
        logger.error(f"Error occurred in scraping pages: {e}")

def run_scraper(site_name, frequency, schema):
    spark = SparkSession.builder \
        .appName(f"{site_name} scraper") \
        .getOrCreate()
    df = spark.createDataFrame([], schema=schema)
    for offer_data in fetch_and_yield_offers(site_name):
        if offer_data:
            current_df = spark.createDataFrame([offer_data], schema=schema)
            df = df.union(current_df)
    if len(df):
        df.write.mode("overwrite").csv(f"../data/parquet/{frequency}/{site_name}/{site_name}parquet-{current_date_as_string()}.csv", header=True)
        pandas_df = df.toPandas()
        pandas_df.to_csv(f"../data/raw/{frequency}/{site_name}/{site_name}-{current_date_as_string()}.csv", index=False)
    # # else:
    #     generate an empty array
    df.show()
    spark.stop()
    # return (calcu)

# sef statistic()
    # date,site_name,Postes,Secteur d'activité,Locations,Dates,Societes,Contrats,Profils,Missions,Details entreprises,Salaire,Metiers,Niveau d'expérience,Niveau d'études,Langues exigées

# def statistic2:
#   date, site, _nuber_of_scraped_offer, number_of_corrupted_data,

# montly_detay
# date, site, _nuber_of_scraped_offer, number_of_corrupted_data,

def Reckrute_scraper(site_name: str, frequency: str = "daily"):
    """
    Scrapes job data from a specified website and URL, structures the data according to a predefined schema, and updates historical records.

    Parameters:
    - url (str): The URL from which to scrape the job data.
    - web_site (str): Identifier for the website being scraped.

    Returns:
    None. The function stores the scraped data and updates historical records.
    """
    # Define schema
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
        # Set up logger
        global logger
        logger = set_logger(site_name, frequency)
        logger.info("start scraping")
        dict_result = run_scraper(site_name, frequency, schema)

        # print(f"dict_result = {dict_result}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return

    # store_site_history(dict_result, web_site)
    # store_aggregate_history(dict_result, web_site)
    # monthly_summary_update(dict_result, web_site)


def main(frequency):
    web_site = "Reckrute"
    Reckrute_scraper(web_site, frequency)

if __name__ == '__main__':
    # main(frequency = 'daily')
    logger = set_logger("rekrute", "dayly")
    
    offer_url = "https://www.rekrute.com/offre-emploi-technicien-maintenance-automaticien-senior-recrutement-crouzet-casablanca-159053.html"
    # offer_url = "https://www.rekrute.com/offre-emploi-technicien-maintenance-automaticien-senior-recrutement-crouzet-casablanca-159053.html"
    s = scrape_one_offer(offer_url)
    print(s)