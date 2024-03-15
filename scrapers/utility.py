import os
import re
import sys
import csv
import logging
import requests
import pandas as pd
from functools import reduce
from datetime import datetime, timedelta
from pyspark.sql.functions import col

def current_date_as_string() -> str:
    """
    Returns the current date as a string in the format "YYYY-MM-DD".
    Returns:
        str: The current date in the format "YYYY-MM-DD".
    """
    return datetime.now().strftime('%Y-%m-%d')

def remove_duplicate_whitespace(text: str) -> str:
    """
    Removes duplicate whitespace from the given string and replaces it with a single space, using regular expressions.

    Args:
        text (str): The input string from which duplicate whitespace will be removed.

    Returns:
        str: The modified string with all duplicate whitespace replaced by a single space.
    """
    return re.sub(r'\s+', ' ', text)

def check_url_in_cache(url_offer, web_site, logger):
    """
    Checks if a given URL has already been scraped and stored in a cache file specific to a site.

    Parameters:
    - url_offer (str): The URL to check in the cache.
    - web_site (str): The name of the site, used to name the cache file.
    - logger (logging.Logger): A logger instance for logging important actions and events.

    Returns:
    - bool where the first element is True if the URL was already in the cache, 
             False otherwise.
    """
    cache_dir = "./.caches"
    file_path = f"{cache_dir}/{web_site}.csv"

    try:
        # Check if the cache file exists
        if not os.path.exists(file_path):
            return False
        
        # Check if the URL is in the cache
        with open(file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            for row in reader:
                if row[1] == url_offer:
                    logger.info(f"URL already scraped: {url_offer}")
                    return True
            logger.info(f"URL never scraped: {url_offer}")
            return False
    except Exception as e:
        logger.error(f"Something went wrong in check_url_in_cache: {e}")
        return False

def write_to_cache(url_offer, web_site, logger):
    """
    Writes a new URL to the cache or updates the cache if it already exists. Ensures the cache file does not exceed 100 lines.
    If the cache directory or file does not exist, they are created.

    Parameters:
    - url_offer (str): The URL to add to the cache.
    - web_site (str): The name of the site, used to name the cache file.
    - logger (logging.Logger): A logger instance for logging important actions and events.
    """
    cache_dir = "./.caches"
    os.makedirs(cache_dir, exist_ok=True)  # Ensure the cache directory exists
    file_path = f"{cache_dir}/{web_site}.csv"
    max_age = datetime.now() - timedelta(days=7)

    try:
        # Create or update the cache file
        with open(file_path, 'a+', newline='') as file:
            reader = csv.reader(file)
            lines = list(reader)

            # Remove lines older than a week
            lines = [line for line in lines if datetime.strptime(line[0], '%Y-%m-%d %H:%M:%S') > max_age]

            # Manage cache size: if more than 100 lines, remove the oldest
            if len(lines) >= 100:
                logger.info("Maximum cache size reached. Removing the oldest entry.")
                lines = lines[1:]

            # Add the new URL to the cache
            lines.append([datetime.now().strftime('%Y-%m-%d %H:%M:%S'), url_offer])
            
            # Write updated lines to the file
            file.seek(0)
            writer = csv.writer(file)
            writer.writerows(lines)
            logger.info(f"Added new URL to cache: {url_offer}")
    except Exception as e:
        logger.error(f"Something went wrong in write_to_cache: {e}")

def set_logger(web_site: str, frequency: str) -> logging.Logger:
    logger = logging.getLogger(f'scraping_{web_site}')
    logger.setLevel(logging.DEBUG)  # Set the logging level for the logger
    
    # Define log file path
    folder_path = os.path.abspath(f"../logs/scrapers/{frequency}/{web_site}/")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path, exist_ok=True)
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_file_path = f'{folder_path}/{current_date}-{web_site}.log'
    print(f"for more info check: {log_file_path}")
    
    sys.stdout = open(f'{log_file_path}', 'a')
    sys.stderr = open(f'{log_file_path}', 'a')
    
    # Create file handler which logs even debug messages
    # fh = logging.FileHandler(log_file_path)
    # fh.setLevel(logging.DEBUG)  # Set logging level for the file handler
    
    # Create console handler which logs all messages (default level is INFO)
    ch = logging.StreamHandler()
    
    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    # fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    # Add the handlers to the logger
    # logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger

def get_html_content(url, logger):
    # Mimic a browser's User-Agent
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logger.info(f"Successfully retrieved HTML content. Status code: {response.status_code}")
            # encoding = response.encoding  # Get the encoding from the response
            # logger.info(f"Successfully retrieved HTML content. Status code: {response.status_code}")
            # return response.content.decode(encoding)  # Decode content using the detected encoding
            encoding = response.encoding  # Get the encoding from the response
            logger.info(f"Encoding detected: {encoding}")
            return response.content.decode(encoding, errors='ignore')  # Decode content using the detected encoding
        else:
            logger.warning(f"Failed to retrieve HTML content. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred: {e}")
        return None

def calculate_non_null_percentage(df, frequency, web_site):
    total_rows = df.count()
    
    null_rows = df.filter(
        reduce(lambda x, y: x & y, (col(c).isNull() for c in df.columns))
    )
       
    non_null_percentage_dict = {"date" : current_date_as_string(),
                                "frequency": frequency ,
                                "number of offers": total_rows,
                                "corrapted_rows": null_rows.count()}

    for column in df.columns:
        if column != "":  # Skip columns with an empty string as name
            non_null_count = df.where(col(column).isNotNull()).count()
            non_null_percentage = (non_null_count / total_rows) * 100
            if non_null_percentage > 0:  # Only add to dictionary if percentage is non-zero
                non_null_percentage_dict[column] = non_null_percentage

    output_folder = os.path.abspath("../history/")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_file = f"{output_folder}/{web_site}.csv"
    file_exists = os.path.isfile(output_file)
    
    # Write dictionary to CSV file
    with open(output_file, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        if not file_exists:  # If the file doesn't exist, write keys
            csvwriter.writerow(non_null_percentage_dict.keys())
        csvwriter.writerow(non_null_percentage_dict.values())
        