import os
import re
import logging
import requests
from datetime import datetime

missing_value = 'non spécifié'
logger = None

def current_date_as_string() -> str:
    """
    Returns the current date as a string in the format "YYYY-MM-DD".
    Returns:
        str: The current date in the format "YYYY-MM-DD".
    """
    return datetime.now().strftime('%Y-%m-%d')

def already_scraperd(url_offer, site_name, logger):
    """
    Checks if a given URL has already been scraped and stored in a cache file specific to a site.
    If the URL is new, it is added to the file. The function also ensures that the file does not
    exceed 100 lines. If it does, the oldest entry is removed (FIFO). If the cache file or directory
    does not exist, they are created.

    Parameters:
    - url_offer (str): The URL to check or add to the cache.
    - site_name (str): The name of the site, used to name the cache file.
    - logger (logging.Logger): A logger instance for logging important actions and events.

    Returns:
    - bool: True if the URL was already in the cache, False if it was added to the cache.

    Notes:
    - The cache directory is named '.caches' and located in the current working directory.
    - Each site has its own cache file named after the site within the cache directory.
    - This function uses a simple form of cache management to limit the file size.
    """
    
    # Ensure the cache directory exists, create if not
    cache_dir = "./.caches"
    os.makedirs(cache_dir, exist_ok=True)
    
    # Construct the path to the cache file for the given site
    file_path = f"{cache_dir}/.{site_name}.txt"
    
    try:
        # Open the file to read current lines, 'r+' mode to read and write
        with open(file_path, 'r+') as file:
            lines = file.readlines()
            # Check if the URL is already cached
            if url_offer + '\n' in lines:
                logger.info(f"URL already scraped: {url_offer}")
                return True
            else:
                # If more than 100 lines, remove the oldest
                if len(lines) >= 100:
                    logger.info("Maximum cache size reached. Removing the oldest entry.")
                    lines = lines[1:]
                
                # Add the new URL to the cache
                lines.append(url_offer + '\n')
                file.seek(0)
                file.writelines(lines)
                logger.info(f"Added new URL to cache: {url_offer}")
                return False
    except FileNotFoundError:
        # If the file doesn't exist, create it and add the URL
        with open(file_path, 'w') as file:
            file.write(url_offer + '\n')
            logger.info(f"Cache file created and URL added: {url_offer}")
        return False


def set_logger(site_name:str, frequency:str) -> logging.Logger:
    """
    Configures and returns a logger for a web scraping job.
    Returns:
        logging.Logger: A configured logger object for logging scraping activities.
    Notes:
        This function sets up logging for a web scraping job, providing both a file handler and a console handler.
        It configures the logging level to DEBUG, allowing logging of all messages.
        The log messages include the timestamp, logging level, and the message it
    """
    logger = logging.getLogger(f'scraping_{site_name}')
    logger.setLevel(logging.DEBUG)  # Set the logging level for the logger
    # Define log file path
    log_file_path = f'../logs/scrapers/{frequency}/{site_name}/{current_date_as_string()}-{site_name}.log'
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    # Create file handler which logs even debug messages
    fh = logging.FileHandler(log_file_path)
    fh.setLevel(logging.DEBUG)  # Set logging level for the file handler
    # Create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)  # Set logging level for the console handler
    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def check_value_data(data: str, field: str, logger) -> str:
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
    # Remove multiple spaces, tabs, and newline characters
    cleaned_data = re.sub(r'\s+', ' ', data)
    return cleaned_data

# def get_html_content(url, logger):
#     try:
#         response = requests.get(url)
#         if response.status_code == 200:
#             logger.info(f"sucsued to retrieve HTML content. Status code: {response.status_code}")
#             return response.text
#         else:
#             logger.warning(f"Failed to retrieve HTML content. Status code: {response.status_code}")
#             return None
#     except requests.exceptions.RequestException as e:
#         logger.error(f"An error occurred: {e}")
#         return None

def get_html_content(url, logger):
    # Mimic a browser's User-Agent
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logger.info(f"Successfully retrieved HTML content. Status code: {response.status_code}")
            return response.text
        else:
            logger.warning(f"Failed to retrieve HTML content. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred: {e}")
        return None