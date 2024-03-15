import logging
import requests

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

# Create logger
logger = logging.getLogger('html_logger')
logger.setLevel(logging.DEBUG)

# Create console handler and set level to INFO
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Add formatter to ch
ch.setFormatter(formatter)

# Add ch to logger
logger.addHandler(ch)

# Test the function
html_content = get_html_content("https://www.rekrute.com/offres.html?p=1", logger)
print(html_content)
