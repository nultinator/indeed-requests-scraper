import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode, urlparse, parse_qs
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(keyword, location, locality, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    foramtted_location = locality.replace(" ", "+")
    url = f"https://www.indeed.com/jobs?q={formatted_keyword}&l={foramtted_location}"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code == 200:
                success = True
            
            else:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
                ## Extract Data

            soup = BeautifulSoup(response.text, "html.parser")
            
            div_cards = soup.select("div[data-testid='slider_item']")


            for div_card in div_cards:

                name = div_card.select_one("h2").text

                parsed_url = urlparse(div_card.find("a").get("href"))
                query_params = parse_qs(parsed_url.query)
                has_job_key = "jk" in query_params.keys()
                if not has_job_key:
                    continue
                job_key = query_params["jk"][0]
                url = f"https://www.indeed.com/viewjob?jk={job_key}"                

                company_name = div_card.select_one("span[data-testid='company-name']").text
                
                rating = None
                rating_holder = div_card.select_one("span[data-testid='holistic-rating']")
                if rating_holder:
                    rating = rating_holder.text

                location = div_card.select_one("div[data-testid='text-location']").text

                
                search_data = {
                    "name": name,
                    "url": url,
                    "stars": rating,
                    "company_name": company_name,
                    "location": location
                }

                print(search_data)
                

            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")



if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "us"
    LOCALITY = "Westland MI"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["writer"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        scrape_search_results(keyword, LOCATION, LOCALITY, retries=MAX_RETRIES)
        
    logger.info(f"Crawl complete.")

    