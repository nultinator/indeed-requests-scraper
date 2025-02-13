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



@dataclass
class SearchData:
    name: str = ""
    url: str = ""
    stars: float = None
    company_name: str = ""
    location: str = ""

    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            # Check string fields
            if isinstance(getattr(self, field.name), str):
                # If empty set default text
                if getattr(self, field.name) == "":
                    setattr(self, field.name, f"No {field.name}")
                    continue
                # Strip any trailing spaces, etc.
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())

class DataPipeline:
    
    def __init__(self, csv_filename="", storage_queue_limit=50):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.csv_filename = csv_filename
        self.csv_file_open = False
    
    def save_to_csv(self):
        self.csv_file_open = True
        data_to_save = []
        data_to_save.extend(self.storage_queue)
        self.storage_queue.clear()
        if not data_to_save:
            return

        keys = [field.name for field in fields(data_to_save[0])]
        file_exists = os.path.isfile(self.csv_filename) and os.path.getsize(self.csv_filename) > 0
        with open(self.csv_filename, mode="a", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)

            if not file_exists:
                writer.writeheader()

            for item in data_to_save:
                writer.writerow(asdict(item))

        self.csv_file_open = False
                    
    def is_duplicate(self, input_data):
        if input_data.name in self.names_seen:
            logger.warning(f"Duplicate item found: {input_data.name}. Item dropped.")
            return True
        self.names_seen.append(input_data.name)
        return False
            
    def add_data(self, scraped_data):
        if self.is_duplicate(scraped_data) == False:
            self.storage_queue.append(scraped_data)
            if len(self.storage_queue) >= self.storage_queue_limit and self.csv_file_open == False:
                self.save_to_csv()
                       
    def close_pipeline(self):
        if self.csv_file_open:
            time.sleep(3)
        if len(self.storage_queue) > 0:
            self.save_to_csv()



def scrape_search_results(keyword, location, locality, page_number, data_pipeline=None, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    foramtted_location = locality.replace(" ", "+")
    url = f"https://www.indeed.com/jobs?q={formatted_keyword}&l={foramtted_location}&start={page_number * 10}"
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

                
                search_data = SearchData(
                    name=name,
                    url=url,
                    stars=rating,
                    company_name=company_name,
                    location=location
                )
                

                data_pipeline.add_data(search_data)
            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")




def start_scrape(keyword, pages, location, locality, data_pipeline=None, retries=3):
    for page in range(pages):
        scrape_search_results(keyword, location, locality, page, data_pipeline=data_pipeline, retries=retries)


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

        crawl_pipeline = DataPipeline(csv_filename=f"{filename}.csv")
        start_scrape(keyword, PAGES, LOCATION, LOCALITY, data_pipeline=crawl_pipeline, retries=MAX_RETRIES)
        crawl_pipeline.close_pipeline()
        aggregate_files.append(f"{filename}.csv")
    logger.info(f"Crawl complete.")

    