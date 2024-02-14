from urllib.parse import urljoin
from typing import List, Union
from bs4 import BeautifulSoup
import xmltodict
import requests
import logging
import time
import json
import gzip
import os

logging.basicConfig(
    filename='log.log',
    filemode='w',
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    )

def timed(func: callable) -> callable:
    def time_wrapper(*args, **kwargs):
        logging.info('-' * 50) if log else print('-' * 50)
        logging.info(f'Starting {func.__name__}') if log else print(f'Starting {func.__name__}')
        logging.info('-' * 50) if log else print('-' * 50)
        start = time.time()
        func(*args, **kwargs)
        end = time.time()
        logging.info('-' * 50) if log else print('-' * 50)
        logging.info(f'{func.__name__} time: {float(end - start):.2f} seconds') if log else print(f'{func.__name__} time: {float(end - start):.2f} seconds')
        logging.info('-' * 50) if log else print('-' * 50)
    return time_wrapper

log = True
@timed
def main(categories: List[str], limit: Union[int, None] = None) -> None:
    if os.path.exists('stage'):
        for file in os.listdir('stage'):
            os.remove(os.path.join('stage', file))
    else:
        os.makedirs('stage')

    """
    Scrape the FTP server for the PubMed baseline data, then turn that data into soup.
    """
    pubmed_baseline = 'https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/'
    response = requests.get(pubmed_baseline)
    soup = BeautifulSoup(response.text, 'html.parser')

    file_count = len(list(i for i in soup.find_all('a', href=True) if i['href'].endswith('.gz')))
    downloaded_count = 0
    data_dict = {category: {} for category in categories}

    """
    Find all the 'a' tags in the soup, then filter out the ones that end with .gz. These are the files we want to download.
    """
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.endswith('.gz'):
            """
            If the file ends with .gz, download it to the stage, unzip the contents, then delete the .gz file.
            """
            downloaded_count += 1
            logging.info(f'FILE {downloaded_count} of {limit if limit else file_count}')
            file_url = urljoin(pubmed_baseline, href)
            file_name = os.path.join('stage', os.path.basename(href))

            with requests.get(file_url, stream=True) as file_response:
                file_response.raise_for_status()

                with open(file_name, 'wb') as gz_file:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        gz_file.write(chunk)

            with gzip.open(file_name, 'rb') as zipped_file:
                unzipped_content = zipped_file.read()
                unzipped_file_name = file_name[:-3]
                with open(unzipped_file_name, 'wb') as unzipped_file:
                    unzipped_file.write(unzipped_content)
            os.remove(file_name)
            logging.info(f'Unzipped and deleted {file_name}')

            
            """
            Now that the file is unzipped, parse the XML and extract the relevant data.
            """
            logging.info(f'Processing {unzipped_file_name}...')
            parse_start = time.time()
            data = xmltodict.parse(open(unzipped_file_name, 'rb'))
            parse_end = time.time()
            logging.info(f'Parse time: {float(parse_end - parse_start):.2f} seconds')

            processing_start = time.time()
            articles = data['PubmedArticleSet']['PubmedArticle']
            for article in articles:
                try:
                    if article['MedlineCitation']['Article']['Abstract']:
                        has_abstract = True
                        abstract = article['MedlineCitation']['Article']['Abstract']['AbstractText']
                except:
                    has_abstract = False
                if has_abstract:
                    id = article['MedlineCitation']['PMID']['#text']
                    name = article['MedlineCitation']['Article']['ArticleTitle']
                    if isinstance(name, str):
                        name = name.lower()
                    elif isinstance(name, dict):
                        name = name.get('#text').lower()
                    else:
                        name = None
                    if name:
                        dict_to_add = {
                            'title': article['MedlineCitation']['Article']['ArticleTitle'],
                            'abstract': abstract,
                        }
                        for category in categories:
                            if category in name:
                                data_dict[category][id] = dict_to_add

            """
            After processing the unzipped file, delete it from the stage.
            """
            os.remove(unzipped_file_name)
            processing_end = time.time()
            logging.info(f'Processing time: {float(processing_end - processing_start):.2f} seconds')

            write_start = time.time()
            if not os.path.exists('data'):
                os.makedirs('data')
            for d, v in data_dict.items():
                with open(f'data/{d}.json', 'w') as f:
                    f.write(json.dumps(v, indent=4))
            write_end = time.time()
            logging.info(f'Write time: {float(write_end - write_start):.2f} seconds')

if __name__ == '__main__':
    main(categories=['oncology', 'cancer', 'leukemia'])