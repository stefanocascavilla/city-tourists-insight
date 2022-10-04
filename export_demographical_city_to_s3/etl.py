import prefect
import requests
import re

from typing import List
from prefect import task, Flow, Parameter
from bs4 import BeautifulSoup

FLOW_NAME = 'export_demographical_city_to_s3'

WIKIPEDIA_PREFIX = 'https://en.wikipedia.org/wiki/{}'
PATTERN = r'\[.*?\]'


@task(name='Retrieve Wikipedia City Page')
def retrieve_wikipedia_city_page(city_name: str) -> BeautifulSoup:
    """
    
    :param city_name: The name of the city to retrieve info from Wikipedia
    :return: A BeautifulSoup object
    """
    prefect.context.logger.info(f'Retrieving Wikipedia page for {city_name} city...')
    wikipedia_page_url = WIKIPEDIA_PREFIX.format(city_name)
    web_page = requests.get(wikipedia_page_url, 'html.parser')
    prefect.context.logger.info(f'Retrieved Wikipedia page for {city_name} city.')

    return BeautifulSoup(web_page.content)

@task(name='Scraping the City Page')
def scrape_wikipedia_city_page(city_page: BeautifulSoup) -> dict:
    """
    
    :param city_page: The BeautifulSoup instance representing the Wikipedia page
    :return: A dict containing info about the city
    """
    city_name = city_page.find('span', class_='mw-page-title-main').get_text()
    prefect.context.logger.info(f'Retrieving information about {city_name} city...')

    longitude = city_page.find('span', class_='longitude').get_text()
    latitude = city_page.find('span', class_='latitude').get_text()

    if not city_page.select_one('.mergedtoprow:-soup-contains("Population")'):
        raise ValueError(f'The Wikipedia page for {city_name} does not contain info about population!')

    p = city_page.select_one('.mergedtoprow:-soup-contains("Population")')
    population = re.sub(PATTERN, '', p.find_next('td').get_text())

    return {
        'city_name': city_name,
        'population': population,
        'long': longitude,
        'lat': latitude
    }

@task(name='write_city_info_to_s3')
def write_city_info_to_s3(city_info: List[dict]) -> None:
    """
    
    :param city_info: dict containing info about cities
    :return: None
    """
    pass

with Flow(flow_name=FLOW_NAME) as f:
    city_list = Parameter('city_list', default=['London', 'Tokyo', 'Milan', 'Stockholm'])

    # Get the webpage of cities
    city_pages = retrieve_wikipedia_city_page.map(city_name=city_list)

    # Scrape the webpages to get info
    city_info = scrape_wikipedia_city_page.map(city_page=city_pages)
