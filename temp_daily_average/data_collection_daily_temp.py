from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

# Standard
from datetime import timedelta
from datetime import datetime
from pathlib import Path
from pprint import pprint
import os

# PyPI
from bs4 import BeautifulSoup as BS
import requests

def find_highest_year():
    year_folders = os.listdir(path=Path('data'))
    return max(year_folders)

def find_new_year(year: int):
    url = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access'
    response = requests.get(url)
    parsed_html = BS(response.content, 'html.parser')
    cloud_year_set = set()
    for item in parsed_html.find_all('a'):
        #if item.get_text().endswith('/'):
        cloud_year = item.get_text().replace('/', '')
        cloud_year_set.add(cloud_year)
    cloud_year_set = sorted(cloud_year_set)
    for i in cloud_year_set:
        if i > year:
            return i


#year = 1942
def query_cloud_csvs(url: str, year: int) -> set:
    response = requests.get(url)
    parsed_html = BS(response.content, 'html.parser')
    csv_cloud_set = set()
    for item in parsed_html.find_all('a'):
        if '.csv' in item.get_text():
            csv_cloud_set.add(item.get_text())
    return csv_cloud_set

def query_local_csvs(year: int) -> set:
    csv_local_set = set()
    csv_folder = Path(Path.cwd() / 'data' / str(year)).rglob('*.csv')
    csv_local_list = [x for x in csv_folder]
    for i in csv_local_list:
        csv_local_set.add(str(i).split('/')[-1])
    return csv_local_set

def query_diff_local_cloud(local_set: set, cloud_set: set) -> set:
    diff_set = cloud_set.difference(local_set)
    if diff_set:
        print(f'{len(diff_set)} new data files available for download.')
    else:
        print(f'No new data files for {year}.')
    return diff_set

def download_new_csvs(url: str, year: int, diff_set: set):
    download_path = Path('data') / str(year)
    if os.path.exists(download_path) == False:
        Path(download_path).mkdir(parents=True, exist_ok=True)

    for i in diff_set:
        try:
            download_url = url + '/' + i
            print(download_url)
            result = requests.get(download_url)
            open(f'data/{year}/{i}', 'wb').write(result.content)
            #print(result.content)
        except requests.exceptions.InvalidURL:
            print('Bad url', i)

if __name__ == '__main__':
    year = find_highest_year()
    print(year)
    url = f'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{year}'
    cloud_set = query_cloud_csvs(url, year)
    print(len(cloud_set))
    local_set = query_local_csvs(year)
    print(len(local_set))
    diff_set = query_diff_local_cloud(local_set, cloud_set)
    print(diff_set)
    if len(diff_set) > 0:
        download_new_csvs(url, year, diff_set)
    else:
        year = find_new_year(year)
        print('new year:', year)


# (1) scan local data directory and identify the dir name of highest year
# (2) look for tracking.json in that directory and find the highest key
#     associated with a current file in the directory
# (3) use beautiful soup to pull the list) of csv links from the site,
#     match the local hightest key with the key in the dict created from
#     the list (make sure they match), then start downloading using the 
#     next key in the list
# (4) once all keys on the list have been downloaded (and verified that,
#     there are no new items), iterate to the next year and start over
# (5) once all files in entire site have been downloaded, design a quick
#     way to test for a new year, then download the new or missed days
# (I) Idea: test to see if any dates in the year path have been changed
#     (using some criteria)