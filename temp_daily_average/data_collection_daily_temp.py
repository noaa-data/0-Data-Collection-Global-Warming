from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from prefect.engine.signals import SKIP

# Standard
from datetime import timedelta
from datetime import datetime
from pathlib import Path
from pprint import pprint
import os

# PyPI
from bs4 import BeautifulSoup as BS
import requests


@task # pylink: disable=
def find_highest_year(url: str):
    year_folders = os.listdir(path=Path('data'))
    if year_folders:
        return max(year_folders)
    else:
        return 0
    # year = find_new_year(next_year=True, year=1900)
    # return year
    raise SKIP


@task
def build_url(year):
    return f'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{year}'


@task
def query_cloud_csvs(url: str, year: int) -> set:
    response = requests.get(url)
    parsed_html = BS(response.content, 'html.parser')
    csv_cloud_set = set()
    for item in parsed_html.find_all('a'):
        if '.csv' in item.get_text():
            csv_cloud_set.add(item.get_text())
    return csv_cloud_set


@task
def query_local_csvs(year: int) -> set:
    csv_local_set = set()
    csv_folder = Path(Path.cwd() / 'data' / str(year)).rglob('*.csv')
    csv_local_list = [x for x in csv_folder]
    for i in csv_local_list:
        csv_local_set.add(str(i).split('/')[-1])
    return csv_local_set


@task
def query_diff_local_cloud(local_set: set, cloud_set: set) -> set:
    diff_set = cloud_set.difference(local_set)
    if diff_set:
        print(f'{len(diff_set)} new data files available for download.')
    else:
        print(f'No new data files for this run.')
    return diff_set


@task
def download_new_csvs(url: str, year: int, diff_set: set) -> bool:
    if int(year) > 0:
        count = 0
        download_path = Path('data') / str(year)
        if os.path.exists(download_path) == False:
            Path(download_path).mkdir(parents=True, exist_ok=True)

        for i in diff_set:
            if count <= 1000:
                try:
                    download_url = url + '/' + i
                    print(download_url)
                    result = requests.get(download_url)
                    open(f'data/{year}/{i}', 'wb').write(result.content)
                    #print(result.content)
                except requests.exceptions.InvalidURL:
                    print('Bad url', i)
            count += 1
        if count <= 1000:
            return True
    elif year == 0:
        return True


@task
def find_new_year(next_year: bool, year: int):
    if next_year:
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
            if int(i) > int(year):
                year = i
                download_path = Path('data') / str(year)
                if os.path.exists(download_path) == False:
                    Path(download_path).mkdir(parents=True, exist_ok=True)
                print('STATUS => new year:', year)
                return year
    print('STATUS => current year not finished.')


schedule = IntervalSchedule(interval=timedelta(minutes=0.2))


with Flow('NOAA Daily Average Temp Records', schedule) as flow:
    base_url = Parameter('base_url', default='https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/')
    
    t1_year = find_highest_year(url=base_url)
    t2_url  = build_url(year=t1_year)
    t3_cset = query_cloud_csvs(url=t2_url, year=t1_year)
    t4_lset = query_local_csvs(t1_year)
    t5_dset = query_diff_local_cloud(local_set=t4_lset, cloud_set=t3_cset)
    t6_next = download_new_csvs(url=t2_url, year=t1_year, diff_set=t5_dset)
    t7_task = find_new_year(next_year=t6_next, year=t1_year)


flow.run()