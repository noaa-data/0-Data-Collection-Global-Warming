#!/home/pi/github/Data-Collection-Global-Warming/venv/bin python3

from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from prefect.engine.signals import SKIP

# Standard
from datetime import timedelta
from datetime import datetime
from datetime import date
from pathlib import Path
from pprint import pprint
import os

# PyPI
from bs4 import BeautifulSoup as BS
import requests
import pandas as pd


@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def build_url(base_url, year=''):
    return f'{base_url}/{year}'


@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def cloud_csvs_and_timestamps(url: str) -> pd.DataFrame:
    cloud_list = []
    filename, date = None, None
    response = requests.get(f'{url}/{year}')
    parsed_html = BS(response.content, 'html.parser')
    for item in parsed_html('tr'):
        href = item('a')
        filename = href[0].get_text() if href and '.csv' in href[0].get_text() else None
        td = item('td', {'align': 'right'})
        date = td[0].get_text() if td and re.match(r'\d\d\d\d-\d\d-\d\d', td[0].get_text()) else None
        if date and filename:
            cloud_list.append(('cloud', filename, date))
    return pd.DataFrame(cloud_list, columns = ['type', 'filename', 'cloud_date'])


@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def local_csvs_and_timestamps(data_dir: str, year: str) -> pd.DataFrame:
    local_list = []
    dir_path = Path(data_dir) / str(year)
    print(dir_path)
    for file_name in os.listdir(dir_path):
        date = os.stat(os.path.join(dir_path, file_name)).st_ctime
        local_list.append(('local', file_name, str(datetime.fromtimestamp(date))))
    return pd.DataFrame(local_list, columns = ['type', 'filename', 'local_date'])


@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def find_difference(cloud_df, local_df) -> pd.DataFrame:
    return pd.concat([local_df, cloud_df]).drop_duplicates(subset = ['filename'], keep=False)


@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def find_updated_files(cloud_df, local_df) -> pd.DataFrame:
    file_df = pd.DataFrame(columns = ['type', 'filename', 'cloud_date'])
    file_df = file_df.append(cloud_df)
    file_df['local_date'] = file_df['filename'].map(local_df.set_index('filename')['local_date'])
    return file_df[(file_df['cloud_date'] > file_df['local_date'])]


@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def combine_and_return_set(new_df, updated_df) -> set:
    download_df = updated_df.append(new_df)
    return set(download_df['filename'].to_list())


@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def download_new_csvs(url: str, year: str, diff_set: set, data_dir: str) -> bool:
    count = 0
    data_dir = Path(data_dir)
    download_path = data_dir / str(year) #Path('data') / str(year)
    if os.path.exists(download_path) == False:
        Path(download_path).mkdir(parents=True, exist_ok=True)

    for i in diff_set:
        if count <= 2000:
            try:
                download_url = url + '/' + i
                print(download_url)
                result = requests.get(download_url)
                file_path = Path(data_dir) / str(year) / i
                open(file_path, 'wb').write(result.content)
                #print(result.content)
            except requests.exceptions.InvalidURL:
                print('Bad url', i)
        count += 1
    if count <= 2000:
        return True

if os.environ.get('TEST_PREFECT') == 'True':
    schedule = None#IntervalSchedule(interval=timedelta(minutes=0.1))
else:
    schedule = IntervalSchedule(interval=timedelta(minutes=45))

with Flow('NOAA Daily Avg Current Year', schedule=schedule) as flow:
    year = Parameter('year', default=date.today().year)
    base_url = Parameter('base_url', default='https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/')
    data_dir = Parameter('data_dir', default=str(Path.home() / 'data_downloads' / 'noaa_daily_avg_temps'))

    t1_url  = build_url(base_url=base_url, year=year)
    t2_cloud = cloud_csvs_and_timestamps(url=t1_url)
    t3_local = local_csvs_and_timestamps(data_dir=data_dir, year=year)
    t4_new = find_difference(cloud_df=t2_cloud, local_df=t3_local)
    t5_updates = find_updated_files(cloud_df=t2_cloud, local_df=t3_local)
    t6_dwnload = combine_and_return_set(new_df=t4_new, updated_df=t5_updates)
    t7_task = download_new_csvs(url=t1_url, year=year, diff_set=t6_dwnload, data_dir=data_dir)


if os.environ.get('TEST_PREFECT') == 'True':
    flow.run()
else:
    flow.register(project_name="Global Warming Data")
