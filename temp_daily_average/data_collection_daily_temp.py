from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

# Standard
from datetime import timedelta
from datetime import datetime
from pathlib import Path
from pprint import pprint

# PyPI
from bs4 import BeautifulSoup as BS
import requests

url = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/1929'

csv_local_set = set()
csv_folder = Path(Path.cwd() / 'data').rglob('*.csv')
csv_local_list = [x for x in csv_folder]
for i in csv_local_list:
    csv_local_set.add(str(i).split('/')[-1])

response = requests.get(url)
parsed_html = BS(response.content, 'html.parser')
csv_cloud_set = set()
for item in parsed_html.find_all('a'):
    if '.csv' in item.get_text():
        csv_cloud_set.add(item.get_text())
        with open(Path.cwd() / 'data' / item.get_text(), 'w') as f:
            pass

diff = csv_local_set.difference(csv_cloud_set)

print(diff)


# (1) scan local data directory and identify the dir name of highest year
# (2) look for tracking.json in that directory and find the highest key
#     associated with a current file in the directory
# (3) use beautiful soup to pull the list of csv links from the site,
#     match the local hightest key with the key in the dict created from
#     the list (make sure they match), then start downloading using the 
#     next key in the list
# (4) once all keys on the list have been downloaded (and verified that,
#     there are no new items), iterate to the next year and start over
# (5) once all files in entire site have been downloaded, design a quick
#     way to test for a new year, then download the new or missed days
# (I) Idea: test to see if any dates in the year path have been changed
#     (using some criteria)