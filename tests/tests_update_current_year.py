from pathlib import Path
from datetime import date

from temp_daily_average import update_current_year as current

import prefect
from prefect import Flow, Parameter, Task
from prefect import Parameter

year = Parameter('year', default=date.today().year)
base_url = Parameter('base_url', default='https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/')
data_dir = Parameter('data_dir', default=str(Path.home() / 'data_downloads' / 'noaa_daily_avg_temps'))


def test_build_url_success():
    assert current.build_url(base_url, year) == base_url + str(year)