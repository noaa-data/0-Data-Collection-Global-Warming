from pathlib import Path
from datetime import date

import update_current_year as current

import prefect
from prefect import Flow, Parameter, Task
from prefect import Parameter

#year = Parameter('year', default=date.today().year)
base_url = Parameter('base_url', default='https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/')
data_dir = Parameter('data_dir', default=str(Path.home() / 'data_downloads' / 'noaa_daily_avg_temps'))

def test_tests():

pre_state = current.flow



def test_tasks():


#print(current.build_url(base_url, year).run())

# #state = current.flow()
# def test_something():
#     assert current.build_url.run() in current.flow.tasks #in current.flow.tasks()

# def test_do_something_else():
#     assert current.build_url(base_url, year).run() == '' 

# def test_full_flow():
#     assert state.is_successful()
#     assert state.result[current.build_url].is_successful()

#assert current.build_url in flow.tasks()


#assert t1_url in flow.tasks()

#def test_build_url_success():
#assert current.build_url(base_url, year).run() == base_url + str(year)