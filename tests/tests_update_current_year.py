from pathlib import Path
from datetime import date
import pprint

from update_current_year_test import flow, build_url

import prefect
from prefect import Flow, Parameter, Task


print(flow.tasks)

#state = build_url(base_url, year).run()

assert len(flow.tasks) == 10
print(type(flow.tasks))
print(type(build_url))
print(str(build_url))


#print(state.result['find_updated_files'])


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
#assert current.build_url(base_url, year).run() == base_url + str(year)``