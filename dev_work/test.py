import os
import time
import sys
import datetime
from pathlib import Path
from stat import S_ISREG, ST_CTIME, ST_MODE

dir_path = Path.home() / 'data_downloads' / 'noaa_daily_avg_temps' / '2020'
# get all entries in the directory
entries = (os.path.join(dir_path, file_name) for file_name in os.listdir(dir_path))
# Get their stats
entries = ((os.stat(path), path) for path in entries)
# leave only regular files, insert creation date
entries = ((stat[ST_CTIME], path)
           for stat, path in entries if S_ISREG(stat[ST_MODE]))

for file_name in os.listdir(dir_path):
    date = os.stat(os.path.join(dir_path, file_name)).st_ctime
    print(date)
    print(datetime.datetime.fromtimestamp(date))