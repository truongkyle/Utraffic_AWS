import json
import requests
import time
from pytz import timezone
import datetime as dt
import schedule

from resources import ConfigS3
define_s3 = ConfigS3()
timestamp = int(time.time())
try:
    define_s3.upload_file_to_s3('2022-05-10 result_df.csv', f'tomtom-voh/{timestamp}.csv')
    print('Upload')
except:
    print("Couldn't upload")