import json
import requests
import time
from pytz import timezone
import datetime as dt
import schedule
import os

from resources import ConfigS3



API_KEYS = [
  '21HYqiZMMrc5SrNN6jVOk230BWbg3tAj',
  'Pm7CnuOA6G6Uw25xaMtGy6yxSmaqACXM',
  'cBAeCxqGO4OJq7y1wfyFOhA0HRWpgAml',
  'nGKarNyeU0VtPjtM5chG4Uif6KlRUa1E',
  '1j5sGYeflskicFlJYJtMbgFdLB9xRAji'
]

api_counters = [0] * len(API_KEYS)
api_call_counter = 0
retry = 0
MAX_RETRY = 10
LOG_FILE = 'tomtom_logs.txt'

checked_time_list = {
   "1": ["1:57:26", "23:57:26"],
   "2": ["6:28:00", "10:32:00"]
}

STYLES = ['absolute', 'relative', 'relative0', 'relate0-dark', 'relative-delay', 'reduced-sensitivity']
ZOOM = 22
TOMTOM_URL = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/{STYLES[0]}/{ZOOM}/json"

define_s3 = ConfigS3()

API_KEYS_WEATHER = [
  '8df3c71da563b1c6a06e2239c780ba82'
]

LOG_FILE = 'weatherapi_logs.txt'
HCMC = [10.8333, 106.6667]	# https://openweathermap.org/find?q=Ho+Chi+Minh

BASE_URL = f"https://api.openweathermap.org/data/2.5/weather"

PERIOD_LENGTH = 5  # minutes

def parse_date_and_period(timestamp):
    ts = dt.datetime.fromtimestamp(timestamp)
    date, time, weeekday = ts.date(), ts.time(), ts.weekday()

    h, m, s = time.hour, time.minute, time.second

    hour = f"0{h}" if h < 10 else str(h)
    step = (m * 60 + s) // (PERIOD_LENGTH * 60)
    m = PERIOD_LENGTH * step
    minute = f"0{m}" if m < 10 else str(m)
    period = f"period_{hour}_{minute}"

    return str(date), period, weeekday

def crawl_current_weather():
	params = {
		'lat' : HCMC[0],
		'lon' : HCMC[1],
		'appid' : API_KEYS_WEATHER[0]
	}
	data = requests.get(BASE_URL, params).json()
	result = "none"
	try:
		result = data["weather"][0]["main"]
	except:
		pass
	return result

def convert_str_to_time(time_str):
   return dt.datetime.strptime(time_str, '%H:%M:%S').time()

def check_times(list_time):
   tz_VN = timezone('Asia/Ho_Chi_Minh') 
   datetime_VN = dt.datetime.now(tz_VN)
   current_time = datetime_VN.time()
   checked_time = False
   for key, value in checked_time_list.items():
      if convert_str_to_time(value[0]) <= current_time <= convert_str_to_time(value[1]):
        checked_time = True

   return checked_time

def log(message):
  with open(LOG_FILE, 'a+') as f:
    f.write(message)
    

def tom_url(zoom_level): return f"https://api.tomtom.com/traffic/services/4/flowSegmentData/{STYLES[0]}/{zoom_level}/json"

def get_tomtom_data(lat, lng, zoom_level=22):
    global api_counters
    global api_call_counter

    try:
        params = {
          'point': f"{lat},{lng}",
          'unit': 'KMPH',
          'openLr': 'false',
          'key': API_KEYS[api_call_counter]
        }
        data = requests.get(tom_url(zoom_level), params=params).json()

        api_counters[api_call_counter] += 1
        return data['flowSegmentData']
        
    except ValueError:
        api_call_counter += 1
        return get_tomtom_data(lat, lng)
        
    except KeyError:
        log("Key error: " + str(lat) + "," + str(lng) + "\n")
        return None
    
def crawl_data(limit=40):
    """
    Main function to crawl data an
    d map to segment_id
    """
    global checked_time_list
    if not check_times(checked_time_list):
       return
    
    with open('selected_points.json', 'r') as f:
        cover_points = json.load(f)

    weather = crawl_current_weather()
    timestamp = int(time.time())
    output = dict()
    id = 0
        
    for record in cover_points:
        for point in record["segment_ids"]:
            lng, lat = point['lng'], point['lat']
            tomtom_data = get_tomtom_data(lat, lng)
            if tomtom_data == None: continue
            output[point["segment_id"]] = {
                'timestamp': timestamp,
                'velocity': tomtom_data['currentSpeed'],
                'confidence': tomtom_data['confidence'],
                'segment_id': point['segment_id'],
                'source': 'tom-tom',
                'weather': weather
            }
            print(output[point["segment_id"]])
        id += 1
        if id == limit: break
    date, period, weekday = parse_date_and_period(timestamp)
    with open('output.json', 'w') as f:
        json.dump(output, f)
    try:
        define_s3.upload_file_to_s3('output.json', f'tomtom-voh/{date}/{timestamp}.json')
    except:
       pass
    os.remove('output.json')

    # s3 = boto3.client('s3')
    # with open('output.json', 'rb') as f:
    #     s3.upload_fileobj(f, 'tomtom-voh', f"{timestamp}.json")
                
    
    return timestamp
    
    
def crawl_job():
  global api_call_counter
  global retry

  try:
    start_time = time.time()
    timestamp = crawl_data()
    end_time = time.time()

    log(f"[{dt.datetime.now()}] - {timestamp}.json - Run time: {end_time - start_time} (sec)\n")
    # print(f"[{dt.datetime.now()}]  - {timestamp}.json - {end_time - start_time} - {api_counters[api_call_counter]}")
    
  except IndexError:
    if retry == 0:
      log(f"[{dt.datetime.now()}] Reset\n")
      api_call_counter = 0
      retry += 1
      crawl_job()
    elif retry < MAX_RETRY:
      log(f"[{dt.datetime.now()}] Run out of API keys, try to reset: {retry}\n")
      api_call_counter = 0
      retry += 1
      crawl_job()
    else:
      log(f"[{dt.datetime.now()}] Exceed timeout retry, not enough API keys\n")
      raise Exception('Timeout retry, not enough API keys')

def log_api_calls_summary():
  message = [f"[{dt.datetime.now()}] API call summary"]
  for key, counter in zip(API_KEYS, api_counters):
    message.append(f"{key} - {counter}")
  log('\n'.join(message) + '\n')


# Note that the time elapsed while requesting API could be significant, so the gap between each run will be 5 minutes + request_time
schedule.every(5).minutes.do(crawl_job)
schedule.every(60).minutes.do(log_api_calls_summary)

def main():
  while True:
    schedule.run_pending()


if __name__ == '__main__':
   timestamp = crawl_data()
#   print('Running...')
#   try:
#     main()
#   finally:
#     print('Done!')
