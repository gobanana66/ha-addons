# Added missing imports
import os
import sys
import time
import threading
import logging
import schedule
import requests
import json
import base64
import pytz

# %%
import base64, requests, time, json, pytz, logging, os, sys
# import myfitnesspal
# import browser_cookie3
from requests.exceptions import ConnectionError
from datetime import datetime, timedelta
# for influxdb 1.x
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
# for influxdb 2.x
from influxdb_client import InfluxDBClient as InfluxDBClient2
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS
from requests.exceptions import ReadTimeout
#from mfp_sync import sync_mfp_to_fitbit, add_mfp_totals_to_influxdb
# %% [markdown]
# ## Variables


# %%
FITBIT_LOG_FILE_PATH = os.environ.get("FITBIT_LOG_FILE_PATH") or "your/expected/log/file/location/path"
TOKEN_FILE_PATH = os.environ.get("TOKEN_FILE_PATH") or "your/expected/token/file/location/path"
OVERWRITE_LOG_FILE = True
FITBIT_LANGUAGE = 'en_US'
INFLUXDB_VERSION = os.environ.get("INFLUXDB_VERSION") or "2" # Version of influxdb in use, supported values are 1 or 2
# Update these variables for influxdb 1.x versions
INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST") or 'localhost' # for influxdb 1.x
INFLUXDB_PORT = os.environ.get("INFLUXDB_PORT") or 8086 # for influxdb 1.x 
INFLUXDB_USERNAME = os.environ.get("INFLUXDB_USERNAME") or 'your_influxdb_username' # for influxdb 1.x
INFLUXDB_PASSWORD = os.environ.get("INFLUXDB_PASSWORD") or 'your_influxdb_password' # for influxdb 1.x
INFLUXDB_DATABASE = os.environ.get("INFLUXDB_DATABASE") or 'your_influxdb_database_name' # for influxdb 1.x
# Update these variables for influxdb 2.x versions
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET") or "your_bucket_name_here" # for influxdb 2.x
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG") or "your_org_here" # for influxdb 2.x
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN") or "your_token_here" # for influxdb 2.x
INFLUXDB_URL = os.environ.get("INFLUXDB_URL") or "http://homeassistant.local:8086" # for influxdb 2.x
# MAKE SURE you set the application type to PERSONAL. Otherwise, you won't have access to intraday data series, resulting in 40X errors.
client_id = os.environ.get("CLIENT_ID") or "your_application_client_ID" # Change this to your client ID
client_secret = os.environ.get("CLIENT_SECRET") or "your_application_client_secret" # Change this to your client Secret
DEVICENAME = os.environ.get("DEVICENAME") or "Your_Device_Name" # e.g. "Charge5"
ACCESS_TOKEN = "" # Empty Global variable initialization, will be replaced with a functional access code later using the refresh code
AUTO_DATE_RANGE = True # Automatically selects date range from todays date and update_date_range variable
auto_update_date_range = 1 # Days to go back from today for AUTO_DATE_RANGE *** DO NOT go above 2 - otherwise may break rate limit ***
LOCAL_TIMEZONE = os.environ.get("LOCAL_TIMEZONE") or "Automatic" # set to "Automatic" for Automatic setup from User profile (if not mentioned here specifically).
SCHEDULE_AUTO_UPDATE = True if AUTO_DATE_RANGE else False # Scheduling updates of data when script runs
SERVER_ERROR_MAX_RETRY = 3
EXPIRED_TOKEN_MAX_RETRY = 5
SKIP_REQUEST_ON_SERVER_ERROR = True

# # Get cookies from your browser for myfitnesspal.com
# # cj = browser_cookie3.firefox(domain_name='myfitnesspal.com')  # or .chrome(), etc.

# # Authenticate MyFitnessPal client
# # mfp_client = myfitnesspal.Client(cookiejar=cj)
# If MyFitnessPal integration isn't configured, keep a safe default to avoid NameError
mfp_client = None

# Update Google Sheet with weight data 
GOOGLE_FORM_URL = os.environ.get("GOOGLE_FORM_URL")

DEBUG_MODE = False

COLLECTED_RECORDS_FILE_PATH = os.environ.get("COLLECTED_RECORDS_FILE_PATH") or "./debug"

# --- Debugging: Save/load collected_records to/from file ---
def dump_collected_records_to_file(filename="fitbit_collected_records.json"):
    os.makedirs(COLLECTED_RECORDS_FILE_PATH, exist_ok=True)
    full_path = os.path.join(COLLECTED_RECORDS_FILE_PATH, filename)
    with open(full_path, "w") as f:
        json.dump(collected_records, f, indent=2)
    logging.info(f"Dumped {len(collected_records)} records to {full_path}")

def load_collected_records_from_file(filename="fitbit_collected_records.json"):
    full_path = os.path.join(COLLECTED_RECORDS_FILE_PATH, filename)
    with open(full_path, "r") as f:
        return json.load(f)

# %% [markdown]
# ## Logging setup

# %%
if OVERWRITE_LOG_FILE:
    with open(FITBIT_LOG_FILE_PATH, "w"): pass

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(FITBIT_LOG_FILE_PATH, mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Check InfluxDB
def test_influxdb_connection():
    try:
        if INFLUXDB_VERSION == "2":
            health = influxdbclient.ping()
            if not health:
                raise InfluxDBError("InfluxDB v2 server not responding to ping.")
            # Additional optional test: list buckets
            buckets = influxdbclient.buckets_api().find_buckets()
            logging.info(f"Connected to InfluxDB v2. Found {len(buckets.buckets)} buckets.")
            # logging.info(f"{buckets.buckets}")
        elif INFLUXDB_VERSION == "1":
            influxdbclient.ping()  # Raises if fails
            logging.info("Connected to InfluxDB v1.")
        else:
            raise ValueError("Unsupported InfluxDB version.")
    except Exception as e:
        logging.critical(f"InfluxDB connection test failed: {e}")
        sys.exit(1)  # Exit immediately if InfluxDB isn’t reachable



# Variablen für API-Anfrageüberwachung
api_request_count = 0
API_REQUEST_LIMIT = 150  # Maximale Anzahl von Anfragen pro Stunde
api_request_lock = threading.Lock()

def reset_api_request_count():
    global api_request_count
    with api_request_lock:
        api_request_count = 0
    # Timer zum nächsten Reset in einer Stunde
    threading.Timer(3600, reset_api_request_count).start()

# Starte den ersten Reset-Timer
reset_api_request_count()

def increment_api_request_count():
    global api_request_count
    with api_request_lock:
        api_request_count += 1


# %% [markdown]
# ## Setting up base API Caller function

# %%
# Generic Request caller for all 
def request_data_from_fitbit(url, headers={}, params={}, data={}, request_type="get"):
    global ACCESS_TOKEN, api_request_count  # Declare global variables
    retry_attempts = 0
    logging.debug("Requesting data from fitbit via Url : " + url)
    
    while True:
        if request_type == "get":
            headers = {
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "Accept": "application/json",
                'Accept-Language': FITBIT_LANGUAGE
            }
        try:
            # Check API Request Limit
            with api_request_lock:
                if api_request_count >= API_REQUEST_LIMIT:
                    logging.info("API Limit Reached. Waiting for reset.")
                    time.sleep(60)  # Wait 60 seconds before trying again.
                    continue
                else:
                    api_request_count += 1

            # Anfrage senden
            if request_type == "get":
                response = requests.get(url, headers=headers, params=params, data=data)
            elif request_type == "post":
                response = requests.post(url, headers=headers, params=params, data=data)
            else:
                raise Exception("Invalid request type " + str(request_type))

            if response.status_code == 200:  # Success
                return response.json()
            elif response.status_code == 429:  # API Limit reached
                logging.debug(f"Response headers: {response.headers}")
                retry_after = int(response.headers.get("Retry-After", 60)) + 300  # default 60s if missing
                logging.warning(f"Fitbit API limit reached (429). Retrying in {retry_after} seconds.")
                print(f"Fitbit API limit reached (429). Retrying in {retry_after} seconds.")
                time.sleep(retry_after)
            elif response.status_code == 401:  # Access token expired ( most likely )
                logging.info("Current Access Token : " + ACCESS_TOKEN)
                logging.warning("Error code : " + str(response.status_code) + ", Details : " + response.text)
                print("Error code : " + str(response.status_code) + ", Details : " + response.text)
                ACCESS_TOKEN = Get_New_Access_Token(client_id, client_secret)
                logging.info("New Access Token : " + ACCESS_TOKEN)
                time.sleep(30)
                if retry_attempts > EXPIRED_TOKEN_MAX_RETRY:
                    logging.error("Unable to solve the 401 Error. Please debug - " + response.text)
                    raise Exception("Unable to solve the 401 Error. Please debug - " + response.text)
            elif response.status_code in [500, 502, 503, 504]:  # Fitbit server is down or not responding ( most likely ):
                logging.warning("Server Error encountered ( Code 5xx ): Retrying after 120 seconds....")
                time.sleep(120)
                if retry_attempts > SERVER_ERROR_MAX_RETRY:
                    logging.error("Unable to solve the server Error. Retry limit exceed. Please debug - " + response.text)
                    if SKIP_REQUEST_ON_SERVER_ERROR:
                        logging.warning("Retry limit reached for server error : Skipping request -> " + url)
                        return None
            else:
                # Treat 403 (permission denied for a resource like SPO2) as non-fatal: log and return None
                logging.error("Fitbit API request failed. Status code: " + str(response.status_code) + " " + str(response.text))
                print(f"Fitbit API request failed. Status code: {response.status_code}", response.text)
                if response.status_code == 403:
                    logging.warning("Permission denied (403) for this resource. Continuing without this data.")
                    return None
                try:
                    response.raise_for_status()
                except Exception:
                    # If raise_for_status still raises, re-raise after logging
                    logging.exception("Unhandled HTTP error from Fitbit API")
                    raise
                return None

        except ConnectionError as e:
            logging.error("Retrying in 5 minutes - Failed to connect to internet : " + str(e))
            print("Retrying in 5 minutes - Failed to connect to internet : " + str(e))
        retry_attempts += 1
        time.sleep(30)


# %% [markdown]
# ## Token Refresh Management

# %%
def refresh_fitbit_tokens(client_id, client_secret, refresh_token):
    logging.info("Attempting to refresh tokens...")
    url = "https://api.fitbit.com/oauth2/token"
    headers = {
        "Authorization": "Basic " + base64.b64encode((client_id + ":" + client_secret).encode()).decode(),
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    json_data = request_data_from_fitbit(url, headers=headers, data=data, request_type="post")
    access_token = json_data["access_token"]
    new_refresh_token = json_data["refresh_token"]
    tokens = {
        "access_token": access_token,
        "refresh_token": new_refresh_token
    }
    with open(TOKEN_FILE_PATH, "w") as file:
        json.dump(tokens, file)
    logging.info("Fitbit token refresh successful!")
    return access_token, new_refresh_token

def load_tokens_from_file():
    with open(TOKEN_FILE_PATH, "r") as file:
        tokens = json.load(file)
        return tokens.get("access_token"), tokens.get("refresh_token")

def Get_New_Access_Token(client_id, client_secret):
    try:
        access_token, refresh_token = load_tokens_from_file()
    except FileNotFoundError:
        refresh_token = input("No token file found. Please enter a valid refresh token : ")
    access_token, refresh_token = refresh_fitbit_tokens(client_id, client_secret, refresh_token)
    return access_token
    
ACCESS_TOKEN = Get_New_Access_Token(client_id, client_secret)


# %% [markdown]
# ## Influxdb Database Initialization

# %%
if INFLUXDB_VERSION == "2":
    try:
        influxdbclient = InfluxDBClient2(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        influxdb_write_api = influxdbclient.write_api(write_options=SYNCHRONOUS)
    except InfluxDBError as err:
        logging.error("Unable to connect with influxdb 2.x database! Aborted")
        raise InfluxDBError("InfluxDB connection failed:" + str(err))
elif INFLUXDB_VERSION == "1":
    try:
        influxdbclient = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD)
        influxdbclient.switch_database(INFLUXDB_DATABASE)
    except InfluxDBClientError as err:
        logging.error("Unable to connect with influxdb 1.x database! Aborted")
        raise InfluxDBClientError("InfluxDB connection failed:" + str(err))
else:
    logging.error("No matching version found. Supported values are 1 and 2")
    raise InfluxDBClientError("No matching version found. Supported values are 1 and 2:")

test_influxdb_connection()

MAX_RETRIES = 3
INITIAL_BACKOFF = 2  # seconds

def write_points_to_influxdb(points):
    def retry_write(write_func, description="InfluxDB write"):
        attempt = 0
        backoff = INITIAL_BACKOFF
        while attempt < MAX_RETRIES:
            try:
                write_func()
                logging.info(f"{description} succeeded on attempt {attempt + 1}")
                return
            except (InfluxDBError, InfluxDBClientError, ReadTimeout) as e:
                attempt += 1
                logging.warning(f"{description} failed (attempt {attempt}): {e}")
                time.sleep(backoff)
                backoff *= 2
            except Exception as e:
                logging.error(f"{description} encountered unexpected error: {e}")
                break

        # After retries exhausted
        filename = f"failed_records_{int(time.time())}.json"
        dump_collected_records_to_file(filename)
        logging.error(f"{description} failed after {MAX_RETRIES} attempts. Dumped to {filename}")

    if INFLUXDB_VERSION == "2":
        retry_write(lambda: influxdb_write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points), "InfluxDB v2 write")
    elif INFLUXDB_VERSION == "1":
        retry_write(lambda: influxdbclient.write_points(points), "InfluxDB v1 write")
    else:
        logging.error("Unsupported InfluxDB version. Must be 1 or 2.")
        raise InfluxDBClientError("Unsupported InfluxDB version. Must be 1 or 2.")

# %% [markdown]
# ## Set Timezone from profile data

# %%
# Initialize LOCAL_TIMEZONE with a pytz timezone object
if LOCAL_TIMEZONE == "Automatic":
    profile_resp = request_data_from_fitbit("https://api.fitbit.com/1/user/-/profile.json")
    try:
        tzname = None
        if profile_resp and isinstance(profile_resp, dict):
            tzname = profile_resp.get("user", {}).get("timezone")
        if tzname:
            LOCAL_TIMEZONE = pytz.timezone(tzname)
        else:
            logging.warning("Could not determine timezone from profile; defaulting to UTC")
            LOCAL_TIMEZONE = pytz.UTC
    except Exception:
        logging.exception("Failed to set LOCAL_TIMEZONE from profile; defaulting to UTC")
        LOCAL_TIMEZONE = pytz.UTC
else:
    LOCAL_TIMEZONE = pytz.timezone(LOCAL_TIMEZONE)  # Ensure LOCAL_TIMEZONE is a timezone object


# Timezone-safe helper
def safe_to_utc(dt: datetime, timezone):
    if dt.tzinfo is None:
        dt = timezone.localize(dt)
    else:
        dt = dt.astimezone(timezone)
    return dt.astimezone(pytz.utc).isoformat()

# %% [markdown]
# ## Selecting Dates for update

# %%
if AUTO_DATE_RANGE:
    
    end_date = datetime.now(LOCAL_TIMEZONE)
    start_date = end_date - timedelta(days=auto_update_date_range)
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date_str = start_date.strftime("%Y-%m-%d")
else:
    start_date_str = input("Enter start date in YYYY-MM-DD format : ")
    end_date_str = input("Enter end date in YYYY-MM-DD format : ")
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

# %% [markdown]
# ## Setting up functions for Requesting data from server

# %%
collected_records = []

def update_working_dates():
    global end_date, start_date, end_date_str, start_date_str
    end_date = datetime.now(LOCAL_TIMEZONE)
    start_date = end_date - timedelta(days=auto_update_date_range)
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date_str = start_date.strftime("%Y-%m-%d")

# Get last synced battery level of the device (excluding MobileTrack)
def get_battery_level():
    devices = request_data_from_fitbit("https://api.fitbit.com/1/user/-/devices.json")
    if devices:
        for device in devices:
            if device.get("deviceVersion") != "MobileTrack":
                collected_records.append({
                    "measurement": "DeviceBatteryLevel",
                    "time": LOCAL_TIMEZONE.localize(datetime.fromisoformat(device['lastSyncTime'])).astimezone(pytz.utc).isoformat(),
                    "fields": {
                        "value": float(device['batteryLevel'])
                    }
                })
                logging.info(f"Recorded battery level for {device.get('deviceVersion', DEVICENAME)}")
                break  # Only store the first non-MobileTrack device
        else:
            logging.error("No non-MobileTrack device found for battery level logging.")
    else:
        logging.error("Recording battery level failed: No devices found.")

# For intraday detailed data, max possible range in one day. 
def get_intraday_data_limit_1d(date_str, measurement_list):
    for measurement in measurement_list:
        url = 'https://api.fitbit.com/1/user/-/activities/' + measurement[0] + '/date/' + date_str + '/1d/' + measurement[2] + '.json'
        resp = request_data_from_fitbit(url)
        dataset = None
        if resp and isinstance(resp, dict):
            dataset = resp.get("activities-" + measurement[0] + "-intraday", {}).get('dataset')

        if dataset:
            for value in dataset:
                try:
                    log_time = datetime.fromisoformat(date_str + "T" + value['time'])
                    utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
                    collected_records.append({
                            "measurement":  measurement[1],
                            "time": utc_time,
                            "tags": {
                                "Device": DEVICENAME
                            },
                            "fields": {
                                "value": float(value.get('value', 0))
                            }
                        })
                except Exception:
                    logging.exception(f"Failed to process intraday value for {measurement[1]} on {date_str}")
            logging.info("Recorded " +  measurement[1] + " intraday for date " + date_str)
        else:
            logging.warning(f"No intraday dataset for {measurement[1]} on {date_str}; skipping.")

# Max range is 30 days, records BR, SPO2 Intraday, skin temp and HRV - 4 queries
def get_daily_data_limit_30d(start_date_str, end_date_str):

    hrv_resp = request_data_from_fitbit('https://api.fitbit.com/1/user/-/hrv/date/' + start_date_str + '/' + end_date_str + '.json')
    hrv_data_list = hrv_resp.get('hrv') if isinstance(hrv_resp, dict) else None
    if hrv_data_list:
        for data in hrv_data_list:
            try:
                log_time = datetime.fromisoformat(data["dateTime"] + "T04:00:00.000")
                utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
                collected_records.append({
                        "measurement":  "HRV",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            "dailyRmssd": data.get("value", {}).get("dailyRmssd"),
                            "deepRmssd": data.get("value", {}).get("deepRmssd")
                        }
                    })
            except Exception:
                logging.exception(f"Failed to process HRV record: {data}")
        logging.info("Recorded HRV for date " + start_date_str + " to " + end_date_str)
    else:
        logging.warning("No HRV data found for date range %s to %s", start_date_str, end_date_str)

    br_resp = request_data_from_fitbit('https://api.fitbit.com/1/user/-/br/date/' + start_date_str + '/' + end_date_str + '.json')
    br_data_list = br_resp.get('br') if isinstance(br_resp, dict) else None
    if br_data_list:
        for data in br_data_list:
            try:
                log_time = datetime.fromisoformat(data["dateTime"] + "T00:00:00")
                utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
                collected_records.append({
                        "measurement":  "BreathingRate",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            "value": float(data.get("value", {}).get("breathingRate", 0))
                        }
                    })
            except Exception:
                logging.exception(f"Failed to process breathing rate record: {data}")
        logging.info("Recorded BR for date " + start_date_str + " to " + end_date_str)
    else:
        logging.warning("No BreathingRate data for date range %s to %s", start_date_str, end_date_str)

    skin_resp = request_data_from_fitbit('https://api.fitbit.com/1/user/-/temp/skin/date/' + start_date_str + '/' + end_date_str + '.json')
    skin_temp_data_list = skin_resp.get('tempSkin') if isinstance(skin_resp, dict) else None
    if skin_temp_data_list:
        for temp_record in skin_temp_data_list:
            try:
                log_time = datetime.fromisoformat(temp_record["dateTime"] + "T00:00:00")
                utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
                collected_records.append({
                        "measurement":  "Skin Temperature Variation",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            "RelativeValue": float(temp_record.get("value", {}).get("nightlyRelative", 0))
                        }
                    })
            except Exception:
                logging.exception(f"Failed to process skin temp record: {temp_record}")
        logging.info("Recorded Skin Temperature Variation for date " + start_date_str + " to " + end_date_str)
    else:
        logging.warning("No Skin Temperature data for date range %s to %s", start_date_str, end_date_str)

    spo2_resp = request_data_from_fitbit('https://api.fitbit.com/1/user/-/spo2/date/' + start_date_str + '/' + end_date_str + '/all.json')
    if spo2_resp:
        try:
            for days in spo2_resp:
                data = days.get("minutes", [])
                for record in data:
                    try:
                        log_time = datetime.fromisoformat(record.get("minute"))
                        utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
                        collected_records.append({
                                "measurement":  "SPO2_Intraday",
                                "time": utc_time,
                                "tags": {
                                    "Device": DEVICENAME
                                },
                                "fields": {
                                    "value": float(record.get("value", 0)),
                                }
                            })
                    except Exception:
                        logging.exception(f"Failed to process SPO2 intraday record: {record}")
            logging.info("Recorded SPO2 intraday for date " + start_date_str + " to " + end_date_str)
        except Exception:
            logging.exception("Unexpected SPO2 response structure; skipping SPO2 intraday processing")
    else:
        logging.warning("No SPO2 intraday data available for date range %s to %s", start_date_str, end_date_str)

# Only for sleep data - limit 100 days - 1 query
def get_daily_data_limit_100d(start_date_str, end_date_str):

    sleep_resp = request_data_from_fitbit('https://api.fitbit.com/1.2/user/-/sleep/date/' + start_date_str + '/' + end_date_str + '.json')
    sleep_data = sleep_resp.get("sleep") if isinstance(sleep_resp, dict) else None
    if sleep_data:
        for record in sleep_data:
            log_time = datetime.fromisoformat(record["startTime"])
            utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
            try:
                minutesLight= record['levels']['summary']['light']['minutes']
                minutesREM = record['levels']['summary']['rem']['minutes']
                minutesDeep = record['levels']['summary']['deep']['minutes']
            except:
                minutesLight= record['levels']['summary']['asleep']['minutes']
                minutesREM = record['levels']['summary']['restless']['minutes']
                minutesDeep = 0

            collected_records.append({
                    "measurement":  "Sleep Summary",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME,
                        "isMainSleep": record["isMainSleep"],
                    },
                    "fields": {
                        'efficiency': float(record["efficiency"]),
                        'minutesAfterWakeup': float(record['minutesAfterWakeup']),
                        'minutesAsleep': float(record['minutesAsleep']),
                        'minutesToFallAsleep': float(record['minutesToFallAsleep']),
                        'minutesInBed': float(record['timeInBed']),
                        'minutesAwake': float(record['minutesAwake']),
                        'minutesLight': float(minutesLight),
                        'minutesREM': float(minutesREM),
                        'minutesDeep': float(minutesDeep)
                    }
                })
            
            sleep_level_mapping = {'wake': 3, 'rem': 2, 'light': 1, 'deep': 0, 'asleep': 1, 'restless': 2, 'awake': 3, 'unknown': 4}
            for sleep_stage in record['levels']['data']:
                log_time = datetime.fromisoformat(sleep_stage["dateTime"])
                utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
                collected_records.append({
                        "measurement":  "Sleep Levels",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME,
                            "isMainSleep": record["isMainSleep"],
                        },
                        "fields": {
                            'level': float(sleep_level_mapping[sleep_stage["level"]]),
                            'duration_seconds': float(sleep_stage["seconds"])
                        }
                    })
            wake_time = datetime.fromisoformat(record["endTime"])
            utc_wake_time = LOCAL_TIMEZONE.localize(wake_time).astimezone(pytz.utc).isoformat()
            collected_records.append({
                        "measurement":  "Sleep Levels",
                        "time": utc_wake_time,
                        "tags": {
                            "Device": DEVICENAME,
                            "isMainSleep": record["isMainSleep"],
                        },
                        "fields": {
                            'level': float(sleep_level_mapping['wake']),
                            'duration_seconds': None
                        }
                    })
        logging.info("Recorded Sleep data for date " + start_date_str + " to " + end_date_str)
    else:
        logging.error("Recording failed : Sleep data for date " + start_date_str + " to " + end_date_str)

# Max date range 1 year, records HR zones, Activity minutes and Resting HR - 4 + 3 + 1 + 1 = 9 queries
def get_daily_data_limit_365d(start_date_str, end_date_str):
    activity_minutes_list = ["minutesSedentary", "minutesLightlyActive", "minutesFairlyActive", "minutesVeryActive"]
    for activity_type in activity_minutes_list:
        activity_resp = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/tracker/' + activity_type + '/date/' + start_date_str + '/' + end_date_str + '.json')
        activity_minutes_data_list = activity_resp.get("activities-tracker-"+activity_type) if isinstance(activity_resp, dict) else None
        if activity_minutes_data_list:
            for data in activity_minutes_data_list:
                try:
                    log_time = datetime.fromisoformat(data["dateTime"] + "T00:00:00")
                    utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
                    collected_records.append({
                            "measurement": "Activity Minutes",
                            "time": utc_time,
                            "tags": {
                                "Device": DEVICENAME
                            },
                            "fields": {
                                activity_type : float(data.get("value", 0))
                            }
                        })
                except Exception:
                    logging.exception(f"Failed to process activity minutes record: {data}")
            logging.info("Recorded " + activity_type + "for date " + start_date_str + " to " + end_date_str)
        else:
            logging.warning("No activity minutes data for %s in range %s to %s", activity_type, start_date_str, end_date_str)
        

    activity_others_list = ["distance", "calories", "steps"]
    for activity_type in activity_others_list:
        activity_resp = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/tracker/' + activity_type + '/date/' + start_date_str + '/' + end_date_str + '.json')
        activity_others_data_list = activity_resp.get("activities-tracker-"+activity_type) if isinstance(activity_resp, dict) else None
        if activity_others_data_list:
            for data in activity_others_data_list:
                try:
                    logging.info(data)
                    log_time = datetime.fromisoformat(data["dateTime"] + "T00:00:00")
                    utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
                    activity_name = "Total Steps" if activity_type == "steps" else activity_type
                    collected_records.append({
                            "measurement": activity_name,
                            "time": utc_time,
                            "tags": {
                                "Device": DEVICENAME
                            },
                            "fields": {
                                "value" : float(data.get("value", 0))
                            }
                        })
                except Exception:
                    logging.exception(f"Failed to process activity other record: {data}")
            logging.info("Recorded " + activity_name + " for date " + start_date_str + " to " + end_date_str)
        else:
            logging.warning("No activity data for %s in range %s to %s", activity_type, start_date_str, end_date_str)
        

    hr_resp = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/heart/date/' + start_date_str + '/' + end_date_str + '.json')
    HR_zones_data_list = hr_resp.get("activities-heart") if isinstance(hr_resp, dict) else None
    if HR_zones_data_list:
        for data in HR_zones_data_list:
            try:
                log_time = datetime.fromisoformat(data["dateTime"] + "T00:00:00")
                utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
                zones = data.get("value", {}).get("heartRateZones", [])
                fields = {}
                # Safely extract zone minutes
                for idx, name in enumerate(["Normal", "Fat Burn", "Cardio", "Peak"]):
                    try:
                        fields[name] = float(zones[idx].get("minutes", 0)) if idx < len(zones) else 0.0
                    except Exception:
                        fields[name] = 0.0

                collected_records.append({
                        "measurement": "HR zones",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": fields
                    })
                if "restingHeartRate" in data.get("value", {}):
                    collected_records.append({
                                "measurement":  "RestingHR",
                                "time": utc_time,
                                "tags": {
                                    "Device": DEVICENAME
                                },
                                "fields": {
                                    "value": float(data.get("value", {}).get("restingHeartRate", 0))
                                }
                            })
            except Exception:
                logging.exception(f"Failed to process HR zones record: {data}")
        logging.info("Recorded RHR and HR zones for date " + start_date_str + " to " + end_date_str)
    else:
        logging.warning("No HR zones data for date range %s to %s", start_date_str, end_date_str)

# records SPO2 single days for the whole given period - 1 query
def get_daily_data_limit_none(start_date_str, end_date_str):
    data_list = request_data_from_fitbit('https://api.fitbit.com/1/user/-/spo2/date/' + start_date_str + '/' + end_date_str + '.json')
    if data_list != None:
        for data in data_list:
            log_time = datetime.fromisoformat(data["dateTime"] + "T00:00:00")
            utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
            collected_records.append({
                    "measurement":  "SPO2",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "avg": data["value"]["avg"],
                        "max": data["value"]["max"],
                        "min": data["value"]["min"]
                    }
                })
        logging.info("Recorded Avg SPO2 for date " + start_date_str + " to " + end_date_str)
    else:
        logging.error("Recording failed : Avg SPO2 for date " + start_date_str + " to " + end_date_str)

# Fetches latest activities from record ( upto last 100 )
def fetch_latest_activities(end_date_str):
    recent_activities_data = request_data_from_fitbit('https://api.fitbit.com/1/user/-/activities/list.json', params={'beforeDate': end_date_str, 'sort':'desc', 'limit':50, 'offset':0})
    if recent_activities_data != None:
        for activity in recent_activities_data['activities']:
            fields = {}
            if 'activeDuration' in activity:
                fields['ActiveDuration'] = float(activity['activeDuration'])
            if 'averageHeartRate' in activity:
                fields['AverageHeartRate'] = float(activity['averageHeartRate'])
            if 'calories' in activity:
                fields['calories'] = float(activity['calories'])
            if 'duration' in activity:
                fields['duration'] = float(activity['duration'])
            if 'distance' in activity:
                fields['distance'] = float(activity['distance'])
            if 'steps' in activity:
                fields['steps'] = float(activity['steps'])
            starttime = datetime.fromisoformat(activity['startTime'].strip("Z"))
            utc_time = starttime.astimezone(pytz.utc).isoformat()
            collected_records.append({
                "measurement": "Activity Records",
                "time": utc_time,
                "tags": {
                    "ActivityName": activity['activityName']
                },
                "fields": fields
            })
        logging.info("Fetched 50 recent activities before date " + end_date_str)
    else:
        logging.error("Fetching 50 recent activities failed : before date " + end_date_str)

#Get Weight Data
def fetch_weight_logs(start_date_str, end_date_str):
    weight_resp = request_data_from_fitbit(f'https://api.fitbit.com/1/user/-/body/log/weight/date/{start_date_str}/{end_date_str}.json')
    weight_data_list = weight_resp.get("weight") if isinstance(weight_resp, dict) else None
    weight_goal_resp = request_data_from_fitbit(f'https://api.fitbit.com/1/user/-/body/log/weight/goal.json')
    weight_goal = weight_goal_resp.get("goal") if isinstance(weight_goal_resp, dict) else None
    form_data = ''

    if not weight_data_list:
        logging.warning(f"No weight data available for date range {start_date_str} to {end_date_str}")
        return

    for weight in weight_data_list:
        if not weight:
            continue
        try:
            log_time = datetime.fromisoformat(weight.get("date") + "T00:00:00")
            utc_time = safe_to_utc(log_time, LOCAL_TIMEZONE)
            logging.debug(log_time)
            logging.debug(utc_time)
            collected_records.append({
                "measurement": "Weight",
                "time": utc_time,
                "tags": {
                    "Device": DEVICENAME    
                },
                "fields": {
                    "weight": float(weight.get("weight", 0)),
                    "goal": int(float(weight_goal.get("weight", 130i))) if weight_goal and isinstance(weight_goal, dict) else 130i,
                    "bmi": float(weight.get("bmi")) if weight.get("bmi") is not None else None,
                    "fat": float(weight.get("fat")) if weight.get("fat") is not None else None,
                }
            })
            form_data = {
                "entry.1406463651": end_date_str,  # Replace with the actual field ID and value
                "entry.1062141579": weight.get("weight")
            }
        except Exception:
            logging.exception(f"Failed to process weight record: {weight}")

    if GOOGLE_FORM_URL and form_data:
        try:
            response = requests.post(GOOGLE_FORM_URL, data=form_data)
            response.raise_for_status()
            logging.info(f"Form submitted successfully! {start_date_str} to {end_date_str}")
        except requests.RequestException as e:
            logging.error(f"Failed to submit the form: {e}")
    else:
        logging.info("No Google form set up or no form data to submit.")

    logging.info(f"Recorded weight logs for date {start_date_str} to {end_date_str}")
        
# Get food data from MFP
def fetch_myfitnesspal_food(date):
    if mfp_client is None:
        logging.warning("MyFitnessPal client not configured; skipping fetch_myfitnesspal_food.")
        return []

    day = mfp_client.get_date(date.year, date.month, date.day)
    foods = []
    for entry in day.entries:
        logging.info(f"Fetching food entry: {entry.name} on {date.strftime('%Y-%m-%d')}")
        foods.append({
            "name": entry.name,
            "calories": entry.totals.get('calories', 0),
            # Add more nutrients if needed
        })
    return foods

def log_food_to_fitbit(food_name, calories, date_str):
    payload = {
        "foodName": food_name,
        "mealTypeId": 7,  # 1=Breakfast, ..., 7=Other
        "unitId": 304,    # 304 = "serving"
        "amount": 1,
        "date": date_str,
        "calories": calories
    }
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    url = f"https://api.fitbit.com/1/user/-/foods/log.json"
    response = requests.post(url, data=payload, headers=headers)
    if response.status_code == 201:
        logging.info(f"Logged {food_name} to Fitbit.")
    else:
        logging.error(f"Failed to log {food_name}: {response.status_code} {response.text}")



# %% [markdown]
# ## Call the functions one time as a startup update OR do switch to bulk update mode

# %%
if AUTO_DATE_RANGE:
    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]
    if len(date_list) > 3:
        logging.warn("Auto schedule update is not meant for more than 3 days at a time, please consider lowering the auto_update_date_range variable to aviod rate limit hit!")
    
    if DEBUG_MODE:
        logging.info("DEBUG MODE ENABLED: Loading records from file instead of calling Fitbit API.")
        debug_records = load_collected_records_from_file(f"fitbit_collected_{start_date_str}_to_{end_date_str}.json")
        write_points_to_influxdb(debug_records)
    else:
        for date_str in date_list:
            # sync_mfp_to_fitbit(date_str, ACCESS_TOKEN)
            # mfp_totals = sync_mfp_to_fitbit(date_str, ACCESS_TOKEN)
            # if mfp_totals:
            #     add_mfp_totals_to_influxdb(
            #         date_str,
            #         mfp_totals,
            #         device_name="MyFitnessPal",
            #         influxdb_version=INFLUXDB_VERSION,
            #         influxdb_write_api=influxdb_write_api,
            #         influxdb_bucket=INFLUXDB_BUCKET,
            #         influxdb_org=INFLUXDB_ORG,
            #         influxdbclient=influxdbclient
            #     )
            get_intraday_data_limit_1d(date_str, [('heart','HeartRate_Intraday','1sec'),('steps','Steps_Intraday','1min')]) # 2 queries x number of dates ( default 2)
        get_daily_data_limit_30d(start_date_str, end_date_str) # 3 queries
        get_daily_data_limit_100d(start_date_str, end_date_str) # 1 query
        get_daily_data_limit_365d(start_date_str, end_date_str) # 8 queries
        get_daily_data_limit_none(start_date_str, end_date_str) # 1 query
        get_battery_level() # 1 query
        fetch_weight_logs(start_date_str, end_date_str)
        fetch_latest_activities(end_date_str)
        dump_collected_records_to_file(f"fitbit_collected_{start_date_str}_to_{end_date_str}.json")
        write_points_to_influxdb(collected_records)
        collected_records = []
else:
    # Do Bulk update----------------------------------------------------------------------------------------------------------------------------

    schedule.every(1).hours.do(lambda : Get_New_Access_Token(client_id,client_secret)) # Auto-refresh tokens every 1 hour
    
    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]


    def yield_dates_with_gap(date_list, gap):
        start_index = -1*gap
        while start_index < len(date_list)-1:
            start_index  = start_index + gap
            end_index = start_index+gap
            if end_index > len(date_list) - 1:
                end_index = len(date_list) - 1
            if start_index > len(date_list) - 1:
                break
            yield (date_list[start_index],date_list[end_index])

    def do_bulk_update(funcname, start_date, end_date):
        global collected_records
        funcname(start_date, end_date)
        schedule.run_pending()
        write_points_to_influxdb(collected_records)
        collected_records = []

    do_bulk_update(fetch_weight_logs, date_list[0], date_list[-1])
    fetch_latest_activities(date_list[-1])
    write_points_to_influxdb(collected_records)
    do_bulk_update(get_daily_data_limit_none, date_list[0], date_list[-1])
    for date_range in yield_dates_with_gap(date_list, 360):
        do_bulk_update(get_daily_data_limit_365d, date_range[0], date_range[1])
    for date_range in yield_dates_with_gap(date_list, 98):
        do_bulk_update(get_daily_data_limit_100d, date_range[0], date_range[1])
    for date_range in yield_dates_with_gap(date_list, 28):
        do_bulk_update(get_daily_data_limit_30d, date_range[0], date_range[1])
    for single_day in date_list:
        # sync_mfp_to_fitbit(single_day, ACCESS_TOKEN)
        # mfp_totals = sync_mfp_to_fitbit(single_day, ACCESS_TOKEN)
        # if mfp_totals:
        #     add_mfp_totals_to_influxdb(
        #         single_day,
        #         mfp_totals,
        #         device_name="MyFitnessPal",
        #         influxdb_version=INFLUXDB_VERSION,
        #         influxdb_write_api=influxdb_write_api,
        #         influxdb_bucket=INFLUXDB_BUCKET,
        #         influxdb_org=INFLUXDB_ORG,
        #         influxdbclient=influxdbclient
        #     )
        do_bulk_update(get_intraday_data_limit_1d, single_day, [('heart','HeartRate_Intraday','1sec'),('steps','Steps_Intraday','1min')])
    dump_collected_records_to_file(f"fitbit_collected_{start_date_str}_to_{end_date_str}.json")
    logging.info("Success : Bulk update complete for " + start_date_str + " to " + end_date_str)
    print("Bulk update complete!")

# %% [markdown]
# ## Schedule functions at specific intervals (Ongoing continuous update)

# %%
# Ongoing continuous update of data
if SCHEDULE_AUTO_UPDATE:
    
    schedule.every(1).hours.do(lambda : Get_New_Access_Token(client_id,client_secret)) # Auto-refresh tokens every 1 hour
    schedule.every(3).minutes.do( lambda : get_intraday_data_limit_1d(end_date_str, [('heart','HeartRate_Intraday','1sec'),('steps','Steps_Intraday','1min')] )) # Auto-refresh detailed HR and steps
    schedule.every(1).hours.do( lambda : get_intraday_data_limit_1d((datetime.strptime(end_date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"), [('heart','HeartRate_Intraday','1sec'),('steps','Steps_Intraday','1min')] )) # Refilling any missing data on previous day end of night due to fitbit sync delay ( see issue #10 )
    schedule.every(20).minutes.do(get_battery_level) # Auto-refresh battery level
    schedule.every(3).hours.do(lambda: (get_daily_data_limit_30d(start_date_str, end_date_str)))
    # def scheduled_sync_mfp_to_fitbit():
    #     sync_mfp_to_fitbit(end_date_str, ACCESS_TOKEN)
    #     mfp_totals = sync_mfp_to_fitbit(end_date_str, ACCESS_TOKEN)
    #     if mfp_totals:
    #         add_mfp_totals_to_influxdb(
    #             end_date_str,
    #             mfp_totals,
    #             device_name="MyFitnessPal",
    #             influxdb_version=INFLUXDB_VERSION,
    #             influxdb_write_api=influxdb_write_api,
    #             influxdb_bucket=INFLUXDB_BUCKET,
    #             influxdb_org=INFLUXDB_ORG,
    #             influxdbclient=influxdbclient
    #         )
    # schedule.every(3).hours.do(scheduled_sync_mfp_to_fitbit)
    schedule.every(4).hours.do(lambda : get_daily_data_limit_100d(start_date_str, end_date_str))
    schedule.every(1).hours.do( lambda : get_daily_data_limit_365d(start_date_str, end_date_str))
    schedule.every(6).hours.do(lambda : get_daily_data_limit_none(start_date_str, end_date_str))
    schedule.every(1).hours.do( lambda : fetch_latest_activities(end_date_str))
    schedule.every(5).hours.do( lambda : fetch_weight_logs(start_date_str, end_date_str))

    while True:
        schedule.run_pending()
        if len(collected_records) != 0:
            write_points_to_influxdb(collected_records)
            collected_records = []
        time.sleep(30)
        update_working_dates()
        