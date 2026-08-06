# Google Health API fetcher for Home Assistant
# Migrated from the Fitbit Web API fetcher (Fitbit_Fetch.py).
#
# The Fitbit Web API is turned down in September 2026. Cloud access to Fitbit /
# Pixel data now lives behind the Google Health API (health.googleapis.com/v4),
# authenticated with Google OAuth 2.0.  This script keeps the original InfluxDB
# schema (measurement + field names) so existing Grafana / Home Assistant
# dashboards keep working, but talks to the new API underneath.
#
# See MIGRATION.md for setup, the field-by-field mapping, and the known gaps
# (device battery level, weight goal and BMI have no Google Health equivalent).

import os
import sys
import time
import threading
import logging
import schedule
import requests
import json
import pytz
from urllib.parse import urlencode
from requests.exceptions import ConnectionError, ReadTimeout
from datetime import datetime, timedelta

# for influxdb 1.x
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
# for influxdb 2.x
from influxdb_client import InfluxDBClient as InfluxDBClient2
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS

# %% [markdown]
# ## Variables

# %%
LOG_FILE_PATH = os.environ.get("FITBIT_LOG_FILE_PATH") or "your/expected/log/file/location/path"
TOKEN_FILE_PATH = os.environ.get("TOKEN_FILE_PATH") or "your/expected/token/file/location/path"
OVERWRITE_LOG_FILE = True

INFLUXDB_VERSION = os.environ.get("INFLUXDB_VERSION") or "2"  # supported values are 1 or 2
# Update these variables for influxdb 1.x versions
INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST") or 'localhost'
INFLUXDB_PORT = os.environ.get("INFLUXDB_PORT") or 8086
INFLUXDB_USERNAME = os.environ.get("INFLUXDB_USERNAME") or 'your_influxdb_username'
INFLUXDB_PASSWORD = os.environ.get("INFLUXDB_PASSWORD") or 'your_influxdb_password'
INFLUXDB_DATABASE = os.environ.get("INFLUXDB_DATABASE") or 'your_influxdb_database_name'
# Update these variables for influxdb 2.x versions
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET") or "your_bucket_name_here"
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG") or "your_org_here"
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN") or "your_token_here"
INFLUXDB_URL = os.environ.get("INFLUXDB_URL") or "http://homeassistant.local:8086"

# --- Google OAuth 2.0 credentials (from your Google Cloud project) ---
# These replace the old Fitbit client_id / client_secret. See MIGRATION.md.
CLIENT_ID = os.environ.get("CLIENT_ID") or "your_google_oauth_client_id"
CLIENT_SECRET = os.environ.get("CLIENT_SECRET") or "your_google_oauth_client_secret"

DEVICENAME = os.environ.get("DEVICENAME") or "Your_Device_Name"  # e.g. "Charge5" (used as an InfluxDB tag only)

AUTO_DATE_RANGE = True  # Automatically selects date range from today's date and auto_update_date_range
auto_update_date_range = 1  # Days to go back from today for AUTO_DATE_RANGE. Keep this small (<=2).
LOCAL_TIMEZONE = os.environ.get("LOCAL_TIMEZONE") or "Automatic"
SCHEDULE_AUTO_UPDATE = True if AUTO_DATE_RANGE else False
SERVER_ERROR_MAX_RETRY = 3
EXPIRED_TOKEN_MAX_RETRY = 5
SKIP_REQUEST_ON_SERVER_ERROR = True

# Optional weight sync to a Google Form (unchanged from the original addon)
GOOGLE_FORM_URL = os.environ.get("GOOGLE_FORM_URL")

DEBUG_MODE = False
COLLECTED_RECORDS_FILE_PATH = os.environ.get("COLLECTED_RECORDS_FILE_PATH") or "./debug"

# --- Google Health API constants ---
GHEALTH_BASE = "https://health.googleapis.com/v4/users/me"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
# Scopes required for the metrics this addon reads. Every googlehealth.* scope is
# "Restricted" and needs a Google privacy/security review before production use.
GHEALTH_SCOPES = [
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",
    "https://www.googleapis.com/auth/googlehealth.sleep.readonly",
]

ACCESS_TOKEN = ""  # populated by Get_New_Access_Token() below

# --- Debugging: Save/load collected_records to/from file ---
def dump_collected_records_to_file(filename="collected_records.json"):
    os.makedirs(COLLECTED_RECORDS_FILE_PATH, exist_ok=True)
    full_path = os.path.join(COLLECTED_RECORDS_FILE_PATH, filename)
    with open(full_path, "w") as f:
        json.dump(collected_records, f, indent=2)
    logging.info(f"Dumped {len(collected_records)} records to {full_path}")

def load_collected_records_from_file(filename="collected_records.json"):
    full_path = os.path.join(COLLECTED_RECORDS_FILE_PATH, filename)
    with open(full_path, "r") as f:
        return json.load(f)

# %% [markdown]
# ## Logging setup

# %%
if OVERWRITE_LOG_FILE:
    with open(LOG_FILE_PATH, "w"):
        pass

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode='a'),
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
            buckets = influxdbclient.buckets_api().find_buckets()
            logging.info(f"Connected to InfluxDB v2. Found {len(buckets.buckets)} buckets.")
        elif INFLUXDB_VERSION == "1":
            influxdbclient.ping()
            logging.info("Connected to InfluxDB v1.")
        else:
            raise ValueError("Unsupported InfluxDB version.")
    except Exception as e:
        logging.critical(f"InfluxDB connection test failed: {e}")
        sys.exit(1)

# Variables for API request monitoring
api_request_count = 0
API_REQUEST_LIMIT = 150  # conservative per-hour cap; adjust to your Google Health quota
api_request_lock = threading.Lock()

def reset_api_request_count():
    global api_request_count
    with api_request_lock:
        api_request_count = 0
    threading.Timer(3600, reset_api_request_count).start()

reset_api_request_count()

# %% [markdown]
# ## Generic Google Health API caller

# %%
def request_data_from_ghealth(url, params=None, request_type="get"):
    """Generic GET caller for Google Health API endpoints.

    Handles the shared concerns: bearer auth, the local rate-limit counter,
    401 (token refresh), 429 (rate limit) and 5xx (server) backoff.  Returns the
    parsed JSON dict on success, or None on a permanent-but-non-fatal failure
    (e.g. 403 permission denied for a scope the user did not grant).
    """
    global ACCESS_TOKEN, api_request_count
    params = params or {}
    retry_attempts = 0
    logging.debug("Requesting data from Google Health via URL : " + url)

    while True:
        headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Accept": "application/json",
        }
        try:
            with api_request_lock:
                if api_request_count >= API_REQUEST_LIMIT:
                    logging.info("API Limit Reached. Waiting for reset.")
                    time.sleep(60)
                    continue
                else:
                    api_request_count += 1

            if request_type == "get":
                response = requests.get(url, headers=headers, params=params, timeout=60)
            else:
                raise Exception("Invalid request type " + str(request_type))

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logging.debug(f"Response headers: {response.headers}")
                retry_after = int(response.headers.get("Retry-After", 60)) + 300
                logging.warning(f"Google Health API limit reached (429). Retrying in {retry_after} seconds.")
                time.sleep(retry_after)
            elif response.status_code == 401:
                logging.warning("Error code : 401, Details : " + response.text)
                ACCESS_TOKEN = Get_New_Access_Token(CLIENT_ID, CLIENT_SECRET)
                logging.info("Refreshed Google access token after 401.")
                time.sleep(15)
                if retry_attempts > EXPIRED_TOKEN_MAX_RETRY:
                    logging.error("Unable to solve the 401 Error. Please debug - " + response.text)
                    raise Exception("Unable to solve the 401 Error. Please debug - " + response.text)
            elif response.status_code in [500, 502, 503, 504]:
                logging.warning("Server Error encountered (Code 5xx): Retrying after 120 seconds....")
                time.sleep(120)
                if retry_attempts > SERVER_ERROR_MAX_RETRY:
                    logging.error("Unable to solve the server Error. Retry limit exceeded - " + response.text)
                    if SKIP_REQUEST_ON_SERVER_ERROR:
                        logging.warning("Retry limit reached for server error : Skipping request -> " + url)
                        return None
            else:
                logging.error("Google Health API request failed. Status code: " + str(response.status_code) + " " + str(response.text))
                if response.status_code in (400, 403, 404):
                    # 400 -> unsupported method for this data type / bad filter
                    # 403 -> scope not granted for this data type
                    # 404 -> no data / data type not yet exposed
                    logging.warning(f"Non-fatal {response.status_code} for {url}. Continuing without this data.")
                    return None
                try:
                    response.raise_for_status()
                except Exception:
                    logging.exception("Unhandled HTTP error from Google Health API")
                    raise
                return None

        except (ConnectionError, ReadTimeout) as e:
            logging.error("Retrying in 5 minutes - Network error : " + str(e))
        retry_attempts += 1
        time.sleep(30)

# --- Helpers for the Google Health resource shape -------------------------------

def _iso_z(dt):
    """UTC ISO-8601 with a trailing Z, e.g. 2026-05-12T00:00:00Z."""
    return dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _to_float(value, default=None):
    """Google serializes 64-bit ints (beatsPerMinute, steps) as strings."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def _first(d, *keys, default=None):
    """Return the first present, non-None key from a dict (schema is still evolving)."""
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def gh_list(data_type, start_dt, end_dt, extra_params=None):
    """List raw data points for a data type over [start_dt, end_dt).

    Uses the documented list form:
      GET .../dataTypes/{type}/dataPoints?startTime=&endTime=&pageSize=
    Follows nextPageToken pagination. Returns a flat list of dataPoint dicts.

    NOTE: max query range is 14 days for heart-rate / active-minutes /
    total-calories / calories-in-heart-rate-zone, and 90 days for everything
    else. Callers are responsible for chunking; see the *_limit_* wrappers.
    """
    points = []
    page_token = None
    while True:
        params = {
            "startTime": _iso_z(start_dt),
            "endTime": _iso_z(end_dt),
            "pageSize": "10000",
        }
        if extra_params:
            params.update(extra_params)
        if page_token:
            params["pageToken"] = page_token
        url = f"{GHEALTH_BASE}/dataTypes/{data_type}/dataPoints"
        resp = request_data_from_ghealth(url, params=params)
        if not resp:
            break
        points.extend(resp.get("dataPoints", []) or [])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return points

def gh_daily_rollup(data_type, start_dt, end_dt):
    """Fetch per-day aggregates via the dailyRollUp method.

    Google exposes rollups through a custom verb on the collection:
      GET .../dataTypes/{type}/dataPoints:dailyRollUp?startTime=&endTime=
    The rollup response wraps points under "dataPoints" (each a per-day bucket).
    The exact per-type payload keys are read defensively by the callers below.
    """
    points = []
    page_token = None
    while True:
        params = {
            "startTime": _iso_z(start_dt),
            "endTime": _iso_z(end_dt),
        }
        if page_token:
            params["pageToken"] = page_token
        url = f"{GHEALTH_BASE}/dataTypes/{data_type}/dataPoints:dailyRollUp"
        resp = request_data_from_ghealth(url, params=params)
        if not resp:
            break
        points.extend(resp.get("dataPoints", []) or [])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return points

# %% [markdown]
# ## Google OAuth 2.0 Token Management

# %%
def refresh_google_tokens(client_id, client_secret, refresh_token):
    logging.info("Attempting to refresh Google tokens...")
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    resp = requests.post(
        GOOGLE_TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=60,
    )
    if resp.status_code != 200:
        logging.error(f"Token refresh failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()
    json_data = resp.json()
    access_token = json_data["access_token"]
    # Google usually returns the same refresh_token; keep the old one if absent.
    new_refresh_token = json_data.get("refresh_token", refresh_token)
    tokens = {"access_token": access_token, "refresh_token": new_refresh_token}
    with open(TOKEN_FILE_PATH, "w") as file:
        json.dump(tokens, file)
    logging.info("Google token refresh successful!")
    return access_token, new_refresh_token

def load_tokens_from_file():
    with open(TOKEN_FILE_PATH, "r") as file:
        tokens = json.load(file)
        return tokens.get("access_token"), tokens.get("refresh_token")

def Get_New_Access_Token(client_id, client_secret):
    try:
        _, refresh_token = load_tokens_from_file()
    except FileNotFoundError:
        refresh_token = input("No token file found. Please enter a valid Google refresh token : ")
    if not refresh_token:
        raise RuntimeError("No Google refresh token available. Complete the OAuth consent flow (see MIGRATION.md).")
    access_token, _ = refresh_google_tokens(client_id, client_secret, refresh_token)
    return access_token

ACCESS_TOKEN = Get_New_Access_Token(CLIENT_ID, CLIENT_SECRET)

# %% [markdown]
# ## InfluxDB Database Initialization

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
        filename = f"failed_records_{int(time.time())}.json"
        dump_collected_records_to_file(filename)
        logging.error(f"{description} failed after {MAX_RETRIES} attempts. Dumped to {filename}")

    if not points:
        return
    if INFLUXDB_VERSION == "2":
        retry_write(lambda: influxdb_write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points), "InfluxDB v2 write")
    elif INFLUXDB_VERSION == "1":
        retry_write(lambda: influxdbclient.write_points(points), "InfluxDB v1 write")
    else:
        logging.error("Unsupported InfluxDB version. Must be 1 or 2.")
        raise InfluxDBClientError("Unsupported InfluxDB version. Must be 1 or 2.")

# %% [markdown]
# ## Timezone

# %%
# Google Health returns physicalTime already in UTC (with a separate utcOffset),
# so intraday/sample records are stored directly as UTC. LOCAL_TIMEZONE is only
# used to place "daily" values (which carry a civil date, not a timestamp) at
# local midnight, matching the original addon's behaviour.
if LOCAL_TIMEZONE == "Automatic":
    logging.warning("LOCAL_TIMEZONE='Automatic' is not supported against Google Health (no profile timezone read). Defaulting to UTC. Set local_timezone explicitly (e.g. 'Europe/Berlin').")
    LOCAL_TIMEZONE = pytz.UTC
else:
    LOCAL_TIMEZONE = pytz.timezone(LOCAL_TIMEZONE)

def parse_physical_time(sample):
    """Parse a Google Health {physicalTime: '...Z'} into a UTC ISO string."""
    pt = _first(sample, "physicalTime", "startTime", "time")
    if not pt:
        return None
    dt = datetime.fromisoformat(pt.replace("Z", "+00:00"))
    return dt.astimezone(pytz.utc).isoformat()

def local_date_to_utc(date_str, hour=0, minute=0):
    """A civil date (YYYY-MM-DD) placed at local midnight, expressed in UTC."""
    naive = datetime.fromisoformat(f"{date_str}T{hour:02d}:{minute:02d}:00")
    return LOCAL_TIMEZONE.localize(naive).astimezone(pytz.utc).isoformat()

# %% [markdown]
# ## Date selection

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

collected_records = []

def update_working_dates():
    global end_date, start_date, end_date_str, start_date_str
    end_date = datetime.now(LOCAL_TIMEZONE)
    start_date = end_date - timedelta(days=auto_update_date_range)
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date_str = start_date.strftime("%Y-%m-%d")

def _day_bounds(date_str):
    """Return (start_dt, end_dt) spanning one civil day in LOCAL_TIMEZONE."""
    start = LOCAL_TIMEZONE.localize(datetime.fromisoformat(date_str + "T00:00:00"))
    return start, start + timedelta(days=1)

def _range_bounds(start_date_str, end_date_str):
    start = LOCAL_TIMEZONE.localize(datetime.fromisoformat(start_date_str + "T00:00:00"))
    end = LOCAL_TIMEZONE.localize(datetime.fromisoformat(end_date_str + "T00:00:00")) + timedelta(days=1)
    return start, end

# %% [markdown]
# ## Data collection functions (Google Health -> original InfluxDB schema)

# %%

# NOTE: The Google Health "devices" concept has no read endpoint, so the
# original DeviceBatteryLevel measurement can no longer be populated. This is a
# hard gap in the new API (see MIGRATION.md). Kept as a no-op for clarity.
def get_battery_level():
    logging.info("Device battery level is not available in the Google Health API; skipping.")

# Intraday heart rate + steps for a single day.
# heart-rate (Sample kind) has a 14-day max range, so per-day is safe.
def get_intraday_data_limit_1d(date_str, measurement_list=None):
    start_dt, end_dt = _day_bounds(date_str)

    # Heart rate -> HeartRate_Intraday
    hr_points = gh_list("heart-rate", start_dt, end_dt)
    for p in hr_points:
        hr = p.get("heartRate", {})
        utc_time = parse_physical_time(hr.get("sampleTime", {}))
        bpm = _to_float(hr.get("beatsPerMinute"))
        if utc_time is None or bpm is None:
            continue
        collected_records.append({
            "measurement": "HeartRate_Intraday",
            "time": utc_time,
            "tags": {"Device": DEVICENAME},
            "fields": {"value": bpm},
        })
    logging.info(f"Recorded {len(hr_points)} HeartRate_Intraday points for {date_str}")

    # Steps -> Steps_Intraday (Interval kind, countSum per interval)
    step_points = gh_list("steps", start_dt, end_dt)
    for p in step_points:
        st = p.get("steps", {})
        interval = st.get("interval", {})
        utc_time = parse_physical_time({"physicalTime": _first(interval, "startTime")})
        count = _to_float(_first(st, "countSum", "count"))
        if utc_time is None or count is None:
            continue
        collected_records.append({
            "measurement": "Steps_Intraday",
            "time": utc_time,
            "tags": {"Device": DEVICENAME},
            "fields": {"value": count},
        })
    logging.info(f"Recorded {len(step_points)} Steps_Intraday points for {date_str}")


# HRV, Breathing rate, skin temperature, SpO2 intraday. 90-day max range each.
def get_daily_data_limit_30d(start_date_str, end_date_str):
    start_dt, end_dt = _range_bounds(start_date_str, end_date_str)

    # HRV -> HRV (dailyRmssd, deepRmssd). Google exposes rmssd (ms).
    hrv_points = gh_list("daily-heart-rate-variability", start_dt, end_dt)
    for p in hrv_points:
        hrv = p.get("dailyHeartRateVariability", p.get("heartRateVariability", {}))
        date_str = _first(hrv, "date")
        rmssd = _to_float(_first(hrv, "rmssd", "dailyRmssd"))
        deep = _to_float(_first(hrv, "deepRmssd"))
        utc_time = local_date_to_utc(date_str, hour=4) if date_str else parse_physical_time(hrv.get("sampleTime", {}))
        if utc_time is None:
            continue
        collected_records.append({
            "measurement": "HRV",
            "time": utc_time,
            "tags": {"Device": DEVICENAME},
            "fields": {"dailyRmssd": rmssd, "deepRmssd": deep},
        })
    logging.info(f"Recorded {len(hrv_points)} HRV points for {start_date_str} to {end_date_str}")

    # Breathing rate -> BreathingRate
    br_points = gh_list("daily-respiratory-rate", start_dt, end_dt)
    for p in br_points:
        br = p.get("dailyRespiratoryRate", {})
        date_str = _first(br, "date")
        value = _to_float(_first(br, "breathingRate", "respiratoryRate", "value"))
        if date_str is None or value is None:
            continue
        collected_records.append({
            "measurement": "BreathingRate",
            "time": local_date_to_utc(date_str),
            "tags": {"Device": DEVICENAME},
            "fields": {"value": value},
        })
    logging.info(f"Recorded {len(br_points)} BreathingRate points for {start_date_str} to {end_date_str}")

    # Skin temperature variation -> Skin Temperature Variation (nightly relative)
    temp_points = gh_list("daily-sleep-temperature-derivations", start_dt, end_dt)
    for p in temp_points:
        temp = p.get("dailySleepTemperatureDerivations", p.get("dailySleepTemperatureDerivation", {}))
        date_str = _first(temp, "date")
        value = _to_float(_first(temp, "nightlyRelative", "relativeTemperatureCelsius", "value"))
        if date_str is None or value is None:
            continue
        collected_records.append({
            "measurement": "Skin Temperature Variation",
            "time": local_date_to_utc(date_str),
            "tags": {"Device": DEVICENAME},
            "fields": {"RelativeValue": value},
        })
    logging.info(f"Recorded {len(temp_points)} Skin Temperature Variation points for {start_date_str} to {end_date_str}")

    # SpO2 intraday -> SPO2_Intraday (oxygen-saturation Sample kind, percentage)
    spo2_points = gh_list("oxygen-saturation", start_dt, end_dt)
    for p in spo2_points:
        ox = p.get("oxygenSaturation", {})
        utc_time = parse_physical_time(ox.get("sampleTime", {}))
        value = _to_float(_first(ox, "percentage", "value"))
        if utc_time is None or value is None:
            continue
        collected_records.append({
            "measurement": "SPO2_Intraday",
            "time": utc_time,
            "tags": {"Device": DEVICENAME},
            "fields": {"value": value},
        })
    logging.info(f"Recorded {len(spo2_points)} SPO2_Intraday points for {start_date_str} to {end_date_str}")


# Sleep -> Sleep Summary + Sleep Levels (sleep Session kind)
def get_daily_data_limit_100d(start_date_str, end_date_str):
    start_dt, end_dt = _range_bounds(start_date_str, end_date_str)
    sleep_points = gh_list("sleep", start_dt, end_dt)
    if not sleep_points:
        logging.warning(f"No sleep data for {start_date_str} to {end_date_str}")
        return

    # Google upper-case stage enums -> the addon's original numeric mapping.
    stage_level_mapping = {
        'AWAKE': 3, 'WAKE': 3, 'REM': 2, 'LIGHT': 1, 'DEEP': 0,
        'ASLEEP': 1, 'RESTLESS': 2, 'UNKNOWN': 4, 'OUT_OF_BED': 3,
    }

    for p in sleep_points:
        sleep = p.get("sleep", {})
        interval = sleep.get("interval", {})
        start_time = _first(interval, "startTime")
        end_time = _first(interval, "endTime")
        if not start_time:
            continue
        utc_time = parse_physical_time({"physicalTime": start_time})

        summary = sleep.get("summary", {})
        stages_summary = summary.get("stagesSummary", []) or []
        stage_minutes = {s.get("type", "").upper(): _to_float(s.get("minutes"), 0.0) for s in stages_summary}
        minutesLight = stage_minutes.get("LIGHT", 0.0)
        minutesREM = stage_minutes.get("REM", 0.0)
        minutesDeep = stage_minutes.get("DEEP", 0.0)

        # isMainSleep is not part of Google's sleep schema; default True.
        is_main = bool(_first(sleep, "isMainSleep", default=True))

        collected_records.append({
            "measurement": "Sleep Summary",
            "time": utc_time,
            "tags": {"Device": DEVICENAME, "isMainSleep": is_main},
            "fields": {
                'efficiency': _to_float(_first(summary, "efficiency"), 0.0),
                'minutesAfterWakeup': _to_float(_first(summary, "minutesAfterWakeUp", "minutesAfterWakeup"), 0.0),
                'minutesAsleep': _to_float(_first(summary, "minutesAsleep"), 0.0),
                'minutesToFallAsleep': _to_float(_first(summary, "minutesToFallAsleep"), 0.0),
                'minutesInBed': _to_float(_first(summary, "minutesInSleepPeriod", "minutesInBed", "timeInBed"), 0.0),
                'minutesAwake': _to_float(_first(summary, "minutesAwake"), 0.0),
                'minutesLight': minutesLight,
                'minutesREM': minutesREM,
                'minutesDeep': minutesDeep,
            },
        })

        # Per-stage transitions -> Sleep Levels
        for stage in sleep.get("stages", []) or []:
            s_start = _first(stage, "startTime")
            s_end = _first(stage, "endTime")
            if not s_start:
                continue
            level_name = str(stage.get("type", "UNKNOWN")).upper()
            duration = None
            if s_start and s_end:
                d0 = datetime.fromisoformat(s_start.replace("Z", "+00:00"))
                d1 = datetime.fromisoformat(s_end.replace("Z", "+00:00"))
                duration = (d1 - d0).total_seconds()
            collected_records.append({
                "measurement": "Sleep Levels",
                "time": parse_physical_time({"physicalTime": s_start}),
                "tags": {"Device": DEVICENAME, "isMainSleep": is_main},
                "fields": {
                    'level': float(stage_level_mapping.get(level_name, 4)),
                    'duration_seconds': duration,
                },
            })

        # Final wake marker at end of sleep, mirroring the original addon.
        if end_time:
            collected_records.append({
                "measurement": "Sleep Levels",
                "time": parse_physical_time({"physicalTime": end_time}),
                "tags": {"Device": DEVICENAME, "isMainSleep": is_main},
                "fields": {'level': float(stage_level_mapping['AWAKE']), 'duration_seconds': None},
            })
    logging.info(f"Recorded Sleep data for {start_date_str} to {end_date_str}")


# Activity minutes, distance/calories/steps daily totals, HR zones, resting HR.
def get_daily_data_limit_365d(start_date_str, end_date_str):
    start_dt, end_dt = _range_bounds(start_date_str, end_date_str)

    # --- Activity Minutes (intensity buckets) -> Activity Minutes ---
    # Google 'activity-level' (Interval, list/reconcile) carries a per-interval
    # intensity level; we sum interval durations per civil day into the original
    # minutesSedentary/Lightly/Fairly/VeryActive fields.
    level_field_map = {
        'SEDENTARY': 'minutesSedentary',
        'LIGHT': 'minutesLightlyActive', 'LIGHTLY_ACTIVE': 'minutesLightlyActive',
        'MODERATE': 'minutesFairlyActive', 'FAIRLY_ACTIVE': 'minutesFairlyActive',
        'VIGOROUS': 'minutesVeryActive', 'VERY_ACTIVE': 'minutesVeryActive',
    }
    activity_points = gh_list("activity-level", start_dt, end_dt)
    per_day = {}
    for p in activity_points:
        al = p.get("activityLevel", {})
        interval = al.get("interval", {})
        s_start = _first(interval, "startTime")
        s_end = _first(interval, "endTime")
        level = str(_first(al, "level", "activityLevel", default="")).upper()
        field = level_field_map.get(level)
        if not (s_start and s_end and field):
            continue
        d0 = datetime.fromisoformat(s_start.replace("Z", "+00:00"))
        d1 = datetime.fromisoformat(s_end.replace("Z", "+00:00"))
        day_key = d0.astimezone(LOCAL_TIMEZONE).strftime("%Y-%m-%d")
        minutes = (d1 - d0).total_seconds() / 60.0
        per_day.setdefault(day_key, {}).setdefault(field, 0.0)
        per_day[day_key][field] += minutes
    for day_key, fields in per_day.items():
        collected_records.append({
            "measurement": "Activity Minutes",
            "time": local_date_to_utc(day_key),
            "tags": {"Device": DEVICENAME},
            "fields": {k: float(v) for k, v in fields.items()},
        })
    if per_day:
        logging.info(f"Recorded Activity Minutes for {start_date_str} to {end_date_str}")

    # --- Distance / Total calories / Steps daily totals ---
    # distance -> 'distance' (millimeters). Original stored the user's Fitbit
    # distance unit; here we convert mm to kilometers.
    for dtype, measurement, payload_key, value_keys, scale in [
        ("distance", "distance", "distance", ("distanceMillimeters", "distance"), 1e-6),
        ("total-calories", "calories", "totalCalories", ("caloriesKcal", "calories", "value"), 1.0),
        ("steps", "Total Steps", "steps", ("countSum", "count", "value"), 1.0),
    ]:
        rollups = gh_daily_rollup(dtype, start_dt, end_dt)
        for p in rollups:
            payload = p.get(payload_key, p)
            date_str = _first(payload, "date") or _first(p, "date")
            value = _to_float(_first(payload, *value_keys))
            if date_str is None or value is None:
                continue
            collected_records.append({
                "measurement": measurement,
                "time": local_date_to_utc(date_str),
                "tags": {"Device": DEVICENAME},
                "fields": {"value": float(value) * scale},
            })
        if rollups:
            logging.info(f"Recorded {measurement} for {start_date_str} to {end_date_str}")

    # --- HR zones -> HR zones (Normal/Fat Burn/Cardio/Peak minutes) ---
    hrz_points = gh_list("daily-heart-rate-zones", start_dt, end_dt)
    zone_name_map = {
        'OUT_OF_RANGE': 'Normal', 'NORMAL': 'Normal',
        'FAT_BURN': 'Fat Burn', 'CARDIO': 'Cardio', 'PEAK': 'Peak',
    }
    for p in hrz_points:
        hrz = p.get("dailyHeartRateZones", {})
        date_str = _first(hrz, "date")
        zones = _first(hrz, "heartRateZones", "zones", default=[]) or []
        fields = {'Normal': 0.0, 'Fat Burn': 0.0, 'Cardio': 0.0, 'Peak': 0.0}
        for z in zones:
            name = zone_name_map.get(str(_first(z, "type", "name", default="")).upper())
            if name:
                fields[name] = _to_float(_first(z, "minutes", "minutesInZone"), 0.0)
        if date_str:
            collected_records.append({
                "measurement": "HR zones",
                "time": local_date_to_utc(date_str),
                "tags": {"Device": DEVICENAME},
                "fields": fields,
            })
    if hrz_points:
        logging.info(f"Recorded HR zones for {start_date_str} to {end_date_str}")

    # --- Resting heart rate -> RestingHR ---
    rhr_points = gh_list("daily-resting-heart-rate", start_dt, end_dt)
    for p in rhr_points:
        rhr = p.get("dailyRestingHeartRate", {})
        date_str = _first(rhr, "date")
        value = _to_float(_first(rhr, "beatsPerMinute", "restingHeartRate", "value"))
        if date_str is None or value is None:
            continue
        collected_records.append({
            "measurement": "RestingHR",
            "time": local_date_to_utc(date_str),
            "tags": {"Device": DEVICENAME},
            "fields": {"value": value},
        })
    if rhr_points:
        logging.info(f"Recorded RestingHR for {start_date_str} to {end_date_str}")


# Daily average SpO2 -> SPO2 (avg/max/min)
def get_daily_data_limit_none(start_date_str, end_date_str):
    start_dt, end_dt = _range_bounds(start_date_str, end_date_str)
    points = gh_list("daily-oxygen-saturation", start_dt, end_dt)
    for p in points:
        ox = p.get("dailyOxygenSaturation", {})
        date_str = _first(ox, "date")
        avg = _to_float(_first(ox, "avgPercentage", "averagePercentage", "avg"))
        mx = _to_float(_first(ox, "maxPercentage", "max"))
        mn = _to_float(_first(ox, "minPercentage", "min"))
        if date_str is None:
            continue
        collected_records.append({
            "measurement": "SPO2",
            "time": local_date_to_utc(date_str),
            "tags": {"Device": DEVICENAME},
            "fields": {"avg": avg, "max": mx, "min": mn},
        })
    if points:
        logging.info(f"Recorded Avg SPO2 for {start_date_str} to {end_date_str}")


# Recent workouts -> Activity Records (exercise Session kind)
def fetch_latest_activities(end_date_str, lookback_days=30):
    end_bound = LOCAL_TIMEZONE.localize(datetime.fromisoformat(end_date_str + "T00:00:00")) + timedelta(days=1)
    start_bound = end_bound - timedelta(days=lookback_days)
    exercises = gh_list("exercise", start_bound, end_bound)
    for p in exercises:
        ex = p.get("exercise", {})
        interval = ex.get("interval", {})
        s_start = _first(interval, "startTime")
        if not s_start:
            continue
        metrics = ex.get("metricsSummary", {})
        fields = {}
        active_duration = _first(ex, "activeDuration")
        if active_duration:
            # e.g. "1800s"
            try:
                fields['ActiveDuration'] = float(str(active_duration).rstrip("s")) * 1000.0  # ms, matching original
            except ValueError:
                pass
        avg_hr = _to_float(_first(metrics, "averageHeartRateBeatsPerMinute"))
        if avg_hr is not None:
            fields['AverageHeartRate'] = avg_hr
        cal = _to_float(_first(metrics, "caloriesKcal"))
        if cal is not None:
            fields['calories'] = cal
        dist_mm = _to_float(_first(metrics, "distanceMillimeters"))
        if dist_mm is not None:
            fields['distance'] = dist_mm / 1e6  # km
        steps = _to_float(_first(metrics, "steps"))
        if steps is not None:
            fields['steps'] = steps
        collected_records.append({
            "measurement": "Activity Records",
            "time": parse_physical_time({"physicalTime": s_start}),
            "tags": {"ActivityName": _first(ex, "displayName", "exerciseType", default="Workout")},
            "fields": fields,
        })
    logging.info(f"Fetched {len(exercises)} recent workouts before {end_date_str}")


# Weight -> Weight (weight, bmi, fat). goal has no Google Health equivalent.
def fetch_weight_logs(start_date_str, end_date_str):
    start_dt, end_dt = _range_bounds(start_date_str, end_date_str)
    weight_points = gh_list("weight", start_dt, end_dt)
    bodyfat_points = gh_list("body-fat", start_dt, end_dt)

    # Index body-fat by day so we can attach it to the matching weight record.
    fat_by_day = {}
    for p in bodyfat_points:
        bf = p.get("bodyFat", {})
        utc = parse_physical_time(bf.get("sampleTime", {}))
        pct = _to_float(_first(bf, "percentage", "value"))
        if utc and pct is not None:
            fat_by_day[utc[:10]] = pct

    if not weight_points:
        logging.warning(f"No weight data available for {start_date_str} to {end_date_str}")
        return

    form_data = ''
    for p in weight_points:
        w = p.get("weight", {})
        utc_time = parse_physical_time(w.get("sampleTime", {}))
        kg = _to_float(_first(w, "weightKilograms", "kilograms", "value"))
        if utc_time is None or kg is None:
            continue
        collected_records.append({
            "measurement": "Weight",
            "time": utc_time,
            "tags": {"Device": DEVICENAME},
            "fields": {
                "weight": kg,
                # goal / bmi are not exposed by Google Health; kept null so the
                # InfluxDB field set stays compatible with old dashboards.
                "goal": None,
                "goal_float": None,
                "bmi": None,
                "fat": fat_by_day.get(utc_time[:10]),
            },
        })
        form_data = {
            "entry.1406463651": end_date_str,
            "entry.1062141579": kg,
        }

    if GOOGLE_FORM_URL and form_data:
        try:
            resp = requests.post(GOOGLE_FORM_URL, data=form_data, timeout=60)
            resp.raise_for_status()
            logging.info(f"Weight form submitted successfully! {start_date_str} to {end_date_str}")
        except requests.RequestException as e:
            logging.error(f"Failed to submit the weight form: {e}")

    logging.info(f"Recorded weight logs for {start_date_str} to {end_date_str}")

# %% [markdown]
# ## Startup update / bulk update

# %%
if AUTO_DATE_RANGE:
    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]
    if len(date_list) > 3:
        logging.warning("Auto schedule update is not meant for more than 3 days at a time; lower auto_update_date_range to avoid rate limits.")

    if DEBUG_MODE:
        logging.info("DEBUG MODE ENABLED: Loading records from file instead of calling the API.")
        debug_records = load_collected_records_from_file(f"collected_{start_date_str}_to_{end_date_str}.json")
        write_points_to_influxdb(debug_records)
    else:
        for date_str in date_list:
            get_intraday_data_limit_1d(date_str)
        get_daily_data_limit_30d(start_date_str, end_date_str)
        get_daily_data_limit_100d(start_date_str, end_date_str)
        get_daily_data_limit_365d(start_date_str, end_date_str)
        get_daily_data_limit_none(start_date_str, end_date_str)
        fetch_weight_logs(start_date_str, end_date_str)
        fetch_latest_activities(end_date_str)
        dump_collected_records_to_file(f"collected_{start_date_str}_to_{end_date_str}.json")
        write_points_to_influxdb(collected_records)
        collected_records = []
else:
    # Bulk backfill mode -------------------------------------------------------
    schedule.every(50).minutes.do(lambda: Get_New_Access_Token(CLIENT_ID, CLIENT_SECRET))

    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]

    def yield_dates_with_gap(date_list, gap):
        start_index = -1 * gap
        while start_index < len(date_list) - 1:
            start_index = start_index + gap
            end_index = start_index + gap
            if end_index > len(date_list) - 1:
                end_index = len(date_list) - 1
            if start_index > len(date_list) - 1:
                break
            yield (date_list[start_index], date_list[end_index])

    def do_bulk_update(funcname, start_date, end_date):
        global collected_records
        funcname(start_date, end_date)
        schedule.run_pending()
        write_points_to_influxdb(collected_records)
        collected_records = []

    do_bulk_update(fetch_weight_logs, date_list[0], date_list[-1])
    fetch_latest_activities(date_list[-1], lookback_days=(end_date - start_date).days + 1)
    write_points_to_influxdb(collected_records)
    collected_records = []
    do_bulk_update(get_daily_data_limit_none, date_list[0], date_list[-1])
    # heart-rate / total-calories have a 14-day max range; keep chunks small.
    for date_range in yield_dates_with_gap(date_list, 14):
        do_bulk_update(get_daily_data_limit_365d, date_range[0], date_range[1])
    for date_range in yield_dates_with_gap(date_list, 90):
        do_bulk_update(get_daily_data_limit_100d, date_range[0], date_range[1])
    for date_range in yield_dates_with_gap(date_list, 90):
        do_bulk_update(get_daily_data_limit_30d, date_range[0], date_range[1])
    for single_day in date_list:
        do_bulk_update(lambda s, e: get_intraday_data_limit_1d(s), single_day, single_day)
    dump_collected_records_to_file(f"collected_{start_date_str}_to_{end_date_str}.json")
    logging.info("Success : Bulk update complete for " + start_date_str + " to " + end_date_str)
    print("Bulk update complete!")

# %% [markdown]
# ## Continuous scheduled updates

# %%
if SCHEDULE_AUTO_UPDATE:
    # Google access tokens expire after ~1 hour; refresh comfortably inside that.
    schedule.every(50).minutes.do(lambda: Get_New_Access_Token(CLIENT_ID, CLIENT_SECRET))
    schedule.every(3).minutes.do(lambda: get_intraday_data_limit_1d(end_date_str))
    schedule.every(1).hours.do(lambda: get_intraday_data_limit_1d((datetime.strptime(end_date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")))
    schedule.every(3).hours.do(lambda: get_daily_data_limit_30d(start_date_str, end_date_str))
    schedule.every(4).hours.do(lambda: get_daily_data_limit_100d(start_date_str, end_date_str))
    schedule.every(1).hours.do(lambda: get_daily_data_limit_365d(start_date_str, end_date_str))
    schedule.every(6).hours.do(lambda: get_daily_data_limit_none(start_date_str, end_date_str))
    schedule.every(1).hours.do(lambda: fetch_latest_activities(end_date_str))
    schedule.every(5).hours.do(lambda: fetch_weight_logs(start_date_str, end_date_str))

    while True:
        schedule.run_pending()
        if len(collected_records) != 0:
            write_points_to_influxdb(collected_records)
            collected_records = []
        time.sleep(30)
        update_working_dates()
