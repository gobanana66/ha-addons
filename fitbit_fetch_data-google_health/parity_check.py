#!/usr/bin/env python3
"""Parity check: Fitbit Web API vs Google Health API, side by side for one date.

Standalone debug tool. It does NOT import or trigger the addon scripts (those
run their full fetch/write on import); instead it has its own thin clients for
both APIs so you can eyeball whether the Google Health numbers match what Fitbit
returns for the same day.

Field names below are the ones verified against live API responses during the
migration (see MIGRATION.md).

Credentials/tokens are read from the environment (share your .env):
  - Google : CLIENT_ID, CLIENT_SECRET, TOKEN_FILE_PATH  (default ./google_health.token)
  - Fitbit : FITBIT_CLIENT_ID, FITBIT_CLIENT_SECRET, FITBIT_TOKEN_FILE (default ./fitbit.token)

Usage:
  set -a; source .env; set +a
  python3 parity_check.py [YYYY-MM-DD]      # defaults to yesterday
"""
import os
import sys
import json
import base64
from datetime import datetime, timedelta

import requests
import pytz

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
DATE = sys.argv[1] if len(sys.argv) > 1 else (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
_tz_name = os.environ.get("LOCAL_TIMEZONE", "UTC")
TZ = pytz.UTC if _tz_name in ("Automatic", "", None) else pytz.timezone(_tz_name)

# Fitbit
FITBIT_CLIENT_ID = os.environ.get("FITBIT_CLIENT_ID")
FITBIT_CLIENT_SECRET = os.environ.get("FITBIT_CLIENT_SECRET")
FITBIT_TOKEN_FILE = os.environ.get("FITBIT_TOKEN_FILE", "./fitbit.token")

# Google
G_CLIENT_ID = os.environ.get("CLIENT_ID")
G_CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
G_TOKEN_FILE = os.environ.get("TOKEN_FILE_PATH", "./google_health.token")
GHEALTH_BASE = "https://health.googleapis.com/v4/users/me"

# Day bounds in UTC for the target local date
_day_start = TZ.localize(datetime.fromisoformat(DATE + "T00:00:00"))
_day_end = _day_start + timedelta(days=1)
UTC_START = _day_start.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
UTC_END = _day_end.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
NEXT_DATE = (datetime.fromisoformat(DATE) + timedelta(days=1)).strftime("%Y-%m-%d")

FITBIT_TOKEN = None
GOOGLE_TOKEN = None


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _f(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def _first(d, *keys, default=None):
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def _dur_seconds(s):
    """Parse a Google duration like '1691s' or '1664.383s' into seconds."""
    if s is None:
        return None
    try:
        return float(str(s).rstrip("s"))
    except ValueError:
        return None

def _local_date(iso):
    """Local YYYY-MM-DD for a UTC ISO timestamp."""
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return dt.astimezone(TZ).strftime("%Y-%m-%d")


# --------------------------------------------------------------------------- #
# Auth
# --------------------------------------------------------------------------- #
def _load(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:
        return {}

def fitbit_access_token():
    if not (FITBIT_CLIENT_ID and FITBIT_CLIENT_SECRET):
        return None
    tok = _load(FITBIT_TOKEN_FILE)
    rt = tok.get("refresh_token")
    if not rt:
        return None
    basic = base64.b64encode(f"{FITBIT_CLIENT_ID}:{FITBIT_CLIENT_SECRET}".encode()).decode()
    r = requests.post(
        "https://api.fitbit.com/oauth2/token",
        headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "refresh_token", "refresh_token": rt},
        timeout=60,
    )
    if r.status_code != 200:
        print(f"[warn] Fitbit token refresh failed ({r.status_code}): {r.text[:200]}")
        return None
    j = r.json()
    tok.update({"access_token": j["access_token"], "refresh_token": j.get("refresh_token", rt)})
    try:
        with open(FITBIT_TOKEN_FILE, "w") as fh:
            json.dump(tok, fh)
    except Exception:
        pass
    return j["access_token"]

def google_access_token():
    if not (G_CLIENT_ID and G_CLIENT_SECRET):
        return None
    tok = _load(G_TOKEN_FILE)
    rt = tok.get("refresh_token")
    if not rt:
        return None
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "refresh_token", "refresh_token": rt,
              "client_id": G_CLIENT_ID, "client_secret": G_CLIENT_SECRET},
        timeout=60,
    )
    if r.status_code != 200:
        print(f"[warn] Google token refresh failed ({r.status_code}): {r.text[:200]}")
        return None
    j = r.json()
    tok.update({"access_token": j["access_token"], "refresh_token": j.get("refresh_token", rt)})
    try:
        with open(G_TOKEN_FILE, "w") as fh:
            json.dump(tok, fh)
    except Exception:
        pass
    return j["access_token"]


# --------------------------------------------------------------------------- #
# Fitbit fetchers (each returns a scalar for DATE, or None)
# --------------------------------------------------------------------------- #
def fb_get(path):
    r = requests.get(
        "https://api.fitbit.com" + path,
        headers={"Authorization": f"Bearer {FITBIT_TOKEN}", "Accept": "application/json"},
        timeout=60,
    )
    if r.status_code != 200:
        return None
    return r.json()

def fb_steps():
    j = fb_get(f"/1/user/-/activities/date/{DATE}.json")
    return _f(_first(j.get("summary", {}), "steps")) if j else None

def fb_distance_km():
    j = fb_get(f"/1/user/-/activities/date/{DATE}.json")
    if not j:
        return None
    for d in j.get("summary", {}).get("distances", []):
        if d.get("activity") == "total":
            return _f(d.get("distance"))
    return None

def fb_resting_hr():
    j = fb_get(f"/1/user/-/activities/heart/date/{DATE}/1d.json")
    if not j:
        return None
    arr = j.get("activities-heart", [])
    return _f(_first(arr[0].get("value", {}), "restingHeartRate")) if arr else None

def _fb_sleep():
    return fb_get(f"/1.2/user/-/sleep/date/{DATE}.json") or {}

def fb_sleep_minutes():
    return _f(_first(_fb_sleep().get("summary", {}), "totalMinutesAsleep"))

def _fb_sleep_stage(stage):
    stages = _fb_sleep().get("summary", {}).get("stages", {})
    return _f(stages.get(stage)) if stages else None

def fb_sleep_deep():  return _fb_sleep_stage("deep")
def fb_sleep_light(): return _fb_sleep_stage("light")
def fb_sleep_rem():   return _fb_sleep_stage("rem")

def fb_spo2_avg():
    j = fb_get(f"/1/user/-/spo2/date/{DATE}.json")
    return _f(_first(j.get("value", {}), "avg")) if isinstance(j, dict) else None

def fb_hrv_rmssd():
    j = fb_get(f"/1/user/-/hrv/date/{DATE}.json")
    arr = j.get("hrv", []) if j else []
    return _f(_first(arr[0].get("value", {}), "dailyRmssd")) if arr else None

def fb_breathing():
    j = fb_get(f"/1/user/-/br/date/{DATE}.json")
    arr = j.get("br", []) if j else []
    return _f(_first(arr[0].get("value", {}), "breathingRate")) if arr else None

def fb_weight():
    j = fb_get(f"/1/user/-/body/log/weight/date/{DATE}.json")
    arr = j.get("weight", []) if j else []
    return _f(arr[-1].get("weight")) if arr else None

def fb_body_fat():
    j = fb_get(f"/1/user/-/body/log/fat/date/{DATE}.json")
    arr = j.get("fat", []) if j else []
    return _f(arr[-1].get("fat")) if arr else None

def fb_skin_temp():
    j = fb_get(f"/1/user/-/temp/skin/date/{DATE}.json")
    arr = j.get("tempSkin", []) if j else []
    return _f(_first(arr[0].get("value", {}), "nightlyRelative")) if arr else None

def fb_workouts():
    """List workouts that START on DATE. Returns list of summary dicts."""
    j = fb_get(f"/1/user/-/activities/list.json?afterDate={DATE}&sort=asc&limit=50&offset=0")
    out = []
    for a in (j or {}).get("activities", []):
        st = a.get("startTime", "")
        if not st.startswith(DATE):
            continue
        out.append({
            "name": a.get("activityName", "?"),
            "min": (_f(a.get("activeDuration")) or 0) / 60000.0,   # ms -> min
            "cal": _f(a.get("calories")),
            "hr": _f(a.get("averageHeartRate")),
        })
    return out


# --------------------------------------------------------------------------- #
# Google Health fetchers
# --------------------------------------------------------------------------- #
FILTER_KIND = {
    "heart-rate": "sample", "oxygen-saturation": "sample", "weight": "sample", "body-fat": "sample",
    "steps": "interval", "distance": "interval",
    "sleep": "session",
    "daily-heart-rate-variability": "daily", "daily-respiratory-rate": "daily",
    "daily-resting-heart-rate": "daily", "daily-oxygen-saturation": "daily",
    "daily-sleep-temperature-derivations": "daily",
}

def _g_filter(data_type):
    snake = data_type.replace("-", "_")
    kind = FILTER_KIND.get(data_type, "sample")
    if kind == "interval":
        field = f"{snake}.interval.start_time"
    elif kind == "session":
        field = f"{snake}.interval.end_time"
    elif kind == "daily":
        field = f"{snake}.date"
        return f'{field} >= "{DATE}" AND {field} < "{NEXT_DATE}"'
    else:
        field = f"{snake}.sample_time.physical_time"
    return f'{field} >= "{UTC_START}" AND {field} < "{UTC_END}"'

def gh_list(data_type, method="list", use_filter=True):
    # method="reconcile" -> merged/deduped stream (use for steps/distance/HR)
    suffix = ":reconcile" if method == "reconcile" else ""
    points, page_token = [], None
    while True:
        params = {"pageSize": "10000"}
        if use_filter:
            params["filter"] = _g_filter(data_type)
        if page_token:
            params["pageToken"] = page_token
        r = requests.get(
            f"{GHEALTH_BASE}/dataTypes/{data_type}/dataPoints{suffix}",
            headers={"Authorization": f"Bearer {GOOGLE_TOKEN}", "Accept": "application/json"},
            params=params, timeout=60,
        )
        if r.status_code != 200:
            if not points:
                gh_list.last_error = f"{data_type}: {r.status_code} {r.text[:140]}"
            break
        body = r.json()
        points.extend(body.get("dataPoints", []) or [])
        page_token = body.get("nextPageToken")
        if not page_token:
            break
    return points
gh_list.last_error = None

def gh_steps():
    total, seen = 0.0, False
    for p in gh_list("steps", method="reconcile"):
        v = _f(_first(p.get("steps", {}), "count", "countSum"))
        if v is not None:
            total += v; seen = True
    return total if seen else None

def gh_distance_km():
    total, seen = 0.0, False
    for p in gh_list("distance", method="reconcile"):
        v = _f(_first(p.get("distance", {}), "millimeters", "distanceMillimeters"))
        if v is not None:
            total += v; seen = True
    return (total / 1e6) if seen else None

def gh_resting_hr():
    for p in gh_list("daily-resting-heart-rate"):
        v = _f(_first(p.get("dailyRestingHeartRate", {}), "beatsPerMinute"))
        if v is not None:
            return v
    return None

def _gh_main_sleep():
    """Return the main-sleep summary dict for DATE, or {}."""
    best = None
    for p in gh_list("sleep"):
        s = p.get("sleep", {})
        if bool(_first(s.get("metadata", {}), "mainSleep", default=False)):
            return s.get("summary", {})
        if best is None:
            best = s.get("summary", {})
    return best or {}

def gh_sleep_minutes():
    return _f(_first(_gh_main_sleep(), "minutesAsleep"))

def _gh_sleep_stage(stage):
    for s in _gh_main_sleep().get("stagesSummary", []) or []:
        if str(s.get("type", "")).upper() == stage:
            return _f(s.get("minutes"))
    return None

def gh_sleep_deep():  return _gh_sleep_stage("DEEP")
def gh_sleep_light(): return _gh_sleep_stage("LIGHT")
def gh_sleep_rem():   return _gh_sleep_stage("REM")

def gh_spo2_avg():
    for p in gh_list("daily-oxygen-saturation"):
        v = _f(_first(p.get("dailyOxygenSaturation", {}), "averagePercentage", "avgPercentage"))
        if v is not None:
            return v
    return None

def gh_hrv_rmssd():
    for p in gh_list("daily-heart-rate-variability"):
        hrv = p.get("dailyHeartRateVariability", {})
        v = _f(_first(hrv, "averageHeartRateVariabilityMilliseconds", "rmssd", "dailyRmssd"))
        if v is not None:
            return v
    return None

def gh_breathing():
    for p in gh_list("daily-respiratory-rate"):
        v = _f(_first(p.get("dailyRespiratoryRate", {}), "breathsPerMinute", "breathingRate"))
        if v is not None:
            return v
    return None

def gh_weight_kg():
    latest = None
    for p in gh_list("weight"):
        w = p.get("weight", {})
        v = _f(_first(w, "weightKilograms", "kilograms", "value"))
        if v is None:
            g = _f(_first(w, "weightGrams"))
            v = (g / 1000.0) if g is not None else None
        if v is not None:
            latest = v
    return latest

def gh_body_fat():
    latest = None
    for p in gh_list("body-fat"):
        v = _f(_first(p.get("bodyFat", {}), "percentage", "value"))
        if v is not None:
            latest = v
    return latest

def gh_skin_temp():
    for p in gh_list("daily-sleep-temperature-derivations"):
        t = p.get("dailySleepTemperatureDerivations", {})
        nightly = _f(_first(t, "nightlyTemperatureCelsius"))
        baseline = _f(_first(t, "baselineTemperatureCelsius"))
        if nightly is not None and baseline is not None:
            return round(nightly - baseline, 3)
    return None

def gh_workouts():
    """Exercise sessions starting on DATE (no filter; windowed client-side; empty-metrics dupes skipped)."""
    out = []
    for p in gh_list("exercise", use_filter=False):
        ex = p.get("exercise", {})
        st = _first(ex.get("interval", {}), "startTime")
        if not st or _local_date(st) != DATE:
            continue
        m = ex.get("metricsSummary", {})
        if not m:
            continue
        out.append({
            "name": _first(ex, "displayName", "exerciseType", default="Workout"),
            "min": (_dur_seconds(_first(ex, "activeDuration")) or 0) / 60.0,
            "cal": _f(_first(m, "caloriesKcal")),
            "hr": _f(_first(m, "averageHeartRateBeatsPerMinute")),
        })
    return out


# --------------------------------------------------------------------------- #
# Scored daily probes  (name, unit, fitbit_fn, ghealth_fn, comparable?)
#   comparable=False => shown, not scored (unit systems or methods differ).
# --------------------------------------------------------------------------- #
PROBES = [
    ("Total steps",       "steps", fb_steps,         gh_steps,        True),
    ("Resting HR",        "bpm",   fb_resting_hr,    gh_resting_hr,   True),
    ("Sleep asleep",      "min",   fb_sleep_minutes, gh_sleep_minutes, True),
    ("Sleep deep",        "min",   fb_sleep_deep,    gh_sleep_deep,   True),
    ("Sleep light",       "min",   fb_sleep_light,   gh_sleep_light,  True),
    ("Sleep REM",         "min",   fb_sleep_rem,     gh_sleep_rem,    True),
    ("Avg SpO2",          "%",     fb_spo2_avg,      gh_spo2_avg,     True),
    ("HRV",               "ms",    fb_hrv_rmssd,     gh_hrv_rmssd,    True),
    ("Breathing rate",    "brpm",  fb_breathing,     gh_breathing,    True),
    ("Body fat",          "%",     fb_body_fat,      gh_body_fat,     True),
    ("Skin temp dev",     "C",     fb_skin_temp,     gh_skin_temp,    False),
    ("Distance",          "km*",   fb_distance_km,   gh_distance_km,  False),
    ("Weight",            "kg*",   fb_weight,        gh_weight_kg,    False),
]

def _fmt(v):
    if v is None:
        return "-"
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    return f"{v:.2f}"

def _verdict(fb, gh, comparable):
    if fb is None and gh is None:
        return "both empty"
    if fb is None or gh is None:
        return "missing one"
    if not comparable:
        return "~ differs"
    denom = max(abs(fb), 1.0)
    rel = abs(fb - gh) / denom
    return "OK" if rel <= 0.05 else f"X {rel*100:.0f}%"

def _table(headers, rows):
    widths = [max(len(str(r[i])) for r in rows + [headers]) for i in range(len(headers))]
    print("  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers)))
    print("  ".join("-" * widths[i] for i in range(len(headers))))
    for r in rows:
        print("  ".join(str(r[i]).ljust(widths[i]) for i in range(len(headers))))


def main():
    global FITBIT_TOKEN, GOOGLE_TOKEN
    print(f"\nParity check for {DATE}  (tz={_tz_name}; UTC window {UTC_START} .. {UTC_END})\n")

    FITBIT_TOKEN = fitbit_access_token()
    GOOGLE_TOKEN = google_access_token()
    print(f"Fitbit auth : {'ok' if FITBIT_TOKEN else 'UNAVAILABLE (set FITBIT_CLIENT_ID/SECRET + fitbit.token)'}")
    print(f"Google auth : {'ok' if GOOGLE_TOKEN else 'UNAVAILABLE (set CLIENT_ID/SECRET + google_health.token)'}\n")

    # --- scored daily metrics ---
    rows = []
    for name, unit, fb_fn, gh_fn, comparable in PROBES:
        fb_val = fb_fn() if FITBIT_TOKEN else None
        gh_val = gh_fn() if GOOGLE_TOKEN else None
        rows.append((name, unit, _fmt(fb_val), _fmt(gh_val), _verdict(fb_val, gh_val, comparable)))
    print("DAILY METRICS")
    _table(("Metric", "Unit", "Fitbit", "Google", "Parity"), rows)

    # --- exercise / workouts (top-level per session) ---
    print("\nWORKOUTS (sessions starting on this date)")
    fb_w = fb_workouts() if FITBIT_TOKEN else []
    gh_w = gh_workouts() if GOOGLE_TOKEN else []

    def _wrows(ws):
        return [(w["name"][:22], _fmt(w["min"]), _fmt(w["cal"]), _fmt(w["hr"])) for w in ws]

    print(f"\n  Fitbit ({len(fb_w)} workouts, {_fmt(sum((w['cal'] or 0) for w in fb_w))} kcal):")
    if fb_w:
        _table(("Activity", "Min", "Cal", "AvgHR"), _wrows(fb_w))
    else:
        print("    (none)")
    print(f"\n  Google ({len(gh_w)} workouts, {_fmt(sum((w['cal'] or 0) for w in gh_w))} kcal):")
    if gh_w:
        _table(("Activity", "Min", "Cal", "AvgHR"), _wrows(gh_w))
    else:
        print("    (none)")

    if gh_list.last_error:
        print(f"\n[google] first non-200: {gh_list.last_error}")
    print("\n* Distance/weight units differ (Fitbit uses your profile unit system, "
          "Google is fixed km/kg). Skin-temp deviation is computed differently by each "
          "API. These are shown for eyeballing, not scored.\n")


if __name__ == "__main__":
    main()
