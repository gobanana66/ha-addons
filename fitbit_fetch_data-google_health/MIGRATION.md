# Migrating this add-on from the Fitbit Web API to the Google Health API

## Why this migration exists

Google is **turning down the Fitbit Web API in September 2026**. Server-side
(cloud) access to Fitbit and Pixel health data now lives behind the new
**Google Health API** (`https://health.googleapis.com/v4/`), authenticated with
**Google OAuth 2.0**. Health Connect is *not* the right target for this add-on —
that is an on-device Android API and cannot be called from a Home Assistant
server process.

This add-on has been rewritten to read from the Google Health API while keeping
the **same InfluxDB measurement and field names**, so your existing Grafana /
Home Assistant dashboards keep working.

> Source references (fetched Aug 2026):
> [About the Google Health API](https://developers.google.com/health/about) ·
> [Set up Google Cloud and OAuth](https://developers.google.com/health/setup) ·
> [Data types](https://developers.google.com/health/data-types) ·
> [Scopes](https://developers.google.com/health/scopes) ·
> [Vitals guide](https://developers.google.com/health/data-types/vitals) ·
> [Workouts guide](https://developers.google.com/health/data-types/workouts) ·
> [Migration guide](https://developers.google.com/health/migration)

## What changed at a glance

| | Old (Fitbit Web API) | New (Google Health API) |
| --- | --- | --- |
| Base URL | `api.fitbit.com/1/...` | `health.googleapis.com/v4/users/me/...` |
| Endpoints | 120+ per-metric endpoints | 1 resource shape, 31 data types, 4 read methods (`list`, `reconcile`, `rollUp`, `dailyRollUp`) |
| Auth | Fitbit OAuth (Basic-auth refresh) | Google OAuth 2.0 (`oauth2.googleapis.com/token`) |
| Tokens | Fitbit tokens | **New Google tokens — old ones do NOT transfer** |
| Intraday HR | Special approval required | Default (~5s resolution) via `list` |
| Access gating | App type "Personal" | Every scope is **Restricted** → Google privacy/security review before >100 users / production |

## One-time Google Cloud + OAuth setup

You'll replace the Fitbit `client_id` / `client_secret` / `refresh_token` with
**Google** ones.

1. **Create a Google Cloud project and enable the API.**
   Go to the [Google Health setup page](https://developers.google.com/health/setup)
   and use "Enable the API and get an OAuth 2.0 Client ID", or manually enable
   the **Google Health API** on the
   [API Library](https://console.developers.google.com/apis/library/health.googleapis.com).

2. **Create an OAuth 2.0 Client ID** (type **Web Server**) on the
   [Credentials page](https://console.developers.google.com/apis/credentials).
   For the **Authorized redirect URI**, the Google guide suggests
   `https://www.google.com`. Copy the **Client ID** and **Client Secret**.

3. **Add yourself as a test user.** On the
   [Audience page](https://console.developers.google.com/auth/audience), keep
   "Publishing status = Testing", "User type = External", and add your Google
   account email under **Test users**.

4. **Add the scopes** this add-on uses on the
   [Data Access page](https://console.developers.google.com/auth/scopes):
   - `.../auth/googlehealth.activity_and_fitness.readonly`
   - `.../auth/googlehealth.health_metrics_and_measurements.readonly`
   - `.../auth/googlehealth.sleep.readonly`

5. **Get a refresh token.** Run the OAuth 2.0 authorization-code flow once. The
   simplest path is the
   [OAuth 2.0 Playground](https://developers.google.com/oauthplayground/) (set
   your own client ID/secret in its settings) or a small local script:
   - Authorization URL:
     ```
     https://accounts.google.com/o/oauth2/v2/auth?client_id=CLIENT_ID&redirect_uri=https://www.google.com&response_type=code&access_type=offline&prompt=consent&scope=<space-separated scopes>
     ```
   - **Do NOT add `include_granted_scopes=true`.** If your Google account ever
     consented to legacy Google Fit `fitness.*` scopes on the same client, they
     get unioned into the token and the Health API data plane rejects the
     mixed-scope token with an opaque error.
   - Exchange the returned `code` at `https://oauth2.googleapis.com/token` and
     keep the `refresh_token`.

6. **Configure the add-on** with the Google values:
   ```yaml
   client_id: "YOUR_GOOGLE_OAUTH_CLIENT_ID"
   client_secret: "YOUR_GOOGLE_OAUTH_CLIENT_SECRET"
   refresh_token: "YOUR_GOOGLE_REFRESH_TOKEN"
   local_timezone: "Europe/Berlin"   # set explicitly; see note below
   ```

> **Refresh-token expiry while in Testing mode:** Google refresh tokens issued
> while the consent screen is in "Testing" status **expire after 7 days**. For
> unattended long-term operation, move the app to "In Production" (which
> requires the Restricted-scope security review) so refresh tokens stop
> expiring.

## Field-by-field mapping (InfluxDB schema preserved)

All rows below were **verified against live API responses** (via Postman) unless
noted. Values that are JSON strings (e.g. `beatsPerMinute`, `steps`) are cast to
float in code. Daily types carry `date` as a nested `{year, month, day}` object.

| InfluxDB measurement | Google Health data type | Method | Verified field(s) |
| --- | --- | --- | --- |
| `HeartRate_Intraday` | `heart-rate` | **reconcile** | `heartRate.sampleTime.physicalTime`, `heartRate.beatsPerMinute` ✅ |
| `Steps_Intraday` | `steps` | **reconcile** | `steps.interval.startTime`, `steps.count` ✅ |
| `SPO2_Intraday` | `oxygen-saturation` | list | `oxygenSaturation.percentage` (docs-confirmed; usually empty — Google-side gap) |
| `Sleep Summary` / `Sleep Levels` | `sleep` | list | `sleep.interval`, `stages[].type`, `summary.stagesSummary[]`, `minutesAsleep/Awake/InSleepPeriod`; `metadata.mainSleep`; efficiency derived ✅ |
| `Activity Records` | `exercise` | list (no filter, windowed client-side) | `metricsSummary.caloriesKcal/distanceMillimeters/steps/averageHeartRateBeatsPerMinute`, `exerciseType`/`displayName`, `activeDuration` ✅ (empty-metrics dupes skipped) |
| `Weight` | `weight` (+ `body-fat`) | list | `weight.weightGrams`÷1000, `bodyFat.percentage` ✅ |
| `RestingHR` | `daily-resting-heart-rate` | list | `dailyRestingHeartRate.beatsPerMinute` ✅ |
| `HRV` | `daily-heart-rate-variability` | list | `averageHeartRateVariabilityMilliseconds`, `deepSleepRootMeanSquareOfSuccessiveDifferencesMilliseconds` ✅ |
| `BreathingRate` | `daily-respiratory-rate` | list | `dailyRespiratoryRate.breathsPerMinute` ✅ |
| `Skin Temperature Variation` | `daily-sleep-temperature-derivations` | list | derived: `nightlyTemperatureCelsius − baselineTemperatureCelsius` ✅ |
| `SPO2` (avg/max/min) | `daily-oxygen-saturation` | list | `averagePercentage`, `upperBoundPercentage`, `lowerBoundPercentage` ✅ |
| `Activity Minutes` | `activity-level` | **reconcile** | `activityLevel.activityLevelType` (SEDENTARY/LIGHTLY_ACTIVE/MODERATELY_ACTIVE/VERY_ACTIVE), summed per day ✅ |
| `distance` / `Total Steps` | `distance` / `steps` | **reconcile** (summed per day) | `distance.millimeters`÷1e6, `steps.count` ✅ |
| `calories` (daily total) | `total-calories` | **dailyRollUp (POST)** | `rollupDataPoints[].totalCalories.kcalSum` ✅ (verified via reference; POST body = CivilTimeInterval `range`) |
| `Active Zone Minutes` | `active-zone-minutes` | dailyRollUp (POST) | value field best-effort, **unverified** |
| `VO2Max` | `daily-vo2-max` | list | value field best-effort, **unverified** |
| `HR zones` | `daily-heart-rate-zones` | **reconcile** | field names best-effort, **unverified** (no live sample seen) |

Filter notes learned during verification: the `list`/`reconcile` endpoints take
an AIP-160 `filter` param (not `startTime`/`endTime`). Daily types filter on
`<type>.date` with **`>=` and `<` only** (no `<=`). `exercise` sessions don't
support time-range filtering at all, so they're listed unfiltered and windowed
client-side. `:dailyRollUp` does **not** accept the `filter` param.

## Known gaps (no Google Health equivalent today)

- **Weight `goal` / `goal_float` / `bmi`** — not exposed by the Google Health
  `weight` type. These fields are written as `null` to keep the InfluxDB field
  set unchanged; update your dashboard panels accordingly.
- **`local_timezone: "Automatic"`** — the `getProfile` resource has no timezone
  field, so `Automatic` falls back to UTC. Set `local_timezone` explicitly.

### Recovered (not the data-type API)

- **`DeviceBatteryLevel`** — restored via the `users/me/pairedDevices` resource
  (`batteryLevel` + `lastSyncTime`), not a `dataTypes` query. `get_battery_level()`
  lists paired devices, skips SCALE devices, and records the tracker/watch
  battery. May require an extra OAuth scope; the call degrades gracefully (logs
  a warning) if the scope isn't granted.
- **`local_timezone: "Automatic"`** — the old code read your Fitbit profile
  timezone. The Health API profile shape differs, so `Automatic` now falls back
  to UTC. **Set `local_timezone` explicitly.**

## Query constraints that shaped the code

- **Max query range:** 14 days for `heart-rate`, `active-minutes`,
  `total-calories`, `calories-in-heart-rate-zone`; 90 days for everything else.
  Bulk-backfill chunking was adjusted accordingly.
- **Pagination:** 10,000 data points per page, followed via `nextPageToken`.
- **Integers as strings:** 64-bit values like `beatsPerMinute` and `steps` come
  back as JSON strings; the code casts them with `_to_float`.
- **Rate limits / retries:** exponential backoff on `429` and `504`, with
  fallback to smaller windows, per Google's guidance.

## Migration checklist

1. [ ] Create the Google Cloud project + OAuth client, add yourself as a test user, add the three scopes.
2. [ ] **Apply for the Restricted-scope security review early** — it gates production and has no published SLA.
3. [ ] Obtain a Google refresh token (Playground or local script) and put it in the add-on config.
4. [ ] Start the add-on; confirm new data lands in InfluxDB under the existing measurements.
5. [ ] Update dashboards for the retired fields (battery, weight goal, BMI).
6. [ ] Before September 2026, move the OAuth app to Production so refresh tokens stop expiring after 7 days.
