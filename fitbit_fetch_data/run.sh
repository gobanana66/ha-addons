#!/usr/bin/with-contenv bashio

# Create necessary directories if they don't exist
mkdir -p /share/fitbit/logs
mkdir -p /share/fitbit/tokens

# Set environment variables
export FITBIT_LOG_FILE_PATH="/share/fitbit/logs/fitbit.log"
export TOKEN_FILE_PATH="/share/fitbit/tokens/fitbit.token"
export OVERWRITE_LOG_FILE=True

# Get config values using bashio
export INFLUXDB_VERSION=$(bashio::config 'influxdb_version')
export INFLUXDB_HOST=$(bashio::config 'influxdb_host')
export INFLUXDB_PORT=$(bashio::config 'influxdb_port')
export INFLUXDB_USERNAME=$(bashio::config 'influxdb_username')
export INFLUXDB_PASSWORD=$(bashio::config 'influxdb_password')
export INFLUXDB_DATABASE=$(bashio::config 'influxdb_database')
export INFLUXDB_BUCKET=$(bashio::config 'influxdb_bucket')
export INFLUXDB_ORG=$(bashio::config 'influxdb_org')
export INFLUXDB_TOKEN=$(bashio::config 'influxdb_token')
export INFLUXDB_URL=$(bashio::config 'influxdb_url')
export INFLUXDB_V3_ACCESS_TOKEN=$(bashio::config 'influxdb_v3_access_token')
export CLIENT_ID=$(bashio::config 'client_id')
export CLIENT_SECRET=$(bashio::config 'client_secret')
export DEVICENAME=$(bashio::config 'devicename')
export GOOGLE_FORM_URL=$(bashio::config 'google_form_url')
export LOCAL_TIMEZONE=$(bashio::config 'local_timezone')

# Write initial refresh token if provided
REFRESH_TOKEN=$(bashio::config 'refresh_token')
if [ ! -f "$TOKEN_FILE_PATH" ] && [ ! -z "$REFRESH_TOKEN" ]; then
    echo "{\"refresh_token\": \"$REFRESH_TOKEN\", \"access_token\": \"\"}" > "$TOKEN_FILE_PATH"
fi

# Start the Fitbit Fetch Data script
python3 /app/Fitbit_Fetch.py