# Fitbit Data Integration Add-on Documentation

## Overview

This Home Assistant add-on provides seamless integration with Fitbit devices, automatically fetching your health and fitness data and storing it in an InfluxDB database. The add-on supports both real-time data collection and historical data backfilling, making it perfect for both new setups and importing your existing Fitbit history.

## Features

### Data Collection
- Heart Rate (including intraday data)
- Step Count (daily and intraday)
- Sleep Tracking (stages, efficiency, duration)
- SpO2 Measurements
- Active Minutes
- Distance and Calories
- Device Battery Level
- Breathing Rate
- Heart Rate Variability (HRV)
- Skin Temperature
- Activity Records
- GPS data for activities (when available)
- Weight and BMI

### Additional Features
- Automatic token refresh
- Historical data backfilling
- Rate limit aware operation
- Support for InfluxDB 1.x, 2.x, and 3.x
- Configurable update intervals
- Timezone awareness

## Prerequisites

### 1. Fitbit Account Setup
1. A Fitbit account and compatible device
2. A Fitbit Developer Application (for API access)
   - Go to [dev.fitbit.com](https://dev.fitbit.com)
   - Log in with your Fitbit account
   - Click "Create App"
   - Fill in the application details:
     - Application Name: Your choice (e.g., "Home Assistant Integration")
     - Description: Brief description of your usage
     - Website URL: Can be http://localhost
     - Organization: Your name
     - OAuth 2.0 Application Type: **Must be "Personal"**
     - Callback URL: http://localhost
     - Default Access Type: Read-Only
   - Accept the terms and create the application
   - Save your **Client ID** and **Client Secret**

### 2. Getting Your Refresh Token
1. Visit [dev.fitbit.com](https://dev.fitbit.com)
2. Follow the [OAuth 2.0 Authorization Guide](https://dev.fitbit.com/build/reference/web-api/developer-guide/authorization/)
3. Use the authorization flow to obtain your refresh token
4. Save your refresh token for the add-on configuration

### 3. InfluxDB Setup
Ensure you have an InfluxDB instance accessible from your Home Assistant installation.

## Installation

1. Add the repository to Home Assistant:
   ```
   https://gitlab.fristerspace.de/demian/fitbit-ha-addon
   ```
2. Install the "Fitbit Fetch Data" add-on from the Add-on Store
3. Configure the add-on (see Configuration section)
4. Start the add-on

## Configuration

### Required Configuration

```yaml
refresh_token: "your-fitbit-refresh-token"
client_id: "your-fitbit-client-id"
client_secret: "your-fitbit-client-secret"
devicename: "your-fitbit-device-name"
```

### InfluxDB Configuration

Set `influxdb_version` to "1", "2", or "3" and fill in the corresponding variables below.

For InfluxDB 1.x:
```yaml
influxdb_version: "1"
influxdb_host: "localhost"
influxdb_port: 8086
influxdb_username: "your-username"
influxdb_password: "your-password"
influxdb_database: "fitbit"
```

For InfluxDB 2.x:
```yaml
influxdb_version: "2"
influxdb_url: "http://your-influxdb-url:8086"
influxdb_bucket: "your-bucket"
influxdb_org: "your-org"
influxdb_token: "your-token"
```

For InfluxDB 3.x:
```yaml
influxdb_version: "3"
influxdb_url: "https://your-cloud-region.aws.cloud2.influxdata.com"
influxdb_database: "your-database"
influxdb_v3_access_token: "your-v3-token"
```

### Optional Configuration

```yaml
local_timezone: "Automatic"  # Your local timezone (e.g., "Europe/Berlin") or "Automatic"
```

## File Storage

The add-on creates and maintains two directories:
- `/share/fitbit/logs/`: Contains operation logs
- `/share/fitbit/tokens/`: Stores authentication tokens

These directories are automatically created and managed by the add-on.

## Troubleshooting

### Common Issues

1. **Add-on Won't Start**
   - Verify all required configuration options are set
   - Check InfluxDB connection details
   - Ensure the Fitbit application is set to "Personal" type
   - Review the add-on logs

2. **No Data Collection**
   - Verify your Fitbit tokens are correct
   - Check if your Fitbit device is syncing
   - Ensure InfluxDB is accessible
   - Look for rate limiting messages in the logs

3. **Authentication Errors**
   - Refresh token might be invalid - obtain a new one
   - Verify Client ID and Secret match your Fitbit application
   - Check if your Fitbit application is set to "Personal" type

### Viewing Logs

1. Open Home Assistant
2. Go to Settings → Add-ons
3. Select the "Fitbit Fetch Data" add-on
4. Click on the "Logs" tab

### Rate Limiting

The add-on implements smart rate limiting to stay within Fitbit's API constraints:
- Maximum 150 requests per hour
- Automatic backoff when limits are reached
- Priority-based data collection

## Support

- For add-on specific issues: [GitLab Issues](https://gitlab.fristerspace.de/demian/fitbit-ha-addon/issues)
- For general questions: [Home Assistant Community](https://community.home-assistant.io/)
- For underlying Fitbit Fetch functionality: [GitHub Repository](https://github.com/arpanghosh8453/public-fitbit-projects)

## Contributing

This is an open-source project. Contributions are welcome on [GitLab](https://gitlab.fristerspace.de/demian/fitbit-ha-addon).

## Grafana Dashboard Integration

While Home Assistant provides basic visualization capabilities, you can create more detailed health insights by connecting your InfluxDB database to Grafana. The original [Fitbit Fetch project](https://github.com/arpanghosh8453/public-fitbit-projects) includes a comprehensive Grafana dashboard that you can import.

### Setting up Grafana

1. Install Grafana (either directly or via [Docker](https://github.com/arpanghosh8453/public-docker-config#grafana))
2. Add your InfluxDB as a data source in Grafana:
   - Name: Your choice (e.g., "Fitbit Data")
   - Type: InfluxDB
   - URL: Your InfluxDB URL
   - Database: Your Fitbit database name
   - User & Password: Your InfluxDB credentials

### Importing the Dashboard

1. Download the dashboard JSON file from the [original repository](https://github.com/arpanghosh8453/public-fitbit-projects/tree/main/Grafana_Dashboard)
2. In Grafana:
   - Click the "+" icon in the sidebar
   - Select "Import"
   - Upload the JSON file or paste its contents
   - Select your InfluxDB data source
   - Click "Import"

### Available Visualizations

The pre-built dashboard includes:
- Heart Rate Trends
- Step Count Analysis
- Sleep Pattern Visualization
- SpO2 Measurements
- Activity Zone Minutes
- Device Battery Level
- And more...

### Customizing the Dashboard

Feel free to modify the dashboard to suit your needs:
- Add new panels
- Modify existing visualizations
- Create additional dashboards
- Set up alerts

The InfluxDB database created by this add-on stores all metrics in a structured format, making it easy to create custom visualizations.

## License

This project is under the BSD 4-Clause License. See the LICENSE file for details.