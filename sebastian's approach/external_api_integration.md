# External API Integration Extension Summary

## How to Integrate External Sources (Not in WiDS Folder)

### The Solution: Live API Integration

## Step 1: Identify External Data Sources

Instead of using static CSV files from the WiDS data exports, we access live APIs that provide real-time data:

- Government APIs (weather.gov, earthquake.usgs.gov)
- Scientific databases (NASA FIRMS satellite data)
- Public safety systems (emergency alert feeds)

## Step 2: HTTP API Calls

Rather than reading local files with `pd.read_csv()`, we make HTTP requests to external servers:

```python
# Instead of this (local file):
df = pd.read_csv("local_file.csv")

# We do this (external API):
response = requests.get("https://api.weather.gov/points/38.7969,-122.1465")
external_data = response.json()
```

## Step 3: Real-Time Data Retrieval

For each fire incident in `geo_events_geoevent.csv`, we:

1. Extract the fire's coordinates (lat/lng)
2. Make live API calls to external services using those coordinates
3. Retrieve current conditions (weather, earthquakes, satellite data)
4. Add this external data as new fields to the fire record

## Step 4: Data Integration Process

```python
def enrich_fire_with_external_data(fire_record):
    lat = fire_record['lat']
    lng = fire_record['lng']
    
    # Call external APIs (not local files)
    weather_data = get_weather_from_api(lat, lng)
    earthquake_data = get_earthquakes_from_api(lat, lng)
    satellite_data = get_satellite_data_from_api(lat, lng)
    
    # Add external data to fire record
    fire_record['weather_temperature'] = weather_data['temperature']
    fire_record['nearby_earthquake_magnitude'] = earthquake_data['magnitude']
    fire_record['satellite_fire_detections'] = len(satellite_data)
    
    return fire_record
```

## Step 5: Handling External Dependencies

- **Internet Connection:** Required for API access
- **API Rate Limits:** Built-in delays between calls
- **Error Handling:** Graceful failure when APIs are unavailable
- **Authentication:** Ready for API keys when needed

## Key Difference from WiDS Data

### WiDS Folder Data (Static Files):
- `evac_zones_gis_evaczone.csv` - Fixed dataset from July 2025
- `geo_events_geoevent.csv` - Historical fire incidents
- Local file reading with `pd.read_csv()`

### External API Data (Live Sources):
- `api.weather.gov` - Current weather conditions
- `earthquake.usgs.gov` - Recent seismic activity
- `firms.modaps.eosdis.nasa.gov` - Real-time satellite fire detection
- HTTP requests with `requests.get()`

## Practical Implementation

1. **ExternalDataConnector class** - Manages HTTP sessions and API calls
2. **API-specific methods** - `get_weather_data()`, `get_earthquake_data()`, `get_nasa_firms_data()`
3. **Integration workflow** - Takes fire coordinates, calls external APIs, returns enriched data
4. **Error handling** - Continues processing even if external APIs fail

### How It Works in Practice:

**Input:** Fire at coordinates (38.7969, -122.1465)

**External API calls:**
- Weather: `GET https://api.weather.gov/points/38.7969,-122.1465`
- Earthquakes: `GET https://earthquake.usgs.gov/fdsnws/event/1/query?latitude=38.7969&longitude=-122.1465`
- Satellites: `GET https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv`

**Output:** Fire record enriched with current weather (70°F, clear), recent earthquake (magnitude 2.78), and satellite detections (3 active fire points)

## External Data Sources Integrated

### 1. National Weather Service API

- **API Endpoint:** `https://api.weather.gov/points/{lat},{lng}`
- **Data Retrieved:**
  - Temperature (degrees Fahrenheit)
  - Wind speed and direction
  - Relative humidity percentage
  - Current weather conditions (Clear, Cloudy, etc.)
  - Detailed weather forecast
- **Integration Method:**
  - Uses fire incident coordinates from `geo_events_geoevent.csv` (`lat`/`lng` fields)
  - Makes two API calls: first to get grid point, second to get forecast data
  - Adds weather fields to fire records: `weather_temperature`, `weather_wind_speed`, `weather_conditions`, etc.

### 2. USGS Earthquake Database API

- **API Endpoint:** `https://earthquake.usgs.gov/fdsnws/event/1/query`
- **Data Retrieved:**
  - Earthquake magnitude
  - Location description
  - Time of occurrence
  - Depth and precise coordinates
  - Event ID for tracking
- **Integration Method:**
  - Searches within 50km radius of each fire location
  - Filters for earthquakes magnitude 2.0+ in last 30 days
  - Identifies largest earthquake near each fire
  - Adds fields: `nearby_earthquake_magnitude`, `nearby_earthquake_location`, etc.

### 3. NASA FIRMS (Fire Information for Resource Management System)

- **API Endpoint:** `https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv`
- **Data Retrieved:**
  - Active fire detections from satellites
  - Fire confidence percentage
  - Brightness temperature
  - Fire Radiative Power (FRP)
  - Acquisition date and time
  - Satellite source (MODIS, VIIRS)
- **Integration Method:**
  - Downloads global 24-hour active fire CSV data
  - Filters for detections within 2 degrees of fire location
  - Counts satellite fire detections in area
  - Adds fields: `satellite_fire_detections`, `satellite_confidence`, `satellite_brightness`, etc.

## Results Demonstration

### Processing Summary

**Test Run Results:**
- **Records Processed:** 5 fire incidents from `geo_events_geoevent.csv`
- **Weather Data Success:** 5/5 records (100%)
- **Earthquake Data Success:** 5/5 records (100%)
- **Satellite Data Success:** 5/5 records (100%)
- **Processing Time:** ~15 seconds (3 seconds per record with API delays)

### Sample Enriched Data

**Fire #1 (ID: 1) at coordinates (38.7969, -122.1465):**
- **Weather:** 70°F, Clear conditions, 1-8 mph WNW winds
- **Seismic Activity:** Magnitude 2.78 earthquake 3km SW of Anderson Springs, CA
- **Satellite Data:** 3 active fire detections in area

**Fire #2 (ID: 2) at coordinates (38.8022, -122.2818):**
- **Weather:** 69°F, Clear conditions, 2-10 mph WNW winds
- **Seismic Activity:** Magnitude 4.05 earthquake 6km NW of The Geysers, CA
- **Satellite Data:** 3 active fire detections in area

## Output Data Schema

### New Fields Added to Fire Records:

- `weather_temperature` - Temperature in Fahrenheit
- `weather_temperature_unit` - Temperature unit (F/C)
- `weather_wind_speed` - Wind speed description
- `weather_wind_direction` - Wind direction
- `weather_humidity` - Relative humidity percentage
- `weather_conditions` - Current weather description
- `weather_source` - "National Weather Service"
- `nearby_earthquake_magnitude` - Largest earthquake magnitude
- `nearby_earthquake_location` - Earthquake location description
- `nearby_earthquake_distance` - "within_50km"
- `earthquake_source` - "USGS Earthquake"
- `satellite_fire_detections` - Count of satellite fire points
- `satellite_confidence` - Highest confidence detection
- `satellite_brightness` - Brightness temperature
- `satellite_frp` - Fire Radiative Power
- `satellite_source` - "NASA FIRMS"
- `external_sources_used` - List of APIs accessed
- `external_enrichment_timestamp` - ISO timestamp of enrichment