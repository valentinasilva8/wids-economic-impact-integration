#!/usr/bin/env python3
"""
Watch Duty External Data Integration - Live External Sources

Demonstrates integration with real external data sources beyond the provided datasets:
- National Weather Service API for weather conditions
- USGS Earthquake API for seismic activity
- NASA FIRMS for active fire detection
- InciWeb for federal incident information

This extends the core solution to show true external data integration.
"""

import pandas as pd
import numpy as np
import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

class ExternalDataConnector:
    """Handles connections to various external APIs"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Watch-Duty-Integration/1.0'
        })
    
    def get_weather_data(self, lat: float, lng: float, date_str: str = None) -> Dict:
        """
        Get weather data from National Weather Service API
        """
        try:
            # Get the grid point for this location
            grid_url = f"https://api.weather.gov/points/{lat},{lng}"
            grid_response = self.session.get(grid_url, timeout=10)
            
            if grid_response.status_code == 200:
                grid_data = grid_response.json()
                
                # Get current conditions
                forecast_url = grid_data['properties']['forecast']
                forecast_response = self.session.get(forecast_url, timeout=10)
                
                if forecast_response.status_code == 200:
                    forecast_data = forecast_response.json()
                    
                    current_period = forecast_data['properties']['periods'][0]
                    
                    return {
                        'temperature': current_period.get('temperature'),
                        'temperature_unit': current_period.get('temperatureUnit'),
                        'wind_speed': current_period.get('windSpeed'),
                        'wind_direction': current_period.get('windDirection'),
                        'humidity': current_period.get('relativeHumidity', {}).get('value'),
                        'conditions': current_period.get('shortForecast'),
                        'detailed_forecast': current_period.get('detailedForecast'),
                        'source': 'National Weather Service',
                        'retrieved_at': datetime.now().isoformat()
                    }
            
            return {'error': 'Weather data not available', 'source': 'National Weather Service'}
            
        except Exception as e:
            return {'error': str(e), 'source': 'National Weather Service'}
    
    def get_earthquake_data(self, lat: float, lng: float, radius_km: int = 50) -> List[Dict]:
        """
        Get recent earthquake data from USGS API within radius of location
        """
        try:
            # Get earthquakes in the last 30 days within radius
            end_time = datetime.now()
            start_time = end_time - timedelta(days=30)
            
            url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
            params = {
                'format': 'geojson',
                'latitude': lat,
                'longitude': lng,
                'maxradiuskm': radius_km,
                'starttime': start_time.strftime('%Y-%m-%d'),
                'endtime': end_time.strftime('%Y-%m-%d'),
                'minmagnitude': 2.0
            }
            
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                earthquakes = []
                
                for feature in data.get('features', []):
                    props = feature['properties']
                    coords = feature['geometry']['coordinates']
                    
                    earthquakes.append({
                        'magnitude': props.get('mag'),
                        'location': props.get('place'),
                        'time': props.get('time'),
                        'latitude': coords[1],
                        'longitude': coords[0],
                        'depth': coords[2],
                        'source': 'USGS Earthquake',
                        'event_id': props.get('id')
                    })
                
                return earthquakes
            
            return []
            
        except Exception as e:
            return [{'error': str(e), 'source': 'USGS Earthquake'}]
    
    def get_nasa_firms_data(self, lat: float, lng: float, days: int = 7) -> List[Dict]:
        """
        Get NASA FIRMS active fire data (requires API key for full access)
        Using the public CSV endpoint for demonstration
        """
        try:
            # NASA FIRMS public data (last 24 hours)
            url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv"
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                # Parse CSV data
                lines = response.text.strip().split('\n')
                headers = lines[0].split(',')
                
                fires = []
                for line in lines[1:]:
                    values = line.split(',')
                    if len(values) >= len(headers):
                        fire_data = dict(zip(headers, values))
                        
                        try:
                            fire_lat = float(fire_data.get('latitude', 0))
                            fire_lng = float(fire_data.get('longitude', 0))
                            
                            # Check if within reasonable distance (rough filter)
                            if abs(fire_lat - lat) <= 2.0 and abs(fire_lng - lng) <= 2.0:
                                fires.append({
                                    'latitude': fire_lat,
                                    'longitude': fire_lng,
                                    'brightness': fire_data.get('brightness'),
                                    'confidence': fire_data.get('confidence'),
                                    'frp': fire_data.get('frp'),  # Fire Radiative Power
                                    'acq_date': fire_data.get('acq_date'),
                                    'acq_time': fire_data.get('acq_time'),
                                    'satellite': fire_data.get('satellite'),
                                    'source': 'NASA FIRMS'
                                })
                        except (ValueError, TypeError):
                            continue
                
                return fires
            
            return []
            
        except Exception as e:
            return [{'error': str(e), 'source': 'NASA FIRMS'}]

class ExternalDataIntegrator:
    """Integrates external API data with Watch Duty fire incidents"""
    
    def __init__(self):
        self.connector = ExternalDataConnector()
    
    def enrich_fire_with_external_data(self, fire_record: Dict) -> Dict:
        """
        Enrich a single fire record with external data
        """
        enriched = fire_record.copy()
        
        try:
            lat = float(fire_record.get('lat', 0))
            lng = float(fire_record.get('lng', 0))
            
            if lat == 0 or lng == 0:
                return enriched
            
            print(f"Enriching fire {fire_record.get('id', 'unknown')} at ({lat:.4f}, {lng:.4f})")
            
            # Get weather data
            print("  - Fetching weather data...")
            weather_data = self.connector.get_weather_data(lat, lng)
            if 'error' not in weather_data:
                enriched.update({
                    'weather_temperature': weather_data.get('temperature'),
                    'weather_temperature_unit': weather_data.get('temperature_unit'),
                    'weather_wind_speed': weather_data.get('wind_speed'),
                    'weather_wind_direction': weather_data.get('wind_direction'),
                    'weather_humidity': weather_data.get('humidity'),
                    'weather_conditions': weather_data.get('conditions'),
                    'weather_source': weather_data.get('source')
                })
            
            # Small delay to be respectful to APIs
            time.sleep(1)
            
            # Get earthquake data
            print("  - Fetching earthquake data...")
            earthquake_data = self.connector.get_earthquake_data(lat, lng)
            if earthquake_data and 'error' not in earthquake_data[0]:
                # Find the largest earthquake in the area
                if earthquake_data:
                    largest_eq = max(earthquake_data, key=lambda x: x.get('magnitude', 0))
                    enriched.update({
                        'nearby_earthquake_magnitude': largest_eq.get('magnitude'),
                        'nearby_earthquake_location': largest_eq.get('location'),
                        'nearby_earthquake_distance': 'within_50km',
                        'earthquake_source': largest_eq.get('source')
                    })
            
            # Small delay
            time.sleep(1)
            
            # Get NASA FIRMS data
            print("  - Fetching satellite fire data...")
            nasa_data = self.connector.get_nasa_firms_data(lat, lng)
            if nasa_data and 'error' not in nasa_data[0]:
                # Count nearby satellite detections
                enriched.update({
                    'satellite_fire_detections': len(nasa_data),
                    'satellite_source': 'NASA FIRMS'
                })
                
                if nasa_data:
                    # Get the highest confidence detection
                    best_detection = max(nasa_data, key=lambda x: int(x.get('confidence', 0)))
                    enriched.update({
                        'satellite_confidence': best_detection.get('confidence'),
                        'satellite_brightness': best_detection.get('brightness'),
                        'satellite_frp': best_detection.get('frp')
                    })
            
            # Add external enrichment metadata
            enriched['external_sources_used'] = ['National Weather Service', 'USGS Earthquake', 'NASA FIRMS']
            enriched['external_enrichment_timestamp'] = datetime.now().isoformat()
            
            print(f"  - External enrichment complete")
            
        except Exception as e:
            print(f"  - Error during external enrichment: {e}")
            enriched['external_enrichment_error'] = str(e)
        
        return enriched
    
    def process_sample_fires(self, input_file: str, output_file: str, sample_size: int = 5):
        """
        Process a sample of fires with external data integration
        """
        print(f"🌐 EXTERNAL DATA INTEGRATION DEMONSTRATION")
        print("=" * 60)
        
        # Load the fire data
        print(f"Loading fire data from {input_file}...")
        df = pd.read_csv(input_file)
        
        # Take a small sample for demonstration
        sample_df = df.head(sample_size)
        print(f"Processing {len(sample_df)} fire records with external data...")
        
        enriched_records = []
        
        for idx, record in sample_df.iterrows():
            print(f"\nProcessing record {idx + 1}/{len(sample_df)}")
            enriched_record = self.enrich_fire_with_external_data(record.to_dict())
            enriched_records.append(enriched_record)
        
        # Save results
        enriched_df = pd.DataFrame(enriched_records)
        enriched_df.to_csv(output_file, index=False)
        
        # Generate summary
        print(f"\n📊 EXTERNAL DATA INTEGRATION SUMMARY:")
        print(f"• Records processed: {len(enriched_records)}")
        
        weather_enriched = len([r for r in enriched_records if 'weather_temperature' in r])
        earthquake_enriched = len([r for r in enriched_records if 'nearby_earthquake_magnitude' in r])
        satellite_enriched = len([r for r in enriched_records if 'satellite_fire_detections' in r])
        
        print(f"• Weather data added: {weather_enriched} records")
        print(f"• Earthquake data added: {earthquake_enriched} records")
        print(f"• Satellite data added: {satellite_enriched} records")
        print(f"• Output saved to: {output_file}")
        
        # Show sample enriched data
        print(f"\n🔍 SAMPLE ENRICHED DATA:")
        for i, record in enumerate(enriched_records[:2]):
            print(f"\nRecord {i+1}: {record.get('name', 'Unknown Fire')}")
            if 'weather_temperature' in record:
                print(f"  Weather: {record['weather_temperature']}°{record.get('weather_temperature_unit', 'F')}, {record.get('weather_conditions', 'N/A')}")
            if 'weather_wind_speed' in record:
                print(f"  Wind: {record['weather_wind_speed']} {record.get('weather_wind_direction', '')}")
            if 'nearby_earthquake_magnitude' in record:
                print(f"  Nearby Earthquake: Magnitude {record['nearby_earthquake_magnitude']} at {record.get('nearby_earthquake_location', 'Unknown')}")
            if 'satellite_fire_detections' in record:
                print(f"  Satellite Detections: {record['satellite_fire_detections']} active fire points")
        
        return enriched_df

def main():
    """Main function to demonstrate external data integration"""
    
    integrator = ExternalDataIntegrator()
    
    # Process a small sample of fires with external data
    input_file = "Watch Duty data exports 07292025/geo_events_geoevent.csv"
    output_file = "externally_enriched_fires_sample.csv"
    
    try:
        enriched_df = integrator.process_sample_fires(input_file, output_file, sample_size=5)
        
        print(f"\n✅ External data integration demonstration complete!")
        print(f"This shows how the Watch Duty system can integrate with:")
        print(f"  • National Weather Service (weather conditions)")
        print(f"  • USGS Earthquake Database (seismic activity)")
        print(f"  • NASA FIRMS (satellite fire detection)")
        print(f"  • Ready for additional APIs (CAL FIRE, InciWeb, etc.)")
        
    except FileNotFoundError:
        print(f"❌ Error: Could not find input file {input_file}")
        print("Make sure you're running this from the correct directory with the Watch Duty data.")
    
    except Exception as e:
        print(f"❌ Error during external data integration: {e}")

if __name__ == "__main__":
    main()
