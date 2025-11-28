#!/usr/bin/env python3
"""
Configuration file for Economic Impact Integration

Set your API keys and configuration parameters here.
"""

# API Configuration
API_KEYS = {
    # US Census Bureau API Key
    # Get free key at: https://api.census.gov/data/key_signup.html
    'census_api_key': 'YOUR_CENSUS_API_KEY_HERE',
    
    # Google Places API Key
    # Get key at: https://developers.google.com/maps/documentation/places/web-service/get-api-key
    'google_places_key': 'YOUR_GOOGLE_PLACES_KEY_HERE',
    
    # US Department of Education API Key (optional)
    # Get key at: https://api.data.gov/signup/
    'education_api_key': 'YOUR_EDUCATION_API_KEY_HERE'
}

# Processing Configuration
PROCESSING_CONFIG = {
    # Chunk size for processing (records per batch)
    'chunk_size': 1000,
    
    # Minimum time between API calls (seconds)
    'api_rate_limit': 1.0,
    
    # Confidence threshold for data quality
    'confidence_threshold': 0.3,
    
    # Maximum retries for failed API calls
    'max_retries': 3,
    
    # Timeout for API calls (seconds)
    'api_timeout': 10
}

# Data Sources Configuration
DATA_SOURCES = {
    'census_base_url': 'https://api.census.gov/data',
    'google_places_base_url': 'https://maps.googleapis.com/maps/api/place',
    'education_base_url': 'https://api.data.gov/ed',
    'reverse_geocoding_url': 'https://api.bigdatacloud.net/data/reverse-geocode-client'
}

# Economic Impact Calculation Weights
IMPACT_WEIGHTS = {
    'tourism_exposure': 0.50,
    'small_business_vulnerability': 0.30,
    'educational_disruption': 0.20
}

# File Paths
FILE_PATHS = {
    'input_file': 'datasets/geo_events_geoevent (2).csv',
    'output_file': 'watch_duty_with_economic_impact.csv',
    'intermediate_prefix': 'intermediate_economic_results_chunk_',
    'log_file': 'economic_integration.log'
}

# Quality Control Settings
QUALITY_CONTROL = {
    'min_zipcode_length': 5,
    'max_impact_index': 1.0,
    'min_impact_index': 0.0,
    'required_fields': ['zipcode', 'economic_impact_index']
}
