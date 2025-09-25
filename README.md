# WiDS Watch Duty Data Integration Project

## Overview

This project addresses the Watch Duty Datathon challenge of connecting external data sources to the primary wildfire incident database while adding value through data enrichment. The solution demonstrates a complete data science methodology, from initial problem analysis through iterative improvement, resulting in a production-ready system that processes 58,051 records and enriches over 12,000 with evacuation zone information from 18 emergency authorities.

## Problem Statement

The core challenge is to develop a method for connecting external data sources to Watch Duty's primary wildfire incident dataset. The solution needs to:

1. Join datasets using primary keys or alternative matching methods
2. Add meaningful value to the existing Watch Duty data
3. Handle real-world data quality and integration challenges
4. Scale to production-level data volumes
5. Provide clear business value for emergency response operations

## Dataset Description

### Primary Dataset
- **`geo_events_geoevent.csv`** - 58,051 wildfire incident records with geographic coordinates (lat/lng), fire names, and existing integration fields

### External Data Sources
- **`evac_zone_status_geo_event_map.csv`** - Evacuation zone status mapping
- **`externally_enriched_fires_sample.csv`** - Sample of externally enriched fire data
- **`fire_perimeters_gis_fireperimeter.csv`** - Fire boundary polygons with temporal information
- **`geo_events_geoevent.csv`** - Large external incident database from multiple sources

## Solution Architecture

### Multi-Dimensional Entity Resolution System

The solution uses a sophisticated entity resolution system that matches fire incidents to evacuation zones using three key dimensions:

1. **Name Matching Algorithm**
   - Canonicalizes fire names by removing agency conventions
   - Standardizes terminology and formatting
   - Uses Jaccard similarity on word sets for robust matching

2. **Geographic Proximity Algorithm**
   - Calculates geodesic distances between fire locations and evacuation zones
   - Implements emergency response-based scoring (0-25 mile range)
   - Accounts for California's geography and emergency coordination distances

3. **Composite Scoring System**
   - Combines name similarity (30%) and geographic proximity (60%)
   - Includes temporal correlation (10%) for future enhancement
   - Uses 40% minimum confidence threshold for matches

### Quality Validation System

- **California Boundary Validation**: Ensures all matches fall within California boundaries
- **Distance Validation**: Maximum 25-mile limit based on emergency response coordination
- **Quality Flags**: Each match includes validation status and accuracy metrics

## Key Results

- **Total Records Processed**: 58,051 fire incidents
- **Enriched Records**: 12,268 with evacuation zone information
- **Enrichment Rate**: 21.1%
- **Average Distance**: 5.1 miles (excellent precision)
- **Geographic Validation**: 100% California-only matches
- **Authority Coverage**: 18 different emergency authorities

## Project Structure

```
├── datasets/                          # Data files
│   ├── evac_zone_status_geo_event_map.csv
│   ├── externally_enriched_fires_sample.csv
│   ├── fire_perimeters_gis_fireperimeter.csv
│   ├── geo_events_geoevent.csv
│   └── wids_data_dictionary.md
├── sebastian's approach/              # Implementation approaches
│   ├── external_api_integration.md
│   ├── external_data_integrator.py
│   ├── full_scale_integrator.py
│   ├── geo_evac_integration_solution.md
│   └── refined_integrator.py
├── Ideas to discuss University Datathon.pdf
├── joining geo to evac approach.pdf
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.8+
- Required packages: pandas, numpy, geopy, scikit-learn

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd wids-watch-duty-integration
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Place your data files in the `datasets/` directory

### Usage

The main integration scripts are located in the `sebastian's approach/` directory:

- `external_data_integrator.py` - Basic data integration
- `full_scale_integrator.py` - Full-scale processing
- `refined_integrator.py` - Optimized version with quality controls

## Technical Implementation

### Performance Optimizations

- **Spatial Pre-filtering**: Quick geographic bounds checking before expensive calculations
- **Chunked Processing**: 5,000-record chunks for memory optimization
- **Smart Sampling**: Geographic diversity maintenance across California
- **Progress Tracking**: Real-time indicators with ETA calculations

### Data Quality Assurance

- **Schema Validation**: Complete compatibility with input structure
- **Data Integrity**: No record loss during processing
- **Quality Metrics**: Confidence scores, distance measurements, validation flags

## Business Impact

### Emergency Response Enhancement
- Direct links between fire incidents and official evacuation zones
- 5.1-mile average geographic accuracy
- Multi-authority coordination across 18 agencies

### Operational Efficiency
- Automated integration replacing manual cross-referencing
- Sub-minute processing for full dataset
- Enhanced situational awareness for emergency responders

## Future Enhancements

- **Fire Perimeters Integration**: Temporal correlation and boundary validation
- **External Events Integration**: Enhanced performance for large datasets
- **Spatial Indexing**: R-tree implementation for improved performance
- **Real-time Processing**: Live data integration capabilities

## Contributing

This project was developed for the WiDS Watch Duty Datathon. For questions or collaboration opportunities, please refer to the project documentation in the `sebastian's approach/` directory.

## License

This project is developed for educational and research purposes as part of the WiDS Watch Duty Datathon challenge.

## Acknowledgments

- Watch Duty for providing the dataset and challenge
- WiDS (Women in Data Science) for organizing the datathon
- Emergency response authorities for providing evacuation zone data
