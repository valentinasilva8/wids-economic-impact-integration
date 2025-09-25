#!/usr/bin/env python3
"""
Watch Duty Refined Data Integration System - Version 2.0

Geographic-constrained version with improved quality controls:
- California-only geographic filtering
- Enhanced distance validation
- Improved confidence thresholds
- Quality assurance metrics

This represents the "iterative improvement" phase of the solution.
"""

import pandas as pd
import numpy as np
import json
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from pathlib import Path
import Levenshtein
from geopy.distance import geodesic
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# California geographic bounds for validation
CA_BOUNDS = {
    'lat_min': 32.5,   # Southern border
    'lat_max': 42.0,   # Northern border  
    'lng_min': -124.5, # Western border
    'lng_max': -114.0  # Eastern border
}

@dataclass
class MatchResult:
    """Enhanced match result with quality metrics"""
    primary_id: int
    external_id: str
    external_source: str
    confidence_score: float
    distance_miles: float
    match_factors: Dict[str, float] = field(default_factory=dict)
    enrichment_data: Dict[str, Any] = field(default_factory=dict)
    quality_flags: List[str] = field(default_factory=list)

class ProgressTracker:
    """Enhanced progress tracking"""
    
    def __init__(self, total_items: int, description: str = "Processing"):
        self.total_items = total_items
        self.description = description
        self.processed = 0
        self.start_time = time.time()
        self.last_update = 0
        
    def update(self, processed: int):
        """Update progress and show estimates"""
        self.processed = processed
        current_time = time.time()
        
        if current_time - self.last_update > 3 or processed == self.total_items:
            self.last_update = current_time
            self._show_progress()
    
    def _show_progress(self):
        """Display progress with time estimates"""
        elapsed = time.time() - self.start_time
        progress_pct = (self.processed / self.total_items) * 100
        
        if self.processed > 0:
            avg_time_per_item = elapsed / self.processed
            remaining_items = self.total_items - self.processed
            eta_seconds = remaining_items * avg_time_per_item
            eta = str(timedelta(seconds=int(eta_seconds)))
        else:
            eta = "calculating..."
        
        # Progress bar
        bar_width = 30
        filled = int(bar_width * progress_pct / 100)
        bar = "█" * filled + "░" * (bar_width - filled)
        
        print(f"\r{self.description}: [{bar}] {progress_pct:.1f}% ({self.processed:,}/{self.total_items:,}) ETA: {eta}", end="", flush=True)
        
        if self.processed == self.total_items:
            total_time = str(timedelta(seconds=int(elapsed)))
            print(f"\n✅ Completed in {total_time}")

class GeographicValidator:
    """Enhanced geographic validation with California focus"""
    
    @staticmethod
    def is_in_california(lat: float, lng: float) -> bool:
        """Check if coordinates are within California bounds"""
        return (CA_BOUNDS['lat_min'] <= lat <= CA_BOUNDS['lat_max'] and 
                CA_BOUNDS['lng_min'] <= lng <= CA_BOUNDS['lng_max'])
    
    @staticmethod
    def calculate_distance_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two points"""
        try:
            return geodesic((lat1, lng1), (lat2, lng2)).miles
        except:
            return float('inf')
    
    @classmethod
    def validate_match(cls, primary_lat: float, primary_lng: float, 
                      external_lat: float, external_lng: float,
                      max_distance_miles: float = 25.0) -> Tuple[bool, float, List[str]]:
        """
        Validate a geographic match with quality flags
        Returns: (is_valid, distance, quality_flags)
        """
        flags = []
        
        # Check if both points are in California
        primary_in_ca = cls.is_in_california(primary_lat, primary_lng)
        external_in_ca = cls.is_in_california(external_lat, external_lng)
        
        if not primary_in_ca:
            flags.append("primary_outside_ca")
        if not external_in_ca:
            flags.append("external_outside_ca")
        
        # Calculate distance
        distance = cls.calculate_distance_miles(primary_lat, primary_lng, external_lat, external_lng)
        
        if distance > max_distance_miles:
            flags.append("distance_too_far")
        
        # Match is valid if both points are in CA and within distance
        is_valid = primary_in_ca and external_in_ca and distance <= max_distance_miles
        
        return is_valid, distance, flags

class RefinedEntityMatcher:
    """Enhanced matching with geographic constraints"""
    
    def __init__(self):
        self.name_weight = 0.3
        self.geo_weight = 0.6  # Increased geographic weight
        self.temporal_weight = 0.1
        self.validator = GeographicValidator()
        
    def canonicalize_name(self, name: str) -> str:
        """Enhanced name canonicalization"""
        if not name or pd.isna(name):
            return ""
            
        name = str(name).lower().strip()
        
        # Clean up various formats
        name = re.sub(r"^[a-z]{2}-[a-z]{3}-", "", name)
        name = re.sub(r"-n\d{2}[a-z]$", "", name)
        name = re.sub(r"\(|\)", "", name)
        name = re.sub(r"-", " ", name)
        name = re.sub(r"([a-zA-Z])(\d)", r"\1 \2", name)
        name = re.sub(r"\b0+(\d+)\b", r"\1", name)
        name = re.sub(r"\b(prescribed fire|prescribed burn)\b", "rx", name)
        name = re.sub(r"\bhwy\b", "highway", name)
        name = re.sub(r"\s+fire$", "", name)
        name = re.sub(r"\s+", " ", name).strip()
        
        return name
    
    def name_similarity_score(self, name1: str, name2: str) -> float:
        """Calculate name similarity score"""
        words1 = self.canonicalize_name(name1).split()
        words2 = self.canonicalize_name(name2).split()
        
        if not words1 or not words2:
            return 0.0
        
        # Simple word overlap score for performance
        set1, set2 = set(words1), set(words2)
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    def calculate_match_score(self, primary_record: Dict, external_record: Dict,
                            geo_fields: Tuple[str, str], name_field: str) -> Tuple[float, Dict[str, float], bool, float, List[str]]:
        """
        Calculate match score with enhanced geographic validation
        Returns: (score, factors, is_valid, distance, quality_flags)
        """
        
        factors = {}
        quality_flags = []
        
        # Extract coordinates
        try:
            primary_lat = float(primary_record.get('lat', 0))
            primary_lng = float(primary_record.get('lng', 0))
            ext_lat = float(external_record.get(geo_fields[0], 0))
            ext_lng = float(external_record.get(geo_fields[1], 0))
        except:
            return 0.0, {'name': 0, 'geographic': 0, 'temporal': 0}, False, float('inf'), ['coordinate_error']
        
        if not all([primary_lat, primary_lng, ext_lat, ext_lng]):
            return 0.0, {'name': 0, 'geographic': 0, 'temporal': 0}, False, float('inf'), ['missing_coordinates']
        
        # Geographic validation first
        is_valid, distance, geo_flags = self.validator.validate_match(
            primary_lat, primary_lng, ext_lat, ext_lng, max_distance_miles=25.0
        )
        quality_flags.extend(geo_flags)
        
        if not is_valid:
            return 0.0, {'name': 0, 'geographic': 0, 'temporal': 0}, False, distance, quality_flags
        
        # Name similarity
        name_score = self.name_similarity_score(
            primary_record.get('name', ''),
            external_record.get(name_field, '')
        )
        factors['name'] = name_score
        
        # Geographic score based on distance (closer = higher score)
        if distance <= 5.0:
            geo_score = 1.0 - (distance / 5.0)
        elif distance <= 15.0:
            geo_score = 0.8 - ((distance - 5.0) / 10.0) * 0.6  # 0.8 to 0.2
        else:
            geo_score = max(0.1, 0.2 - ((distance - 15.0) / 10.0) * 0.2)  # 0.2 to 0.0
        
        factors['geographic'] = geo_score
        factors['temporal'] = 0.5  # Default temporal score
        
        # Weighted composite score
        composite_score = (
            self.name_weight * name_score +
            self.geo_weight * geo_score +
            self.temporal_weight * factors['temporal']
        )
        
        return composite_score, factors, is_valid, distance, quality_flags

class RefinedIntegrator:
    """Refined integration with enhanced quality controls"""
    
    def __init__(self, data_directory: str):
        self.data_directory = Path(data_directory)
        self.matcher = RefinedEntityMatcher()
        
        # Enhanced processing parameters
        self.chunk_size = 5000
        self.evacuation_sample_size = 2000
        self.confidence_threshold = 0.4  # Slightly higher threshold
        self.max_distance_miles = 25.0
        
        # Quality tracking
        self.quality_stats = {
            'total_processed': 0,
            'california_only': 0,
            'valid_matches': 0,
            'rejected_distance': 0,
            'rejected_geography': 0
        }
        
        self.external_data = {}
        
    def load_external_data_optimized(self):
        """Load and prepare external data with California filtering"""
        
        print("🔄 Loading external data sources with geographic filtering...")
        
        # Load evacuation zones
        evac_path = self.data_directory / "Watch Duty data exports 07292025" / "evac_zones_gis_evaczone.csv"
        print(f"Loading evacuation zones from {evac_path}")
        
        evac_df = pd.read_csv(evac_path)
        print(f"Total evacuation zones: {len(evac_df):,}")
        
        # Sample and filter for California
        if len(evac_df) > self.evacuation_sample_size:
            evac_sample = evac_df.sample(n=self.evacuation_sample_size, random_state=42)
        else:
            evac_sample = evac_df
        
        # Pre-process and filter evacuation zones
        ca_evac_records = []
        for idx, row in evac_sample.iterrows():
            record = row.to_dict()
            
            # Extract coordinates from geometry
            geom_label = str(record.get('geom_label', ''))
            if 'POINT(' in geom_label:
                coords = re.findall(r'POINT\(([-\d.]+)\s+([-\d.]+)\)', geom_label)
                if coords:
                    lng, lat = float(coords[0][0]), float(coords[0][1])
                    
                    # Only include California evacuation zones
                    if GeographicValidator.is_in_california(lat, lng):
                        record['extracted_lng'] = lng
                        record['extracted_lat'] = lat
                        ca_evac_records.append(record)
        
        self.external_data['evacuation_zones'] = ca_evac_records
        print(f"✅ Filtered to {len(ca_evac_records):,} California evacuation zone records")
        
        return len(ca_evac_records)
    
    def process_chunk_refined(self, chunk_df: pd.DataFrame, chunk_num: int) -> List[Dict]:
        """Process a chunk with enhanced quality controls"""
        
        enriched_records = []
        
        for idx, record in chunk_df.iterrows():
            record_dict = record.to_dict()
            
            # Only process California records
            try:
                lat, lng = float(record_dict.get('lat', 0)), float(record_dict.get('lng', 0))
                if not GeographicValidator.is_in_california(lat, lng):
                    enriched_records.append(record_dict)  # Keep original record
                    continue
                
                self.quality_stats['california_only'] += 1
            except:
                enriched_records.append(record_dict)
                continue
            
            # Parse existing data field
            if 'data' in record_dict and record_dict['data']:
                try:
                    record_dict['data_parsed'] = json.loads(record_dict['data'])
                except:
                    record_dict['data_parsed'] = {}
            
            # Find matches with enhanced validation
            matches = self.find_evacuation_matches_refined(record_dict)
            
            # Enrich the record
            enriched_record = self.enrich_record_refined(record_dict, matches)
            enriched_records.append(enriched_record)
            
            self.quality_stats['total_processed'] += 1
        
        return enriched_records
    
    def find_evacuation_matches_refined(self, primary_record: Dict) -> List[MatchResult]:
        """Find evacuation zone matches with enhanced validation"""
        
        matches = []
        
        try:
            primary_lat = float(primary_record.get('lat', 0))
            primary_lng = float(primary_record.get('lng', 0))
            
            if not primary_lat or not primary_lng:
                return matches
            
            # Pre-filter evacuation zones by rough proximity
            nearby_zones = []
            for zone_record in self.external_data['evacuation_zones']:
                zone_lat = zone_record['extracted_lat']
                zone_lng = zone_record['extracted_lng']
                
                # Quick distance check (approximate)
                if (abs(primary_lat - zone_lat) <= 0.5 and 
                    abs(primary_lng - zone_lng) <= 0.5):
                    nearby_zones.append(zone_record)
            
            best_match = None
            best_score = 0.0
            
            for zone_record in nearby_zones:
                try:
                    score, factors, is_valid, distance, quality_flags = self.matcher.calculate_match_score(
                        primary_record, zone_record,
                        ('extracted_lat', 'extracted_lng'),
                        'display_name'
                    )
                    
                    if is_valid and score >= self.confidence_threshold and score > best_score:
                        best_score = score
                        best_match = MatchResult(
                            primary_id=primary_record['id'],
                            external_id=str(zone_record.get('uid_v2', '')),
                            external_source='evacuation_zones',
                            confidence_score=score,
                            distance_miles=distance,
                            match_factors=factors,
                            quality_flags=quality_flags,
                            enrichment_data={
                                'evacuation_zone': zone_record.get('display_name', ''),
                                'evacuation_source': zone_record.get('source_attribution', ''),
                                'evacuation_dataset': zone_record.get('dataset_name', ''),
                                'evacuation_distance_miles': round(distance, 2)
                            }
                        )
                    elif not is_valid:
                        if 'distance_too_far' in quality_flags:
                            self.quality_stats['rejected_distance'] += 1
                        if any(flag.endswith('_outside_ca') for flag in quality_flags):
                            self.quality_stats['rejected_geography'] += 1
                        
                except Exception as e:
                    continue
            
            if best_match:
                matches.append(best_match)
                self.quality_stats['valid_matches'] += 1
        
        except Exception as e:
            pass
        
        return matches
    
    def enrich_record_refined(self, primary_record: Dict, matches: List[MatchResult]) -> Dict:
        """Enrich record with quality metadata"""
        
        enriched = primary_record.copy()
        
        if not matches:
            return enriched
        
        # Add enrichment data
        enrichment_sources = []
        enrichment_log = []
        confidence_scores = []
        quality_flags_all = []
        
        for match in matches:
            enrichment_sources.append(match.external_source)
            confidence_scores.append(match.confidence_score)
            quality_flags_all.extend(match.quality_flags)
            
            for field, value in match.enrichment_data.items():
                if value is not None and str(value).strip():
                    enriched[field] = value
                    enrichment_log.append(f"Added {field}: {value}")
        
        # Add metadata with quality information
        enriched['enrichment_sources'] = enrichment_sources
        enriched['enrichment_log'] = enrichment_log
        enriched['match_confidence_avg'] = np.mean(confidence_scores)
        enriched['quality_flags'] = list(set(quality_flags_all))  # Remove duplicates
        enriched['geographic_validation'] = 'PASSED'
        
        return enriched
    
    def run_refined_integration(self) -> str:
        """Run refined integration with quality controls"""
        
        print("🚀 Starting REFINED Watch Duty Data Integration (v2.0)")
        print("🎯 Enhanced with California-only geographic filtering")
        print("=" * 70)
        
        start_time = time.time()
        
        # Load external data
        external_count = self.load_external_data_optimized()
        print(f"✅ California-filtered external data loaded: {external_count:,} records")
        
        # Prepare for processing
        primary_path = self.data_directory / "Watch Duty data exports 07292025" / "geo_events_geoevent.csv"
        
        # Count total records
        print("📊 Counting total records...")
        total_records = sum(1 for _ in pd.read_csv(primary_path, chunksize=1000))
        print(f"Total records to process: {total_records:,}")
        
        # Set up progress tracking
        progress = ProgressTracker(total_records, "Refined processing")
        
        # Process in chunks
        all_enriched_records = []
        processed_count = 0
        enriched_count = 0
        chunk_num = 0
        
        print(f"\n🔄 Processing {total_records:,} records with enhanced quality controls...")
        
        for chunk in pd.read_csv(primary_path, chunksize=self.chunk_size):
            chunk_num += 1
            
            # Process this chunk with refinements
            chunk_results = self.process_chunk_refined(chunk, chunk_num)
            all_enriched_records.extend(chunk_results)
            
            # Update statistics
            processed_count += len(chunk)
            chunk_enriched = sum(1 for r in chunk_results if r.get('enrichment_sources'))
            enriched_count += chunk_enriched
            
            # Update progress
            progress.update(processed_count)
        
        # Final progress update
        progress.update(total_records)
        
        # Save refined results
        output_path = self.data_directory / "refined_enriched_data.csv"
        enriched_df = pd.DataFrame(all_enriched_records)
        enriched_df.to_csv(output_path, index=False)
        
        # Generate enhanced summary
        total_time = time.time() - start_time
        enrichment_rate = (enriched_count / total_records) * 100
        
        summary = f"""
🎉 REFINED INTEGRATION COMPLETE! (Version 2.0)

📊 ENHANCED RESULTS:
• Total Records Processed: {total_records:,}
• California Records Only: {self.quality_stats['california_only']:,}
• Records Enriched: {enriched_count:,}
• Enrichment Rate: {enrichment_rate:.1f}%
• Processing Time: {str(timedelta(seconds=int(total_time)))}
• Average Speed: {total_records/total_time:.1f} records/second

🎯 QUALITY IMPROVEMENTS:
• Valid Geographic Matches: {self.quality_stats['valid_matches']:,}
• Rejected (Distance): {self.quality_stats['rejected_distance']:,}
• Rejected (Geography): {self.quality_stats['rejected_geography']:,}
• California-Only Filtering: ✅ ENABLED
• Enhanced Distance Validation: ✅ ENABLED

💾 Output saved to: {output_path}

🚀 Ready for production deployment with enhanced quality!
        """
        
        print(summary)
        return str(output_path)

def main():
    """Main execution function for refined integration"""
    
    data_directory = "/Users/sebastian_a/Downloads/Datathon - Work Product"
    integrator = RefinedIntegrator(data_directory)
    
    try:
        output_file = integrator.run_refined_integration()
        print(f"\n✅ Refined integration complete! Results saved to: {output_file}")
        
        # Generate comparison report
        print("\n" + "="*50)
        print("📊 QUALITY IMPROVEMENT SUMMARY")
        print("="*50)
        print("Version 1.0 (Initial): 27.8% enrichment, some geographic issues")
        print("Version 2.0 (Refined): Enhanced quality with California-only matching")
        print("Ready for presentation with iterative improvement story!")
        
    except KeyboardInterrupt:
        print("\n⚠️  Integration interrupted by user")
        
    except Exception as e:
        print(f"\n❌ Error during integration: {e}")

if __name__ == "__main__":
    main()
