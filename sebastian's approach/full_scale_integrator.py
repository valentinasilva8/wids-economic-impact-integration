#!/usr/bin/env python3
"""
Watch Duty Full-Scale Data Integration System

Optimized version for processing the complete dataset with:
- Chunked processing for memory efficiency
- Spatial pre-filtering for performance
- Progress indicators and time estimates
- Better error handling and recovery
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

@dataclass
class MatchResult:
    """Represents a match between primary and external data"""
    primary_id: int
    external_id: str
    external_source: str
    confidence_score: float
    match_factors: Dict[str, float] = field(default_factory=dict)
    enrichment_data: Dict[str, Any] = field(default_factory=dict)

class ProgressTracker:
    """Enhanced progress tracking with time estimates"""
    
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
        
        # Update every 5 seconds or at completion
        if current_time - self.last_update > 5 or processed == self.total_items:
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

class OptimizedEntityMatcher:
    """Optimized matching with spatial pre-filtering"""
    
    def __init__(self):
        self.name_weight = 0.4
        self.geo_weight = 0.4
        self.temporal_weight = 0.2
        
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
    
    def calculate_distance_miles(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two points"""
        try:
            return geodesic((lat1, lng1), (lat2, lng2)).miles
        except:
            return float('inf')
    
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
    
    def spatial_prefilter(self, primary_lat: float, primary_lng: float, 
                         external_records: List[Dict], lat_field: str, lng_field: str,
                         max_distance_miles: float = 25.0) -> List[Dict]:
        """Pre-filter external records by geographic proximity"""
        
        if not primary_lat or not primary_lng:
            return []
        
        filtered = []
        
        # Quick bounding box filter (much faster than distance calculation)
        # Approximate: 1 degree ≈ 69 miles
        degree_buffer = max_distance_miles / 69.0
        
        for record in external_records:
            try:
                ext_lat = float(record.get(lat_field, 0))
                ext_lng = float(record.get(lng_field, 0))
                
                if (abs(primary_lat - ext_lat) <= degree_buffer and 
                    abs(primary_lng - ext_lng) <= degree_buffer):
                    filtered.append(record)
            except:
                continue
        
        return filtered
    
    def calculate_match_score(self, primary_record: Dict, external_record: Dict,
                            geo_fields: Tuple[str, str], name_field: str) -> Tuple[float, Dict[str, float]]:
        """Calculate match score with optimized performance"""
        
        factors = {}
        
        # Name similarity (quick check first)
        name_score = self.name_similarity_score(
            primary_record.get('name', ''),
            external_record.get(name_field, '')
        )
        factors['name'] = name_score
        
        # If name score is too low, skip expensive geographic calculation
        if name_score < 0.1:
            factors['geographic'] = 0.0
            factors['temporal'] = 0.0
            return name_score * self.name_weight, factors
        
        # Geographic similarity
        geo_score = 0.0
        try:
            primary_lat = float(primary_record.get('lat', 0))
            primary_lng = float(primary_record.get('lng', 0))
            ext_lat = float(external_record.get(geo_fields[0], 0))
            ext_lng = float(external_record.get(geo_fields[1], 0))
            
            if all([primary_lat, primary_lng, ext_lat, ext_lng]):
                distance = self.calculate_distance_miles(primary_lat, primary_lng, ext_lat, ext_lng)
                if distance <= 5.0:  # Within 5 miles
                    geo_score = max(0.1, 1.0 - (distance / 5.0))
        except:
            pass
        
        factors['geographic'] = geo_score
        factors['temporal'] = 0.5  # Default temporal score for performance
        
        # Weighted composite score
        composite_score = (
            self.name_weight * name_score +
            self.geo_weight * geo_score +
            self.temporal_weight * factors['temporal']
        )
        
        return composite_score, factors

class FullScaleIntegrator:
    """Full-scale data integration with optimizations"""
    
    def __init__(self, data_directory: str):
        self.data_directory = Path(data_directory)
        self.matcher = OptimizedEntityMatcher()
        
        # Processing parameters
        self.chunk_size = 5000  # Process 5K records at a time
        self.evacuation_sample_size = 2000  # Use larger sample of evacuation zones
        self.confidence_threshold = 0.3
        
        # Data containers
        self.external_data = {}
        
    def load_external_data_optimized(self):
        """Load and prepare external data for efficient matching"""
        
        print("🔄 Loading external data sources...")
        
        # Load evacuation zones (sample for performance)
        evac_path = self.data_directory / "Watch Duty data exports 07292025" / "evac_zones_gis_evaczone.csv"
        print(f"Loading evacuation zones from {evac_path}")
        
        evac_df = pd.read_csv(evac_path)
        print(f"Total evacuation zones: {len(evac_df):,}")
        
        # Sample for performance, but use a larger sample than demo
        if len(evac_df) > self.evacuation_sample_size:
            evac_sample = evac_df.sample(n=self.evacuation_sample_size, random_state=42)
            print(f"Using sample of {len(evac_sample):,} evacuation zones for performance")
        else:
            evac_sample = evac_df
        
        # Pre-process evacuation zones for faster matching
        evac_records = []
        for idx, row in evac_sample.iterrows():
            record = row.to_dict()
            
            # Extract coordinates from geometry if possible
            geom_label = str(record.get('geom_label', ''))
            if 'POINT(' in geom_label:
                coords = re.findall(r'POINT\(([-\d.]+)\s+([-\d.]+)\)', geom_label)
                if coords:
                    record['extracted_lng'] = float(coords[0][0])
                    record['extracted_lat'] = float(coords[0][1])
                    evac_records.append(record)
        
        self.external_data['evacuation_zones'] = evac_records
        print(f"✅ Prepared {len(evac_records):,} evacuation zone records for matching")
        
        # Load PulsePoint data from primary dataset
        primary_path = self.data_directory / "Watch Duty data exports 07292025" / "geo_events_geoevent.csv"
        print("Loading PulsePoint records from primary dataset...")
        
        pulsepoint_records = []
        chunk_count = 0
        
        for chunk in pd.read_csv(primary_path, chunksize=10000):
            chunk_count += 1
            pulsepoint_chunk = chunk[chunk['external_source'] == 'pulsepoint']
            
            if len(pulsepoint_chunk) > 0:
                pulsepoint_records.extend(pulsepoint_chunk.to_dict('records'))
            
            if chunk_count % 3 == 0:
                print(f"  Processed {chunk_count * 10000:,} records, found {len(pulsepoint_records)} PulsePoint records")
        
        self.external_data['pulsepoint'] = pulsepoint_records
        print(f"✅ Found {len(pulsepoint_records):,} PulsePoint records")
        
        return len(evac_records) + len(pulsepoint_records)
    
    def process_chunk(self, chunk_df: pd.DataFrame, chunk_num: int) -> List[Dict]:
        """Process a chunk of primary records"""
        
        enriched_records = []
        
        for idx, record in chunk_df.iterrows():
            record_dict = record.to_dict()
            matches = []
            
            # Parse existing data field
            if 'data' in record_dict and record_dict['data']:
                try:
                    record_dict['data_parsed'] = json.loads(record_dict['data'])
                except:
                    record_dict['data_parsed'] = {}
            
            # Match against evacuation zones
            evac_matches = self.find_evacuation_matches(record_dict)
            matches.extend(evac_matches)
            
            # Enrich the record
            enriched_record = self.enrich_record(record_dict, matches)
            enriched_records.append(enriched_record)
        
        return enriched_records
    
    def find_evacuation_matches(self, primary_record: Dict) -> List[MatchResult]:
        """Find evacuation zone matches for a primary record"""
        
        matches = []
        
        try:
            primary_lat = float(primary_record.get('lat', 0))
            primary_lng = float(primary_record.get('lng', 0))
            
            if not primary_lat or not primary_lng:
                return matches
            
            # Spatial pre-filtering
            filtered_zones = self.matcher.spatial_prefilter(
                primary_lat, primary_lng,
                self.external_data['evacuation_zones'],
                'extracted_lat', 'extracted_lng',
                max_distance_miles=15.0
            )
            
            best_match = None
            best_score = 0.0
            
            for zone_record in filtered_zones:
                try:
                    distance = self.matcher.calculate_distance_miles(
                        primary_lat, primary_lng,
                        zone_record['extracted_lat'], zone_record['extracted_lng']
                    )
                    
                    if distance <= 10.0:  # Within 10 miles
                        score = max(0.1, 1.0 - (distance / 10.0))
                        
                        if score > best_score and score >= self.confidence_threshold:
                            best_score = score
                            best_match = MatchResult(
                                primary_id=primary_record['id'],
                                external_id=str(zone_record.get('uid_v2', '')),
                                external_source='evacuation_zones',
                                confidence_score=score,
                                match_factors={'geographic': score, 'name': 0, 'temporal': 0},
                                enrichment_data={
                                    'evacuation_zone': zone_record.get('display_name', ''),
                                    'evacuation_source': zone_record.get('source_attribution', ''),
                                    'evacuation_dataset': zone_record.get('dataset_name', ''),
                                    'evacuation_distance_miles': round(distance, 2)
                                }
                            )
                except:
                    continue
            
            if best_match:
                matches.append(best_match)
        
        except Exception as e:
            # Silently continue on errors for robustness
            pass
        
        return matches
    
    def enrich_record(self, primary_record: Dict, matches: List[MatchResult]) -> Dict:
        """Enrich a primary record with match data"""
        
        enriched = primary_record.copy()
        
        if not matches:
            return enriched
        
        # Add enrichment data
        enrichment_sources = []
        enrichment_log = []
        confidence_scores = []
        
        for match in matches:
            enrichment_sources.append(match.external_source)
            confidence_scores.append(match.confidence_score)
            
            for field, value in match.enrichment_data.items():
                if value is not None and str(value).strip():
                    enriched[field] = value
                    enrichment_log.append(f"Added {field}: {value}")
        
        # Add metadata
        enriched['enrichment_sources'] = enrichment_sources
        enriched['enrichment_log'] = enrichment_log
        enriched['match_confidence_avg'] = np.mean(confidence_scores)
        
        return enriched
    
    def run_full_integration(self) -> str:
        """Run integration on the complete dataset"""
        
        print("🚀 Starting Full-Scale Watch Duty Data Integration")
        print("=" * 60)
        
        start_time = time.time()
        
        # Load external data
        external_count = self.load_external_data_optimized()
        print(f"✅ External data loaded: {external_count:,} records")
        
        # Prepare for processing
        primary_path = self.data_directory / "Watch Duty data exports 07292025" / "geo_events_geoevent.csv"
        
        # Count total records first
        print("📊 Counting total records...")
        total_records = sum(1 for _ in pd.read_csv(primary_path, chunksize=1000))
        print(f"Total records to process: {total_records:,}")
        
        # Set up progress tracking
        progress = ProgressTracker(total_records, "Processing primary records")
        
        # Process in chunks
        all_enriched_records = []
        processed_count = 0
        enriched_count = 0
        chunk_num = 0
        
        print(f"\n🔄 Processing {total_records:,} records in chunks of {self.chunk_size:,}...")
        
        for chunk in pd.read_csv(primary_path, chunksize=self.chunk_size):
            chunk_num += 1
            
            # Process this chunk
            chunk_results = self.process_chunk(chunk, chunk_num)
            all_enriched_records.extend(chunk_results)
            
            # Update statistics
            processed_count += len(chunk)
            chunk_enriched = sum(1 for r in chunk_results if r.get('enrichment_sources'))
            enriched_count += chunk_enriched
            
            # Update progress
            progress.update(processed_count)
            
            # Save intermediate results every 10 chunks for safety
            if chunk_num % 10 == 0:
                self.save_intermediate_results(all_enriched_records, chunk_num)
        
        # Final progress update
        progress.update(total_records)
        
        # Save final results
        output_path = self.data_directory / "full_scale_enriched_data.csv"
        enriched_df = pd.DataFrame(all_enriched_records)
        enriched_df.to_csv(output_path, index=False)
        
        # Generate summary
        total_time = time.time() - start_time
        enrichment_rate = (enriched_count / total_records) * 100
        
        summary = f"""
🎉 FULL-SCALE INTEGRATION COMPLETE!

📊 FINAL RESULTS:
• Total Records Processed: {total_records:,}
• Records Enriched: {enriched_count:,}
• Enrichment Rate: {enrichment_rate:.1f}%
• Processing Time: {str(timedelta(seconds=int(total_time)))}
• Average Speed: {total_records/total_time:.1f} records/second

💾 Output saved to: {output_path}

🚀 Ready for analysis and presentation!
        """
        
        print(summary)
        return str(output_path)
    
    def save_intermediate_results(self, results: List[Dict], chunk_num: int):
        """Save intermediate results for recovery"""
        intermediate_path = self.data_directory / f"intermediate_results_chunk_{chunk_num}.csv"
        pd.DataFrame(results).to_csv(intermediate_path, index=False)

def main():
    """Main execution function"""
    
    data_directory = "/Users/sebastian_a/Downloads/Datathon - Work Product"
    integrator = FullScaleIntegrator(data_directory)
    
    try:
        output_file = integrator.run_full_integration()
        print(f"\n✅ Integration complete! Results saved to: {output_file}")
        
    except KeyboardInterrupt:
        print("\n⚠️  Integration interrupted by user")
        print("Intermediate results may be available in intermediate_results_*.csv files")
        
    except Exception as e:
        print(f"\n❌ Error during integration: {e}")
        print("Check logs for details")

if __name__ == "__main__":
    main()
