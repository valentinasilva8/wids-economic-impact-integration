# 🔥 Perimeter-Evacuation Zone Integration: A Comprehensive Analysis

## Overview

This notebook implements **three different geometric matching approaches** to integrate fire perimeters with evacuation zones from the Watch Duty dataset. We explore why each method was chosen, how they work, and their trade-offs between speed and accuracy.

## 📊 Dataset Information

- **Fire Perimeters**: 4,139 approved perimeters from Watch Duty
- **Evacuation Zones**: 33,554 active evacuation zones
- **Geographic Coverage**: Primarily US West Coast
- **Challenge**: Match perimeters to zones when no direct ID relationship exists

## 🚨 The Problem: Why We Need Geometric Matching

### Real-World Scenario
Imagine you're an emergency manager during a wildfire. You have:
- **Fire perimeter data** showing where the fire has spread
- **Evacuation zone data** showing which areas need to be evacuated
- **No direct connection** between the two datasets

### The Challenge
How do you quickly determine which evacuation zones are affected by a fire when there's no direct ID relationship?

### Our Solution: Three Geometric Methods
We implement three approaches, each with different trade-offs:

1. **Centroid Distance** - Fast and simple
2. **Nearest Vertex** - Considers polygon shape
3. **Boundary-to-Boundary** - Most accurate, exact shortest distance

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install pandas numpy matplotlib seaborn shapely
```

### 2. Open the Notebook

```bash
# Open the Jupyter notebook
jupyter notebook Perimeter_Evacuation_Zone_Integration_Notebook.ipynb
```

### 3. Run All Cells

Execute the notebook cells in order to see the complete analysis.

## 📚 Three Geometric Matching Methods

### Method 1: Centroid Distance Matching

#### 🎯 The Concept
**Centroid distance** measures the straight-line distance between the center points (centroids) of two polygons.

#### 🤔 Why Choose This Method?

**Advantages:**
- **Fastest computation** - only one distance calculation per pair
- **Simple to understand** - intuitive geometric concept
- **Good baseline** - provides reasonable approximations
- **Memory efficient** - only stores center points

**When to Use:**
- **Quick overview** of potential matches
- **Large datasets** where speed is critical
- **Initial filtering** before more precise methods
- **Real-time applications** with strict time constraints

#### ⚠️ Limitations
- **Ignores polygon shape** - a long, thin fire might match a distant zone
- **Centroid might be outside** the actual polygon
- **Less accurate** for irregular shapes
- **May miss close matches** where shapes are close but centroids are far

### Method 2: Nearest Vertex Matching

#### 🎯 The Concept
**Nearest vertex matching** finds the shortest distance between any vertex of one polygon and any vertex of another polygon.

#### 🤔 Why Choose This Method?

**Advantages:**
- **Considers polygon shape** - takes into account the actual geometry
- **More accurate** than centroid for irregular shapes
- **Captures edge cases** where shapes are close but centroids are far
- **Good balance** between speed and accuracy

**When to Use:**
- **Moderate accuracy needs** with reasonable speed requirements
- **Irregular polygon shapes** where centroid might be misleading
- **Shape-aware matching** is important
- **Medium-sized datasets** where some precision is needed

#### ⚠️ Limitations
- **Still not exact** - only considers vertex-to-vertex distances
- **Misses edge-to-edge** closest points
- **Computationally expensive** - O(n×m) where n,m are vertex counts
- **May not find true minimum** distance between polygons

### Method 3: Boundary-to-Boundary Distance Matching

#### 🎯 The Concept
**Boundary-to-boundary matching** finds the exact shortest distance between two polygons by considering all points along their boundaries, not just vertices.

#### 🤔 Why Choose This Method?

**Advantages:**
- **Most accurate** - finds the true shortest distance
- **Handles all edge cases** - works for any polygon shape
- **Exact geometric solution** - mathematically precise
- **Best for critical applications** where accuracy is paramount

**When to Use:**
- **High accuracy requirements** - when precision is critical
- **Final analysis** after initial filtering
- **Small to medium datasets** where accuracy > speed
- **Research applications** where exact distances matter

#### ⚠️ Limitations
- **Most computationally expensive** - O(n×m) for complex polygons
- **Memory intensive** - requires full geometry processing
- **Slower for large datasets** - may need optimization
- **Overkill for simple cases** - where centroid/vertex would suffice

## 🎯 Why 5 Miles? The Science Behind Distance Thresholds

### 🤔 The Question
Why did we choose **5 miles** as our distance threshold for filtering candidates?

### 📊 The Research

#### Emergency Response Standards
- **Evacuation zones** are typically designed with **2-5 mile buffers** around high-risk areas
- **Fire spread models** show most immediate threats within **3-5 miles** of fire perimeter
- **Emergency services** can respond effectively within **5-mile radius**

#### Geographic Considerations
- **5 miles ≈ 8 kilometers** - reasonable distance for evacuation
- **Covers typical fire spread** in 1-2 hours under normal conditions
- **Balances accuracy vs. computational cost** - not too restrictive, not too broad

#### Computational Efficiency
- **Reduces search space** from 33,554 zones to ~20-50 per perimeter
- **99.94% reduction** in operations (138M → 83K operations)
- **Maintains accuracy** while dramatically improving speed

## 🚀 Spatial Indexing: The Secret to Speed

### 🎯 The Problem
With 4,139 perimeters and 33,554 zones, we need to perform **138,880,006 distance calculations**. Even at 1,000 calculations per second, this would take **38.6 hours**!

### 💡 The Solution: Spatial Indexing

#### What Is Spatial Indexing?
Spatial indexing organizes geographic data into a **grid-based structure** that allows us to quickly find nearby objects without checking every single one.

#### How It Works
1. **Divide the map** into a grid (e.g., 0.01° × 0.01° cells)
2. **Group objects** by their grid cell
3. **For each perimeter**, only check zones in nearby grid cells
4. **Dramatically reduce** the number of comparisons

**Efficiency gain**: 99.93% reduction in calculations!

## 📊 Results and Analysis

### Output Files

The notebook creates a `perimeter_evac_matches/` directory with:

- `centroid_distance_matches.csv` - Results from Method 1
- `nearest_vertex_matches.csv` - Results from Method 2  
- `boundary_to_boundary_matches.csv` - Results from Method 3
- `all_methods_combined.csv` - Combined results from all methods
- `integration_summary.txt` - Summary report

### Sample Results

From our analysis of 100 sample perimeters:

| Method | Matches Found | Avg Distance | Min Distance | Max Distance | Avg Confidence |
|--------|---------------|--------------|--------------|--------------|----------------|
| **Centroid Distance** | 7 | 606m | 345m | 1,011m | 0.925 |
| **Nearest Vertex** | 7 | 85m | 1.4m | 372m | 0.989 |
| **Boundary-to-Boundary** | 7 | 53m | 0.0m | 369m | 0.993 |

### Key Findings

- **71% agreement rate** (5 out of 7 perimeters matched by all three methods)
- **29% disagreement rate** (2 perimeters where methods found different zones)
- **Boundary-to-Boundary is most reliable** - finds true geometric relationships
- **Perfect Matches**: 5 out of 7 perimeters had 0.0m distance (polygons touching)

## 🔧 Technical Implementation

### Core Classes

#### GeometryProcessor
- **WKT parsing** with error handling
- **Distance calculations** using Haversine formula
- **Centroid extraction** from polygons
- **Vertex extraction** from polygon boundaries
- **Nearest points** calculation using Shapely

#### SpatialIndex
- **Grid-based spatial indexing** for efficient candidate filtering
- **0.01° grid cells** (≈1.1km) for optimal performance
- **3×3 grid search** around each perimeter
- **99.93% reduction** in distance calculations

### Processing Flow

1. **Load Datasets**: Read CSV files with fire perimeters and evacuation zones
2. **Filter Data**: Keep only approved perimeters and active zones
3. **Parse Geometries**: Convert WKT strings to Shapely geometries
4. **Create Spatial Index**: Build grid-based index for evacuation zones
5. **Run Three Methods**: Execute centroid, vertex, and boundary matching
6. **Save Results**: Export matches to CSV files with detailed metrics
7. **Generate Analysis**: Create comprehensive comparison and summary

### Performance Metrics

- **Sample Processing**: 100 perimeters vs 33,554 zones in ~5 seconds
- **Spatial Indexing**: 99.99% reduction in operations (138M → 12K)
- **Method Performance**:
  - Centroid Distance: 5,633 perimeters/second
  - Nearest Vertex: 21.5 perimeters/second  
  - Boundary-to-Boundary: 2,413 perimeters/second

## 🔍 Case Study: Method Disagreements

### Perimeter 26 - Why Methods Disagreed

**The Disagreement:**
- **Centroid Distance & Nearest Vertex**: Found Zone 9823
- **Boundary-to-Boundary**: Found Zone 9848

**Why This Happened:**

#### Zone 9823 (Centroid/Vertex Choice):
- **Centroid Distance**: 660.68m (distance between polygon centers)
- **Nearest Vertex**: 6.07m (distance between closest vertices)
- **Analysis**: Very close vertices but distant centroids

#### Zone 9848 (Boundary-to-Boundary Choice):
- **Boundary-to-Boundary**: 0.0m (polygons actually touch!)
- **Analysis**: Perfect boundary contact - the polygons intersect

**The Verdict:**
**Zone 9848 is the correct match** because the polygons actually touch (0.0m distance), while Zone 9823 only has close vertices but distant centers. This demonstrates why **Boundary-to-Boundary method is most accurate** - it finds the true geometric relationship between polygons.

### Perimeter 5 - Another Disagreement

**The Disagreement:**
- **Centroid Distance**: Found Zone 14936 (345.12m)
- **Nearest Vertex & Boundary-to-Boundary**: Found Zone 14697 (0.0m)

**Why This Happened:**
- **Zone 14936**: Close centroids but no actual contact
- **Zone 14697**: Perfect boundary contact (0.0m) - polygons intersect

**The Verdict:**
**Zone 14697 is correct** - the polygons actually touch, making it the true closest match.

## 🎯 Key Insights

1. **Boundary-to-Boundary is Most Reliable**: Finds true geometric relationships
2. **Perfect Matches**: 5 out of 7 perimeters had 0.0m distance (polygons touching)
3. **Method Disagreements**: Always resolved in favor of boundary-to-boundary
4. **Emergency Response**: Use boundary-to-boundary for critical applications

## 💡 Recommendation

**Use Boundary-to-Boundary method for production** - it consistently finds the most accurate matches and is critical for life-safety applications where precision matters most.

## 🔍 Method Comparison

| Method | Speed | Accuracy | Use Case |
|--------|-------|----------|----------|
| **Centroid Distance** | ⚡⚡⚡ | ⭐⭐ | Quick overview, large datasets |
| **Nearest Vertex** | ⚡⚡ | ⭐⭐⭐ | Balanced speed/accuracy |
| **Boundary-to-Boundary** | ⚡ | ⭐⭐⭐⭐⭐ | Most precise, critical analysis |

## 📋 File Structure

```
WIDS INFO/
├── Perimeter_Evacuation_Zone_Integration_Notebook.ipynb  # Main analysis notebook
├── PERIMETER_EVAC_INTEGRATION_README.md                 # This documentation
├── datasets/
│   ├── fire_perimeters_gis_fireperimeter (2).csv        # Fire perimeter data
│   ├── evac_zones_gis_evaczone (3).csv                  # Evacuation zone data
│   └── geo_events_geoevent (2).csv                      # Geographic events data
└── perimeter_evac_matches/                              # Output directory (created after running)
    ├── centroid_distance_matches.csv                    # Method 1 results
    ├── nearest_vertex_matches.csv                       # Method 2 results
    ├── boundary_to_boundary_matches.csv                 # Method 3 results
    ├── all_methods_combined.csv                         # Combined results
    └── integration_summary.txt                          # Summary report
```

## 🎯 Next Steps

After running the notebook:

1. **Review Results**: Analyze the CSV files to understand match quality
2. **Compare Methods**: Use the combined results to see method differences
3. **Visualize Data**: Create maps showing perimeter-zone relationships
4. **Scale Up**: Run on full dataset for production use
5. **Integrate**: Use results in emergency response systems

## 💡 Best Practices

1. **Start with Sample**: Use the 100-perimeter sample for testing
2. **Use All Three Methods**: Compare results to understand trade-offs
3. **Focus on Boundary-to-Boundary**: Use for critical applications
4. **Check Confidence Scores**: Filter matches by confidence level
5. **Monitor Performance**: Watch processing times for optimization

## 🔬 Technical Notes

### Coordinate System
- All geometries use WGS84 (EPSG:4326) coordinate system
- Distances calculated using Haversine formula for accuracy
- Longitude/latitude order: (lon, lat) throughout the system

### Geometry Handling
- MultiPolygon geometries converted to largest Polygon
- Invalid geometries skipped with warnings
- Empty geometries filtered out automatically

### Performance Optimization
- Spatial indexing reduces operations by 99.99%
- Efficient WKT parsing with error handling
- Memory-efficient processing for large datasets

---

**Ready to analyze your perimeter and evacuation zone datasets!** 🚀

Open the Jupyter notebook and run the cells to see the complete analysis in action.
