# WiDS Watch Duty Data Dictionary

## Overview

All fields present are described below. Many will have no value to the challenge and can be scrubbed as needed. Any highlighted in red we respectfully request you to prune.

## geo_events_geoevent

- **id** - internal unique ID
- **date_created** - date first entered into WD database
- **date_modified** - date of last user-visible modification, either in meta-data or adding a new report
- **geo_event_type** - "wildfire" or "location". "wildfire" = a wildfire!, "location" = an associated shelter or support location
- **name** - the name of the fire/location
- **is_active** - if "true", the incident is considered ongoing as of the data snapshot
- **description** - unused
- **address** - human-readable address of the incident/location
- **lat, lng** - the latitude and longitude in +/- Decimal Degrees
- **data** - additional metadata encoded as a JSON object (dictionary below is for the "wildfire" geo_event_type)
  - **is_prescribed** - if "true", is a prescribed fire event versus an unplanned incident
  - **prescribed_fire_start / ..._local** - GMT and local time start of the prescribed fire
  - **is_fps** - if "true", Incident Command indicated that Forward Progress has been Stopped
  - **containment** - [0,100] if provided, the containment percentage as provided by Incident Command
  - **acreage** - the total burn area in acres
  - **links** - a JSON-encoded list of important web resources for the incident
  - **has_custom_evacuation_…** - if "true", the text description of the evacuation orders/warnings/advisories is human-generated. Used for counties that do not use pre-planned zones or for other special circumstances
  - **evacuation_…** - a list of named evacuation zone orders/warnings/advisories/notes or other descriptive text indicating same
  - **is_complex_parent** - if "true", represents a top-level Fire Complex (a collection of multiple fire incidents managed together by an Incident Management Team)
  - **reporter_only_notes** - internal notes for reporter-eyes only
  - **temporarily_display_evac_zones** - indication of if pre-planned evacuation zones are displayed on the WD map for this incident, despite no evacuations officially requested
  - **manual_deactivation_started** - if "true", indicates an automated process for deactivation is started
- **notification_type** - "normal" or "silent". Indicates the level of severity of the incident as evaluated by WD operations personnel. "Silent" means that most users will not receive a push notification, and is reserved for minor incidents. We upgrade from "silent" to "normal" as fires progress.
- **external_id** - in the case that an incident is first created using some form of automation, encodes the ID of the incident in the external source data
- **external_source** - an identifier of the specific external source that was used to initially create the incident
- **incident_id** - duplicated from id, unused
- **user_created_id, user_modified_id** - internal IDs indicating what internal user created or last modified the incident
- **reporter_managed** - if "true", a reporter is responsible for making all meta-data updates and adding reports. If "false", these operations are handled by automation.
- **is_visible** - if "true", is visible on the WD map. Often coincides with is_active.
- **data_*** - per-language translations of data

## geo_events_geoeventchangelog

- **date_created** - the date/time when the changes were applied, UTC
- **geo_event_id** - the id from the geo_events_geoevent dataset to which this change is related
- **changes** - a JSON object constructed as `{ <field_name>: [<previous value>, [<new value>], … }` for each field changed

### Additional Changelog Entries

A number of additional changelog entries are synthesized and interleaved with the table contents. These extend the schema of the changes field by adding the following possible entries:

#### radio_traffic_indicates_rate_of_spread

A firefighter has given a Report on Conditions and communicated how quickly the fire front is moving relative to the ground. Possible values are below, with their loose definitions. These values are subjective and thus their definitions are based on subjective analysis from an on-scene human performing a rapid assessment:

- **slow** - "I can easily out walk it"
- **moderate** - "I have to walk at a brisk pace to keep up"
- **rapid** - "It's faster than I can walk"
- **very_rapid** - this term is rarely used in radio traffic, since it's hard to evaluate the difference between "rapid" and "extreme" subjectively
- **extreme** - "It's faster than I could run"

#### radio_traffic_indicates_structure_threat

A firefighter has given a report (often as part of the initial Report on Conditions) indicating that structures are immediately or very soon to be directly impacted by the fire. Loss of structures is very likely without direct intervention.

#### radio_traffic_indicates_spotting

A firefighter has given a report (often as part of the initial Report on Conditions) indicating that the fire front is "spotting" (= creating additional "spot fires" in front of the head of the fire by throwing burning embers up into the air which are carried by the prevailing winds). Possible values are:

- **short_range** - spot fires are in the immediate vicinity of the fire front
- **medium_range** - spot fires are several hundred feet ahead of the fire front
- **long_range** - spot fires are up to miles ahead of the fire front

## fire_perimeters_gis_fireperimeter

- **id** - internal unique ID
- **date_created/_modified** - same as above
- **geo_event_id** - the ID in the geo_events_geoevent dataset to which this perimeter is associated. This is blank for some, which means we weren't tracking whatever fire the perimeter is for
- **approval_status** - "approved", "pending", or "rejected" - outcome of manual review. Only those "approved" should be used in any challenge, as the others had some issue that led us to ignore them
- **source** - original source of the fire perimeter
- **source_unique_id** - opaque unique identifier in the source data
- **source_date_current** - the date provided from the source indicating when the perimeter was considered "current"
- **source_incident_name** - the name in the source metadata for the incident associated
- **source_acres** - acres provided by the source. These often do NOT match the actual perimeter acreage
- **geom** - the perimeter geometry, encoded as e-wkt. Strip the "SRID=4326;" prefix to get simple WKT with decimal degrees WGS84 projection
- **is_visible** - if "true", is displayed on the WD map
- **is_historical** - if "true", is displayed as a "historical" (inactive incident) perimeter versus an "active" (ongoing incident) perimeter
- **source_extra_data** - a JSON-encoded object containing all of the metadata from the source data

### Notes

- A single geo_event_id can have multiple "approved" perimeters. Only one will ever be marked is_visible.
- For the purposes of the challenge, any entry marked "approved" is good to use, and if grouped by geo_event_id and sorted by date_created generates a time-series of perimeters as they have changed over time.

## fire_perimeters_gis_fireperimeterchangelog

Similar to the geoevent change log. There won't be many entries here, other than tracking changes in approval status and visibility. Included for completeness.

## evacuation_zones_gis_evaczone

- **id** - same as above
- **date_created/_modified** - same as above
- **uid_v2** - an ID we use internally, stable for a given evac zone provider & zone name
- **is_active** - if "true", the zone is still used by the provider in question. When they remove a zone, we mark it as is_active = "false"
- **display_name** - human-readable name of the zone
- **region_id** - ID referencing the region (county) the zone belongs to. This ID is internal, and I can provide the regions dataset if you need it. It's easy enough to calculate based on coordinates
- **source_attribution** - display string showing what entity provided the zone data
- **dataset_name** - internal string key for configuration for the zone ingestion pipeline
- **source_extra_data** - unused, future plans
- **geom** - GIS boundary of the evacuation zone. Same encoding as above
- **geom_label** - a point identifying where to draw the label for the zone
- **status** - a string identifying the evacuation zone status (order/warning/advisory). If blank, no status
- **external_status** - a string identifying the status assigned to the zone in the source data, for internal tracking purposes

*Note: Other fields can be ignored. We use them to track splits/joins of evacuation zone shapes.*

## evacuation_zones_gis_evaczonechangelog

- **evac_zone_id** - id from the evaczone dataset
- **date_created** - date/time when the changes were applied
- **changes** - same as above datasets

### Notes

Changes in status over time will be available within this dataset. A series of changelogs that include "status", for a given evac_zone_id, constructs a time series of status changes.