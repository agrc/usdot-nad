usdot-nad
=========
Supports ArcGIS Pro versions: 3.x

Script to ETL the SGID AddressPoints to National Address Dataset (NAD) schema and domains.

### Reference Docs
- [About the NAD](https://www.transportation.gov/gis/national-address-database)
- [NAD Disclaimer](https://www.transportation.gov/mission/open/gis/national-address-database/national-address-database-nad-disclaimer)
- [NAD schema documentation](https://www.transportation.gov/gis/nad/nad-schema)
- [StoryMap: Getting to know the NAD](https://storymaps.arcgis.com/stories/9490f773f65d4c6aa8b79facc528a661)
- [Explore the NAD mapper](https://usdot.maps.arcgis.com/apps/mapviewer/index.html?webmap=5dace4a598d343809327473c68b311ff)

### Running the script
- Download the latest NAD file geodatabase template from [USDOT](https://www.transportation.gov/mission/open/gis/national-address-database/national-address-database-nad-disclaimer) site.
- Point the script (etl_nad.py) to the downloaded NAD gdb template, the SGID address points, and an output directory.
- Reach out to Jason Ford or NAD@dot.gov and request a secure FTP link where you can upload the data.
