usdot-nad
=========
Supports ArcGIS versions: 10.3.1, 10.4

Script to ETL the SGID AddressPoints to National Address Dataset (NAD) schema and domains.

### Running the script
- Download the NAD_template.gdb from [USDOT](https://www.transportation.gov/gis/national-address-database/geodatabase-template) or [NAD Pilot Project](https://sites.google.com/a/appgeo.com/usdot-national-address-database-pilot-project/home) site.
- Copy the SGID address points locally.
- Point the script to the NAD_template.gdb, address points and an output directory.
- They will send you an FTP link where you can upload the file. (old way before June 2020 was: The output GDB is zipped and uploaded to https://drive.google.com/drive/folders/0Bw2vVDej5PsOQW1KV2NoaUh6NTA)
