"""
ETL for SGID address points to national address dataset (NAD).

#only 3 of UGRC SGID Address Point fields are not used to match: OBJECTID, PtLocation, Structure
#2 NAD Always Used fields are not populated: Urbnztn_PR, UUID

Estimate Time: 
- projected address data has been downloaded locally (option 2): 3 h 20 m
"""
import arcpy
import os
from time import strftime, time
import re

# Start the timer
start_time = time()

uniqueRunNum = strftime("%Y%m%d_%H%M%S")

streetDomain = {
    'JCT': 'JUNCTION',
    'COR': 'CORNER',
    'GLN': 'GLEN',
    'VW': 'VIEW',
    'PARK': 'PARK',
    'FLT': 'FLAT',
    'BAY': 'BAY',
    'CIR': 'CIRCLE',
    'FWY': 'FREEWAY',
    'HL': 'HILL',
    'ESTS': 'ESTATES',
    'BLVD': 'BOULEVARD',
    'CRK': 'CREEK',
    'ST': 'STREET',
    'DR': 'DRIVE',
    'RTE': 'ROUTE',
    'EXPY': 'EXPRESSWAY',
    'CTR': 'CENTER',
    'PT': 'POINT',
    'LN': 'LANE',
    'MNR': 'MANOR',
    'RAMP': 'RAMP',
    'VLG': 'VILLAGE',
    'PKWY': 'PARKWAY',
    'RD': 'ROAD',
    'HOLW': 'HOLLOW',
    'HWY': 'HIGHWAY',
    'TER': 'TERRACE',
    'BND': 'BEND',
    'CYN': 'CANYON',
    'PL': 'PLACE',
    'ROW': 'ROW',
    'GRV': 'GROVE',
    'EST': 'ESTATE',
    'TRL': 'TRAIL',
    'FRK': 'FORK',
    'RNCH': 'RANCH',
    'RDG': 'RIDGE',
    'WAY': 'WAY',
    'PASS': 'PASS',
    'RUN': 'RUN',
    'TRCE': 'TRACE',
    'CV': 'COVE',
    'CT': 'COURT',
    'XING': 'CROSSING',
    'SQ': 'SQUARE',
    'CRES': 'CRESCENT',
    'PLZ': 'PLAZA',
    'MDW': 'MEADOW',
    'HTS': 'HEIGHTS',
    'ALY': 'ALLEY',
    'AVE': 'AVENUE',
    'LOOP': 'LOOP'
}

directionDomain = {
    'S': 'SOUTH',
    'E': 'EAST',
    'W': 'WEST',
    'N': 'NORTH'
}

countyFipsDomain = {
    '49025': 'KANE',
    '49027': 'MILLARD',
    '49039': 'SANPETE',
    '49007': 'CARBON',
    '49049': 'UTAH',
    '49005': 'CACHE',
    '49043': 'SUMMIT',
    '49053': 'WASHINGTON',
    '49019': 'GRAND',
    '49047': 'UINTAH',
    '49045': 'TOOELE',
    '49041': 'SEVIER',
    '49017': 'GARFIELD',
    '49003': 'BOX ELDER',
    '49021': 'IRON',
    '49057': 'WEBER',
    '49015': 'EMERY',
    '49033': 'RICH',
    '49051': 'WASATCH',
    '49001': 'BEAVER',
    '49009': 'DAGGETT',
    '49011': 'DAVIS',
    '49055': 'WAYNE',
    '49031': 'PIUTE',
    '49029': 'MORGAN',
    '49035': 'SALT LAKE',
    '49013': 'DUCHESNE',
    '49023': 'JUAB',
    '49037': 'SAN JUAN'
}

tribeDomain = {
    #https://www2.census.gov/geo/docs/reference/codes/AIAlist.txt

def getRenameFieldMap(featurePath, currentName, newName):
    """Create a field map that does a basic field rename."""
    tempMap = arcpy.FieldMap()
    tempMap.addInputField(featurePath, currentName)

    tempName = tempMap.outputField
    tempName.name = newName
    tempName.aliasName = newName
    tempMap.outputField = tempName

    return tempMap

    'Goshute': 'Goshute Reservation',
    'Shoshone': 'Northwestern Shoshone Reservation',
    'Ute Mountain Ute': 'Ute Mountain Reservation',
    'Navajo': 'Navajo Nation Reservation',
    'Paiute': 'Paiute (UT) Reservation',
    'Ute': 'Uintah and Ouray Reservation'
}

def translateValues(nadPoints):
    """Translate address point values and a tribe value to NAD domain values."""
    fields = ['St_PreDir', 'St_PosDir', 'St_PosTyp', 'County', 'NatAmArea']
    with arcpy.da.UpdateCursor(nadPoints, fields) as cursor:
        for row in cursor:
            row = [
                directionDomain.get(row[0], None),
                directionDomain.get(row[1], None),
                streetDomain.get(row[2], None),
                countyFipsDomain.get(row[3], None),
                tribeDomain.get(row[4], None),
            ]
            cursor.updateRow(row)

def populateNewFields(nadPoints): 
    """Populate added fields with values that fit NAD domains."""
    fields = [
        'SHAPE@X', 'SHAPE@Y', 'Longitude', 'Latitude', 'NAD_Source', 
        'St_Name', 'St_PreTyp', 'AddNo_Full', 'Add_Number', 'AddNum_Suf', 'AddrPoint'
    ]
    highway_prefixes = ['HIGHWAY ', 'HWY ', 'US ', 'SR ']
    old_highway_prefixes = ['OLD HIGHWAY ', 'OLD HWY ']
    
    with arcpy.da.UpdateCursor(nadPoints, fields, spatial_reference=arcpy.SpatialReference(4326)) as cursor:
        for row in cursor:
            # Assign coordinate-based fields
            row[2], row[3] = row[0], row[1]
            row[4] = 'Utah Geospatial Resource Center (UGRC)'
            row[10] = f"{round(row[0], 6)} {round(row[1], 6)}"

            # Process street name prefixes safely
            street_name = row[5] if row[5] else ""  # Ensure it's a string

            for prefix in highway_prefixes:
                if street_name.startswith(prefix):
                    row[6], row[5] = prefix.strip(), street_name[len(prefix):]
                    break
            for prefix in old_highway_prefixes:
                if street_name.startswith(prefix):
                    row[6], row[5] = prefix.strip(), street_name[len(prefix):]
                    break
            
            row[7] = f"{row[8]} {row[9]}" if row[9] else row[8]
            cursor.updateRow(row)

def preProccessAddressPoints(sgidPoints):
    """Preprocess address points."""
    removed_count = 0
    non_digit_oids = []
    address_number_field = 'AddNum'
    with arcpy.da.UpdateCursor(sgidPoints, ['OID@', address_number_field]) as cursor:
        for oid, add_num in cursor:
            if not add_num.isdigit():
                cursor.deleteRow()
                non_digit_oids.append(oid)
                removed_count += 1

    print 'OBJECTID in ({})'.format(','.join([str(x) for x in non_digit_oids]))
    print removed_count


        
        print(f"Removed {len(non_digit_oids)} points: {where_clause}")

def calculateNatAmArea(nadPoints):
    """Spatially joins tribal lands to NAD points and updates the 'NatAmArea' field."""
    land_ownership_url = "https://gis.trustlands.utah.gov/mapping/rest/services/Land_Ownership/FeatureServer/0"
    local_tribal_lands = "in_memory/local_tribal_lands"

    if not arcpy.Exists(local_tribal_lands):
        arcpy.CopyFeatures_management(land_ownership_url, local_tribal_lands)

    polygon_layer = arcpy.MakeFeatureLayer_management(local_tribal_lands, "tribal_lands", "state_lgd = 'Tribal Lands'")
    output_joined = "in_memory/joined_NatAmArea"

    arcpy.SpatialJoin_analysis(nadPoints, polygon_layer, output_joined, "JOIN_ONE_TO_ONE", "KEEP_ALL", match_option="INTERSECT")

    tribe_dict = {row[1]: row[0] for row in arcpy.da.SearchCursor(output_joined, ["tribe", "TARGET_FID"])}

    with arcpy.da.UpdateCursor(nadPoints, ["NatAmArea", "OBJECTID"]) as update_cursor:
        for row in update_cursor:
            if row[1] in tribe_dict:
                row[0] = tribe_dict[row[1]]
                update_cursor.updateRow(row)

    arcpy.Delete_management(output_joined)

def calculateCensus_Plc(nadPoints):
    """Spatially joins Census Place to NAD points and updates the 'Census Place Name' field."""
    utah_census_places_2020_url = "https://services1.arcgis.com/99lidPhWCzftIe9K/ArcGIS/rest/services/CensusPlaces2020/FeatureServer/0"
    local_census_places = "in_memory/local_census_places"

    if not arcpy.Exists(local_census_places):
        arcpy.CopyFeatures_management(utah_census_places_2020_url, local_census_places)

    polygon_layer = arcpy.MakeFeatureLayer_management(local_census_places, "CDP", "LSAD20 = '57'")
    output_joined = "in_memory/joined_Census_Plc_NAMELSAD20"

    arcpy.SpatialJoin_analysis(nadPoints, polygon_layer, output_joined, "JOIN_ONE_TO_ONE", "KEEP_ALL", match_option="INTERSECT")

    CDP_dict = {row[1]: row[0] for row in arcpy.da.SearchCursor(output_joined, ["NAMELSAD20", "TARGET_FID"])}

    with arcpy.da.UpdateCursor(nadPoints, ["Census_Plc", "OBJECTID"]) as update_cursor:
        for row in update_cursor:
            if row[1] in CDP_dict:
                row[0] = CDP_dict[row[1]]
                update_cursor.updateRow(row)

    arcpy.Delete_management(output_joined)

def calc_Post_City(nadPoints):
    #Use the ZipCodes SGID layer to find the matching 5 digit zip code and fill the Post_City with the city Name

    rest_service_url = "https://services1.arcgis.com/99lidPhWCzftIe9K/ArcGIS/rest/services/UtahZipCodeAreas/FeatureServer/0"

    # Fields to work with
    zip_code_field = "Zip_Code"
    post_city_field = "Post_City"
    rest_zip5_field = "ZIP5"
    rest_name_field = "NAME"

    
    # Create an update cursor for the point layer
    with arcpy.da.UpdateCursor(nadPoints, [zip_code_field, post_city_field]) as update_cursor:
        for row in update_cursor:
            zip_code = row[0]
            post_city = None  # Initialize post_city to None

            # Create a search cursor for the REST service
            query = f"{rest_zip5_field} = '{zip_code}'"
            with arcpy.da.SearchCursor(rest_service_url, [rest_name_field], query) as search_cursor:
                for search_row in search_cursor:
                    post_city = search_row[0]
                    break  # Assuming only one match per zip code

            # Update the Post_City field if a match was found
            if post_city:
                row[1] = post_city
                update_cursor.updateRow(row)
            #else:
                #print(f"No match found for Zip_Code: {zip_code}")

    print("Post_City field updated successfully.")

def calc_street(nadPoints):
    update_count = 0
    # Calculate "STREET" field where applicable
    fields = ['St_PreDir', 'St_PreTyp', 'St_Name', 'St_PosDir', 'St_PosTyp', 'StNam_Full']
    with arcpy.da.UpdateCursor(nadPoints, fields) as cursor:
        print("Looping through rows in FC ...")
        for row in cursor:
            # Handle None values
            parts = [row[i] if row[i] is not None else '' for i in range(5)]
            
            # Construct the full street name
            new_value = re.sub(r'\s+', ' ', " ".join(parts).strip())
            
            # Update only if there is a change
            if row[5] != new_value:
                row[5] = new_value
                cursor.updateRow(row)
                update_count += 1
    
    print("Total count of updates to {0}: {1}".format(fields[5], update_count))

def blanks_to_nulls(nadPoints):

    update_count = 0
    flist = ['ADDNUM_SUF', 'BUILDING', 'UNIT', 'LandmkName', 'Inc_Muni', 'Parcel_ID', 'Addr_Type', 'St_PosTyp', 'St_PosDir', 'St_PreDir', 'Placement']
    fields = arcpy.ListFields(nadPoints)

    field_list = []
    for field in fields:
        if field.name in flist:
            print("{} appended to field_list".format(field.name))
            field_list.append(field)

    with arcpy.da.UpdateCursor(nadPoints, flist) as cursor:
        print("Looping through rows in FC ...")
        for row in cursor:
            for i in range(len(field_list)):
                if row[i] == '' or row[i] == ' ':
                    #print(f"Found blank in {field_list[i].name}, ObjectID: {row[0]}")
                    update_count += 1
                    row[i] = None
                    #print(f"Set {field_list[i].name} to {row[i]}, ObjectID: {row[0]}") # Debugging line
            try:
                cursor.updateRow(row)
                #print(f"Row updated successfully, ObjectID: {row[0]}") #Debugging line
            except Exception as e:
                print(f"Error updating row: {e}, ObjectID: {row[0]}")

    print("Total count of blanks converted to NULLs is: {}".format(update_count))

if __name__ == '__main__':
    """Address Point ETL to NAD schema.
    It is also a good idea to first repair geometery."""

    print ("Working")
    # downloadable at: https://www.transportation.gov/gis/national-address-database/geodatabase-template
    baseNadSchema = r'C:\Users\hchou\Project\usdot-nad\NAD_Template_202310.gdb\Addr_Point'
    workingSchemaFolder = r'C:\Users\hchou\Project\usdot-nad\outputs'
    if not os.path.exists(workingSchemaFolder):
        os.mkdir(workingSchemaFolder)
    workingSchema = arcpy.CreateFileGDB_management(workingSchemaFolder, 'NAD_AddressPoints' + uniqueRunNum + '.gdb')[0]
    workingNad = os.path.join(workingSchema, 'NAD')

    #Option 1
    sgidAddressPoints = r"C:\\Users\\hchou\\AppData\\Roaming\\ESRI\\ArcGISPro\\Favorites\\internal@SGID@internal.agrc.utah.gov.sde\\SGID.LOCATION.AddressPoints"
    address_points_local_gdb = "sgid_data.gdb"
    if arcpy.Exists(os.path.join(workingSchemaFolder, address_points_local_gdb)):
        arcpy.Delete_management(os.path.join(workingSchemaFolder, address_points_local_gdb))
    address_points_local_gdb = arcpy.CreateFileGDB_management(workingSchemaFolder, address_points_local_gdb)[0]
    address_points_local = arcpy.Copy_management(sgidAddressPoints, os.path.join(address_points_local_gdb, 'Address_Points_Local'))[0]
    arcpy.RepairGeometry_management(address_points_local, delete_null=True)
    print ('Projecting address points')
    projected_address_points = arcpy.Project_management(in_dataset=sgidAddressPoints,
                                                        out_dataset=os.path.join(address_points_local_gdb, 'AddressPointsProject'),
                                                        out_coor_system=3857,
                                                        transform_method="WGS_1984_(ITRF00)_To_NAD_1983")[0]
    
    #: use option 2 if you already have the sgid address points downloaded locally (and comment out the code block above)

    #Option 2
    #projected_address_points = r'C:\Users\hchou\Project\usdot-nad\outputs\sgid_data.gdb\AddressPointsProject'

    preProccessAddressPoints(projected_address_points)
    arcpy.Copy_management(baseNadSchema, workingNad)
    print ('Append points to NAD feature class with field map')
    arcpy.Append_management(projected_address_points,
                            workingNad,
                            schema_type='NO_TEST',
                            #field_mapping=fieldMap
                            field_mapping=r'AddNum_Pre "AddNum_Pre" true true false 15 String 0 0,First,#;Add_Number "Add_Number" true true false 4 Long 0 0,First,#,AddressPointsProject,AddNum,0,9;AddNum_Suf "AddNum_Suf" true true false 15 String 0 0,First,#,AddressPointsProject,AddNumSuffix,0,3;AddNo_Full "AddNo_Full" true true false 100 String 0 0,First,#;St_PreMod "St_PreMod" true true false 15 String 0 0,First,#;St_PreDir "St_PreDir" true true false 10 String 0 0,First,#,AddressPointsProject,PrefixDir,0,9;St_PreTyp "St_PreTyp" true true false 50 String 0 0,First,#;St_PreSep "St_PreSep" true true false 20 String 0 0,First,#;St_Name "St_Name" true true false 254 String 0 0,First,#,AddressPointsProject,StreetName,0,49;St_PosTyp "St_PosTyp" true true false 50 String 0 0,First,#,AddressPointsProject,StreetType,0,3;St_PosDir "St_PosDir" true true false 10 String 0 0,First,#,AddressPointsProject,SuffixDir,0,9;St_PosMod "St_PosMod" true true false 25 String 0 0,First,#;Building "Building" true true false 75 String 0 0,First,#,C:\Users\hchou\Project\usdot-nad\outputs\sgid_data.gdb\AddressPointsProject,Building,0,74;Floor "Floor" true true false 75 String 0 0,First,#;Unit "Unit" true true false 75 String 0 0,First,#,AddressPointsProject,UnitType,0,19;Room "Room" true true false 75 String 0 0,First,#;Seat "Seat" true true false 75 String 0 0,First,#;Addtl_Loc "Addtl_Loc" true true false 225 String 0 0,First,#;SubAddress "SubAddress" true true false 255 String 0 0,First,#;LandmkName "LandmkName" true true false 150 String 0 0,First,#,AddressPointsProject,LandmarkName,0,74;County "County" true true false 100 String 0 0,First,#,AddressPointsProject,CountyID,0,14;Inc_Muni "Inc_Muni" true true false 100 String 0 0,First,#,AddressPointsProject,City,0,29;Post_City "Post_City" true true false 40 String 0 0,First,#;Census_Plc "Census_Plc" true true false 100 String 0 0,First,#;Uninc_Comm "Uninc_Comm" true true false 100 String 0 0,First,#;Nbrhd_Comm "Nbrhd_Comm" true true false 100 String 0 0,First,#;NatAmArea "NatAmArea" true true false 100 String 0 0,First,#;NatAmSub "NatAmSub" true true false 100 String 0 0,First,#;Urbnztn_PR "Urbnztn_PR" true true false 100 String 0 0,First,#;PlaceOther "PlaceOther" true true false 100 String 0 0,First,#;PlaceNmTyp "PlaceNmTyp" true true false 50 String 0 0,First,#;State "State" true true false 2 String 0 0,First,#,AddressPointsProject,State,0,1;Zip_Code "Zip_Code" true true false 7 String 0 0,First,#,AddressPointsProject,ZipCode,0,4;Plus_4 "Plus_4" true true false 4 String 0 0,First,#;UUID "UUID" true true false 38 Guid 0 0,First,#;AddAuth "AddAuth" true true false 100 String 0 0,First,#,AddressPointsProject,AddSource,0,29;AddrRefSys "AddrRefSys" true true false 75 String 0 0,First,#,AddressPointsProject,AddSystem,0,39;Longitude "Longitude" true true false 8 Double 0 0,First,#;Latitude "Latitude" true true false 8 Double 0 0,First,#;NatGrid "NatGrid" true true false 50 String 0 0,First,#,AddressPointsProject,USNG,0,9;Elevation "Elevation" true true false 2 Short 0 0,First,#;Placement "Placement" true true false 25 String 0 0,First,#,AddressPointsProject,PtLocation,0,24;AddrPoint "AddrPoint" true true false 50 String 0 0,First,#;Related_ID "Related_ID" true true false 50 String 0 0,First,#;RelateType "RelateType" true true false 50 String 0 0,First,#;ParcelSrc "ParcelSrc" true true false 50 String 0 0,First,#;Parcel_ID "Parcel_ID" true true false 50 String 0 0,First,#,AddressPointsProject,ParcelID,0,29;AddrClass "AddrClass" true true false 50 String 0 0,First,#;Lifecycle "Lifecycle" true true false 50 String 0 0,First,#;Effective "Effective" true true false 8 Date 0 0,First,#;Expire "Expire" true true false 8 Date 0 0,First,#;DateUpdate "DateUpdate" true true false 8 Date 0 0,First,#,AddressPointsProject,LoadDate,-1,-1;AnomStatus "AnomStatus" true true false 50 String 0 0,First,#;LocatnDesc "LocatnDesc" true true false 75 String 0 0,First,#;Addr_Type "Addr_Type" true true false 50 String 0 0,First,#,AddressPointsProject,PtType,0,14;DeliverTyp "DeliverTyp" true true false 50 String 0 0,First,#;NAD_Source "NAD_Source" true true false 75 String 0 0,First,#;DataSet_ID "DataSet_ID" true true false 254 String 0 0,First,#,AddressPointsProject,UTAddPtID,0,139'
                            )
    print ('Populate new fields')
    populateNewFields(workingNad)
    print ('Translate values to NAD domain values')

    arcpy.RepairGeometry_management(workingNad)
    print(f"Geometry repaired for: {workingNad}")

    calculateNatAmArea(workingNad)
    calculateCensus_Plc(workingNad)
    calc_Post_City(workingNad)
    translateValues(workingNad)
    blanks_to_nulls(workingNad)
    calc_street(workingNad)
    print ('Completed')

    # End the timer
    end_time = time()

    # Calculate elapsed time
    elapsed_seconds = end_time - start_time

    # Convert to hours, minutes, and seconds
    hours = int(elapsed_seconds // 3600)
    minutes = int((elapsed_seconds % 3600) // 60)
    seconds = int(elapsed_seconds % 60)

    # Print the result in a readable format
    print(f"Process completed in {hours}h {minutes}m {seconds}s.")
