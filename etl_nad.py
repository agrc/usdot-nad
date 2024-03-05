"""ETL for SGID address points to national address dataset (NAD)."""
import arcpy
import os
from time import strftime

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


def getRenameFieldMap(featurePath, currentName, newName):
    """Create a field map that does a basic field rename."""
    tempMap = arcpy.FieldMap()
    tempMap.addInputField(featurePath, currentName)

    tempName = tempMap.outputField
    tempName.name = newName
    tempName.aliasName = newName
    tempMap.outputField = tempName

    return tempMap


def translateValues(nadPoints):
    """Translate address point values to NAD domain values."""
    with arcpy.da.UpdateCursor(nadPoints,
                               ['St_PreDir', 'St_PosDir', 'St_PosTyp', 'County']) as cursor:
        for row in cursor:
            row[0] = directionDomain.get(row[0], None)
            row[1] = directionDomain.get(row[1], None)
            row[2] = streetDomain.get(row[2], None)
            row[3] = countyFipsDomain.get(row[3], None)
            cursor.updateRow(row)


def populateNewFields(nadPoints):
    """Popluate added fields with values that fit NAD domains."""
    with arcpy.da.UpdateCursor(nadPoints,
                               ['SHAPE@X', 'SHAPE@Y', 'Longitude', 'Latitude', 'NAD_Source'],
                               spatial_reference=arcpy.SpatialReference(4326)) as cursor:
        x = 0
        for row in cursor:
            row[2] = row[0]
            row[3] = row[1]
            row[4] = 'Utah UGRC'
            cursor.updateRow(row)
            x = x + 1
            print x


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


def createFieldMapping(sgidPoints):
    """Create all field maps and add them to a field mapping for append."""
    # Create field mappings
    sgidFMs = arcpy.FieldMappings()

    # Perform some field renaming
    mapPairs = [
        ('State', 'State'),
        ('City', 'Inc_Muni'),
        ('CountyID', 'County'),
        ('ZipCode', 'Zip_Code'),
        ('PrefixDir', 'St_PreDir'),
        ('StreetName', 'St_Name'),
        ('StreetType', 'St_PosTyp'),
        ('SuffixDir', 'St_PosDir'),
        ('AddNum', 'Add_Number'),
        ('FullAdd', 'StNam_Full'),
        ('LandmarkName', 'LandmkName'),
        ('Building', 'Building'),
        ('UnitType', 'Unit'),
        ('AddSource', 'AddAuth'),
        ('AddSystem', 'AddrRefSys'),
        ('LoadDate', 'DateUpdate')]

    for p in mapPairs:
        print p
        sgidFMs.addFieldMap(getRenameFieldMap(sgidPoints, p[0], p[1]))

    return sgidFMs


if __name__ == '__main__':
    """Address Point ETL to NAD schema.
       It is also a good idea to first repair geometery."""
    print "Working"
    # downloadable at: https://www.transportation.gov/gis/national-address-database/geodatabase-template
    baseNadSchema = r'C:\\Users\\gbunce\\Documents\\projects\\NAD_update\\NAD_Template_202310.gdb\\NAD'
    workingSchemaFolder = r'C:\\Users\\gbunce\\Documents\\projects\\NAD_update\\outputs'
    if not os.path.exists(workingSchemaFolder):
        os.mkdir(workingSchemaFolder)
    workingSchema = arcpy.CreateFileGDB_management(workingSchemaFolder, 'NAD_AddressPoints' + uniqueRunNum + '.gdb')[0]
    workingNad = os.path.join(workingSchema, 'NAD')
    
    # sgidAddressPoints = r'Database Connections\\internal@SGID@internal.agrc.utah.gov.sde\\SGID.LOCATION.AddressPoints'
    # address_points_local_gdb = "sgid_data.gdb"
    # if arcpy.Exists(os.path.join(workingSchemaFolder, address_points_local_gdb)):
    #     arcpy.Delete_management(os.path.join(workingSchemaFolder, address_points_local_gdb))
    # address_points_local_gdb = arcpy.CreateFileGDB_management(workingSchemaFolder, address_points_local_gdb)[0]
    # address_points_local = arcpy.Copy_management(sgidAddressPoints, os.path.join(address_points_local_gdb, 'Address_Points_Local'))[0]
    # arcpy.RepairGeometry_management(address_points_local, delete_null=True)
    # print 'Projecting address points'
    # projected_address_points = arcpy.Project_management(in_dataset=sgidAddressPoints,
    #                                                     out_dataset=os.path.join(address_points_local_gdb, 'AddressPointsProject'),
    #                                                     out_coor_system=3857,
    #                                                     transform_method="WGS_1984_(ITRF00)_To_NAD_1983")[0]

    projected_address_points = "C:\Users\gbunce\Documents\projects\NAD_update\outputs\sgid_data.gdb\AddressPointsProject"

    # Non numeric address numbers don't work
    preProccessAddressPoints(projected_address_points)
    arcpy.Copy_management(baseNadSchema, workingNad)

    fieldMap = createFieldMapping(projected_address_points)
    print 'Append points to NAD feature class with field map'
    arcpy.Append_management(projected_address_points,
                            workingNad,
                            schema_type='NO_TEST',
                            field_mapping=fieldMap)
    print 'Populate new fields'
    populateNewFields(workingNad)
    print 'Translate values to NAD domain values'
    translateValues(workingNad)
    # Output GDB is zipped and uploaded to https://drive.google.com/drive/folders/0Bw2vVDej5PsOQW1KV2NoaUh6NTA
    print 'Completed'
