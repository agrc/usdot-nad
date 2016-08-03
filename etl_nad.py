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
    49025: 'KANE',
    49027: 'MILLARD',
    49039: 'SANPETE',
    49007: 'CARBON',
    49049: 'UTAH',
    49005: 'CACHE',
    49043: 'SUMMIT',
    49053: 'WASHINGTON',
    49019: 'GRAND',
    49047: 'UINTAH',
    49045: 'TOOELE',
    49041: 'SEVIER',
    49017: 'GARFIELD',
    49003: 'BOX ELDER',
    49021: 'IRON',
    49057: 'WEBER',
    49015: 'EMERY',
    49033: 'RICH',
    49051: 'WASATCH',
    49001: 'BEAVER',
    49009: 'DAGGETT',
    49011: 'DAVIS',
    49055: 'WAYNE',
    49031: 'PIUTE',
    49029: 'MORGAN',
    49035: 'SALT LAKE',
    49013: 'DUCHESNE',
    49023: 'JUAB',
    49037: 'SAN JUAN'
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
                               ['StN_PreDir', 'StN_PosDir', 'StN_PosTyp', 'County']) as cursor:
        for row in cursor:
            row[0] = directionDomain.get(row[0], None)
            row[1] = directionDomain.get(row[1], None)
            row[2] = streetDomain.get(row[2], None)
            row[3] = countyFipsDomain.get(row[3], None)
            cursor.updateRow(row)


def populateNewFields(nadPoints):
    """Popluate added fields with values that fit NAD domains."""

    arcpy.CalculateField_management(nadPoints,
                                    'long',
                                    '!SHAPE.CENTROID.X@DECIMALDEGREES!',
                                    'PYTHON_9.3')

    arcpy.CalculateField_management(nadPoints,
                                    'lat',
                                    '!SHAPE.CENTROID.Y@DECIMALDEGREES!',
                                    'PYTHON_9.3')

    arcpy.CalculateField_management(nadPoints,
                                    'Source',
                                    '"Utah AGRC"',
                                    'PYTHON_9.3')


def preProccessAddressPoints(sgidPoints):
    """Preprocess address points."""
    pass


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
        ('PrefixDir', 'StN_PreDir'),
        ('StreetName', 'StreetName'),
        ('StreetType', 'StN_PosTyp'),
        ('SuffixDir', 'StN_PosDir'),
        ('AddNum', 'Add_Number'),
        ('LandmarkName', 'landmkName'),
        ('Building', 'Building'),
        ('UnitType', 'Unit'),
        ('AddSource', 'AddAuth'),
        ('AddSystem', 'UniqWithin'),
        ('LoadDate', 'LastUpdate')]

    for p in mapPairs:
        print p
        sgidFMs.addFieldMap(getRenameFieldMap(sgidPoints, p[0], p[1]))

    return sgidFMs


if __name__ == '__main__':
    """Address Point must be in NAD coordinate system.
       It is also a good idea to first repair geometery."""
    print "Working"
    baseNadSchema = r'.\data\NAD_template.gdb'
    workingSchema = r'.data\outputs\NAD_AddressPoints' + uniqueRunNum + '.gdb'
    workingNad = os.path.join(workingSchema, 'NAD')
    sgidAddressPoints = r'.\data\temp\sgid.gdb\AddressPoints_Project'
    arcpy.Copy_management(baseNadSchema, workingSchema)

    fieldMap = createFieldMapping(sgidAddressPoints)
    print 'Append points to NAD feature class with field map'
    arcpy.Append_management(sgidAddressPoints,
                            workingNad,
                            schema_type='NO_TEST',
                            field_mapping=fieldMap)
    print 'Populate new fields'
    populateNewFields(workingNad)
    print 'Translate values to NAD domain values'
    translateValues(workingNad)
    print 'Completed'
