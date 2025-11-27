import pandas as pd

COLUMN_MAPPING = {
    "Main Event Disaster Type": "hasType",
    "Disaster Name": "eventName",
    "Date/Period": "startDate",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Main Area/s Affected / Location": "hasLocation",  
    "Additional Perils/Disaster Sub-Type Occurences (Compound Disaster, e.g. Typhoon Haiyan = rain + wind + storm surge)": "hasSubtype",
    "PREPAREDNESS_Announcements_Warnings Released / Status Alert or Alert/ State of Calamity": "State of Calamity",
    "PREPAREDNESS_No. of Evacuation Centers": "evacuationCenters",
    "IMPACT_Number of Affected Areas_Barangays": "affectedBarangays",
    "IMPACT_Casualties_Dead_Total": "dead",
    "IMPACT_Casualties_Injured_Total": "injured",
    "IMPACT_Casualties_Missing_Total": "missing",
    "IMPACT_Affected_Families": "affectedFamilies",
    "IMPACT_Affected_Persons": "affectedPersons",
    "IMPACT_Evacuated_Families": "displacedFamilies",
    "IMPACT_Evacuated_Persons": "displacedPersons",
    "IMPACT_Damages to Properties_Houses_Fully": "totallyDamagedHouses",
    "IMPACT_Damages to Properties_Houses_Partially": "partiallyDamagedHouses",
    "IMPACT_Damages to Properties_Infrastructure (in Millions)": "infraDamageAmount",
    "IMPACT_Damages to Properties_Agriculture (in Millions)": "agricultureDamageAmount",
    "IMPACT_Damages to Properties_Private/Commercial (in Millions)": "commercialDamageAmount",
    "IMPACT_Status of Lifelines_Electricity or Power Supply": "PowerAffected",
    "IMPACT_Status of Lifelines_Communication Lines": "CommunicationAffected",
    "IMPACT_Status of Lifelines_Transportation_Roads and Bridges": "RoadAndBridgesAffected",
    "IMPACT_Status of Lifelines_Transportation_Seaports": "SeaportsAffected",
    "IMPACT_Status of Lifelines_Transportation_Airports": "AirportsAffected",
    "IMPACT_Status of Lifelines_Water_Dams and other Reservoirs": "areDamsAffected",
    "IMPACT_Status of Lifelines_Water_Tap": "isTapAffected",
    "Allocated Funds for the Affected Area/s": "allocatedFunds",
    "NGO-LGU Support Units Present": "agencyLGUsPresent",
    "International Organizations Present": "internationalOrgsPresent",
    "Amount of Donation from International Organizations (including local NGOs)": "amoungNGOs",
    "Supply of Relief Goods_Canned Goods, Rice, etc._Cost": "itemCost",    # itemTypeOrNeeds: Canned Goods, Rice
    "Supply of Relief Goods_Canned Goods, Rice, etc._Quantity": "itemQty", # itemTypeOrNeeds: Canned Goods, Rice
    "Supply of Relief Goods_Water_Cost": "itemCost",    # itemTypeOrNeeds: Water
    "Supply of Relief Goods_Water_Quantity": "itemQty", # itemTypeOrNeeds: Water
    "Supply of Relief Goods_Clothing_Cost": "itemCost",    # itemTypeOrNeeds: Clothing
    "Supply of Relief Goods_Clothing_Quantity": "itemQty", # itemTypeOrNeeds: Clothing
    "Supply of Relief Goods_Medicine_Cost": "itemCost",    # itemTypeOrNeeds: Medicine
    "Supply of Relief Goods_Medicine_Quantity": "itemQty", # itemTypeOrNeeds: Medicine
    "Supply of Relief Goods_Items Not Specified (Cost)": "itemCost", # itemTypeTypeOrNeeds: Others

}

COLUMNS_TO_CLEAN = {
    "date": "normalize_date",
    "location": "resolve_location",
}

def load_with_tiered_headers(path):
    # Read first 3 rows as headers (0,1,2)
    df = pd.read_excel(path, header=[0, 1, 2])

    # Build merged header strings
    df.columns = [
        "_".join([str(x) for x in col if str(x) != "nan"]).strip()
        for col in df.columns
    ]

    return df

df = load_with_tiered_headers("disaster_report.xlsx")
df = df.rename(columns=COLUMN_MAPPING)