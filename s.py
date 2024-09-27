import pyodbc 
import pandas as pd
import ast
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port


###################### DEFINE GREG JSON DATA SKELETON ############################

policyJSON = {
    "policy": {
      "name": "",
      "policyNum": "null",
      "states": "Oregon",
      "lineOfBusiness": "Commercial",
      "policyLineItem": "Owner Operator",
      "coverageTerm": "Annual",
      "policyCategory": "Taxicabs and Limousines",
      "underwritingCode": "New Business",
      "agent": "Quantum Risk Solutions (QRSBRK)",
      "effectiveDate": "null",
      "expirationDate": "null",
      "radius": "Local",
      "classCode": "Non-fleet",
      "businessUseClass": "Service",
      "secondaryCategory": "Taxi"
    },
    "insured": {
      "agent": "Quantum Risk Solutions (QRSBRK)",
      "entity": "Individual",
      "firstName": "null",
      "lastName": "null",
      "middleName": "null",
      "dob": "null",
      "suffix": "null",
      "gender": "Male",
      "ssn": "null",
      "address1": "null",
      "address2": "null",
      "city": "null",
      "state": "Oregon",
      "zipCode": "null",
      "email": "null",
      "phoneNumber": "null",
      "licenseState": "Oregon",
      "licenseNumber": "null",
      "licenseEff": "null",
      "licenseExp": "null",
      "contactName": "null",
      "contactNumber": "null",
      "contactEmail": "null",
      "corporationName": "null",
      "taxIdNumber": "null"
    },
    "drivers": {
      "values": [
        {
          "driverFirstName": "null",
          "driverMiddleName": "null",
          "driverLastName": "null",
          "states": "Oregon",
          "licenseNumber": "null",
          "licenseEffDate": "null",
          "licenseExpDate": "null",
          "driverEffDate": "null",
          "driverExpDate": "null",
          "driverBirthDate": "null"
        }
      ]
    },
    "lossHistory": {
      "incidents": [
        {
          "accidentDate": "null",
          "reportedDate": "null",
          "claimNumber": "null",
          "claimType": "Body Injury",
          "subClaimNumber": "null",
          "totalIncurred": "null",
          "liabilityPaid": "null",
          "openReserve": "null",
          "status": "Yes",
          "previousPolicyNumber": "null",
          "priorCarrierName": "null",
          "originalInceptionDate": "null",
          "expirationDate": "null",
          "isExperienceMode": "Yes",
          "isPolicyTransferred": "Yes"
        }
      ]
    },
    "documents": {},
    "vehicles": {
      "values": [
        {
          "yesNo": "No",
          "category": "Taxicabs and Limousines",
          "classification": "null",
          "vehicleCategory": "Taxicab - Owner-Driver",
          "vehicleType": "Car Service",
          "state": "Oregon",
          "vehicleState": "null",
          "vehicleWeight": "0 - 10,000",
          "fuelType": "Gas",
          "fleet": "Yes",
          "vin": "null",
          "make": "null",
          "model": "null",
          "modelYear": "null",
          "seating": "null",
          "wheelChair": "Yes",
          "plateNumber": "null",
          "garageZipCode": "null",
          "zoneCode": "null",
          "rateClassCode": "null",
          "baseName": "null",
          "baseType": "Black Car",
          "baseNumber": "null",
          "baseEffDate": "null",
          "baseExpDate": "null",
          "shl": "null",
          "garageAddress1": "null",
          "garageAddress2": "null",
          "garageZipCode2": "null",
          "garageCity": "null",
          "garageCounty": "null",
          "garageState": "Oregon",
          "garageCountry": "null",
        "overallPremium": "",
        "personalInjuryProtectionPremium": "",
        "pedPipProtectionPremium": "",
        "medicalPaymentsPremium": "",
        "underinsuredMotoristPremium": "",
        "uninsuredMotoristPremium": "",
        "hiredCSLPremium": "",
        "nonOwnedCSLPremium": ""
        }
      ]
    },
    "payments": {
      "paymentType": "100% DEPOSIT"
    },
    "reinsurance": {
      "reinsuranceType": "General Reinsurance",
      "resInsAmmout": ""
    },
    "coverage": {
      "overall": "Combined Single Limit",
      "deductable": "null",
      "deductableAmount": "null",
      "deductableAutoEntry": "null",
      "combinedSectionLimit": "0",
      "combinedSectionEntry": "Excluded",
      "splitSectionBodyPerPerson": "0",
      "splitSectionBodyPerAccidentOptions": "0",
      "splitSectionPropertyDamageOptions": "0",
      "splitSectionAutoEntryOptions": "Excluded",
      "pIProtectionSingleLimit": "35,000",
      "pIProtectionSingleEntry": "Excluded",
      "pIProtectionSplitBodyPerPerson": "25,000",
      "pIProtectionSplitBodyPerAccident": "25,000",
      "pIProtectionSplitPropertyDamage": "10,000",
      "pIProtectionSplitAutoEntry": "Excluded",
      "pedPipSingleLimit": "No",
      "medicalSingleLimit": "35,000",
      "medicalSingleEntry": "Excluded",
      "medicalSplitBodyPerPerson": "25,000",
      "medicalSplitBodyPerAccident": "25,000",
      "medicalSplitPropertyDamage": "10,000",
      "medicalSplitAutoEntry": "Excluded",
      "underinsuredMotoristSingleLimit": "35,000",
      "underinsuredMotoristSingleAutoEntry": "Excluded",
      "underMotoristBodyPerPerson": "25,000",
      "underMotoristBodyPerAccident": "25,000",
      "underMotoristProperty": "10,000",
      "underMotoristAuto": "Excluded",
      "cslSingleLimit": "35,000",
      "cslBodyPerAccident": "25,000",
      "cslBodyPerPerson": "25,000",
      "cslSingleAuto": "Excluded",
      "cslProperty": "10,000",
      "cslSplitAuto": "Excluded",
      "nonCslBodyPerAccident": "25,000",
      "nonCslBodyPerPerson": "25,000",
      "nonCslProperty": "10,000",
      "nonCslSingleAuto": "Excluded",
      "nonCslSingleLimit": "35,000",
      "nonCslSplitAuto": "Excluded",
      "unMotoristAuto": "Excluded",
      "unMotoristBodyPerAccident": "25,000",
      "unMotoristBodyPerPerson": "25,000",
      "unMotoristProperty": "10,000",
      "uninsuredMotoristSingleAutoEntry": "Excluded",
      "uninsuredMotoristSingleLimit": "35,000",
      "personalInjury": "Combined Single Limit",
      "medicalPayments": "Combined Single Limit",
      "underinsuredMotorist": "Combined Single Limit",
      "uninsuredMotorist": "Combined Single Limit",
      "csl": "Yes",
      "nonOwnedCSL": "Yes",
      "overallPremium": "",
      "personalInjuryProtectionPremium": "",
      "pedPipProtectionPremium": "",
      "medicalPaymentsPremium": "",
      "underinsuredMotoristPremium": "",
      "uninsuredMotoristPremium": "",
      "hiredCSLPremium": "",
      "nonOwnedCSLPremium": ""
    },
    "renewal": {
          "renewalDecision": "undecided",
          "nonRenewalReason": "undecided",
          "dateOfDecision": "null"
    },
    "underwriting": {
          "creditsDebits": "",
          "remarks": ""
    },
    "cancellation": {
          "cancellationReason": "null",
          "isCancelled": "No",
          "cancellationDate": ""
    }
    
}


stateCodes = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AS": "American Samoa",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District Of Columbia",
    "FM": "Federated States Of Micronesia",
    "FL": "Florida",
    "GA": "Georgia",
    "GU": "Guam",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MH": "Marshall Islands",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "MP": "Northern Mariana Islands",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PW": "Palau",
    "PA": "Pennsylvania",
    "PR": "Puerto Rico",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VI": "Virgin Islands",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    '': "None"
    }

greg = {
    "policy": {
      "name": "",
      "policyNum": "null",
      "states": "Oregon",
      "lineOfBusiness": "Commercial",
      "policyLineItem": "Owner Operator",
      "coverageTerm": "Annual",
      "policyCategory": "Taxicabs and Limousines",
      "underwritingCode": "New Business",
      "agent": "Quantum Risk Solutions (QRSBRK)",
      "effectiveDate": "null",
      "expirationDate": "null",
      "radius": "Local",
      "classCode": "Non-fleet",
      "businessUseClass": "Service",
      "sizeClass": "Light Trucks "
    },
    "insured": {
      "agent": "Quantum Risk Solutions (QRSBRK)",
      "entity": "Individual",
      "firstName": "null",
      "lastName": "null",
      "middleName": "null",
      "dob": "null",
      "suffix": "null",
      "gender": "Male",
      "ssn": "null",
      "address1": "null",
      "address2": "null",
      "city": "null",
      "state": "Oregon",
      "zipCode": "null",
      "email": "null",
      "phoneNumber": "null",
      "licenseState": "Oregon",
      "licenseNumber": "null",
      "licenseEff": "null",
      "licenseExp": "null",
      "contactName": "null",
      "contactNumber": "null",
      "contactEmail": "null",
      "corporationName": "null",
      "taxIdNumber": "null"
    },
    "drivers": {
      "values": [
        {
          "driverFirstName": "null",
          "driverMiddleName": "null",
          "driverLastName": "null",
          "states": "Oregon",
          "licenseNumber": "null",
          "licenseEffDate": "null",
          "licenseExpDate": "null",
          "driverEffDate": "null",
          "driverExpDate": "null",
          "driverBirthDate": "null"
        }
      ]
    },
    "lossHistory": {
      "incidents": [
        {
          "accidentDate": "null",
          "reportedDate": "null",
          "claimNumber": "null",
          "claimType": "Body Injury",
          "subClaimNumber": "null",
          "totalIncurred": "null",
          "liabilityPaid": "null",
          "openReserve": "null",
          "status": "Yes",
          "previousPolicyNumber": "null",
          "priorCarrierName": "null",
          "originalInceptionDate": "null",
          "expirationDate": "null",
          "isExperienceMode": "Yes",
          "isPolicyTransferred": "Yes"
        }
      ]
    },
    "documents": {},
    "coverage": {
      "overall": "Combined Single Limit",
      "deductable": "null",
      "deductableAmount": "null",
      "deductableAutoEntry": "null",
      "combinedSectionLimit": "35,000",
      "combinedSectionEntry": "Excluded",
      "splitSectionBodyPerPerson": "25,000",
      "splitSectionBodyPerAccidentOptions": "25,000",
      "splitSectionPropertyDamageOptions": "10,000",
      "splitSectionAutoEntryOptions": "Excluded",
      "pIProtectionSingleLimit": "35,000",
      "pIProtectionSingleEntry": "Excluded",
      "pIProtectionSplitBodyPerPerson": "25,000",
      "pIProtectionSplitBodyPerAccident": "25,000",
      "pIProtectionSplitPropertyDamage": "10,000",
      "pIProtectionSplitAutoEntry": "Excluded",
      "medicalSingleLimit": "35,000",
      "medicalSingleEntry": "Excluded",
      "medicalSplitBodyPerPerson": "25,000",
      "medicalSplitBodyPerAccident": "25,000",
      "medicalSplitPropertyDamage": "10,000",
      "medicalSplitAutoEntry": "Excluded",
      "underinsuredMotoristSingleLimit": "35,000",
      "underinsuredMotoristSingleAutoEntry": "Excluded",
      "underMotoristBodyPerPerson": "25,000",
      "underMotoristBodyPerAccident": "25,000",
      "underMotoristProperty": "10,000",
      "underMotoristAuto": "Excluded",
      "cslSingleLimit": "35,000",
      "cslBodyPerAccident": "25,000",
      "cslBodyPerPerson": "25,000",
      "cslSingleAuto": "Excluded",
      "cslProperty": "10,000",
      "cslSplitAuto": "Excluded",
      "nonCslBodyPerAccident": "25,000",
      "nonCslBodyPerPerson": "25,000",
      "nonCslProperty": "10,000",
      "nonCslSingleAuto": "Excluded",
      "nonCslSingleLimit": "35,000",
      "nonCslSplitAuto": "Excluded",
      "unMotoristAuto": "Excluded",
      "unMotoristBodyPerAccident": "25,000",
      "unMotoristBodyPerPerson": "25,000",
      "unMotoristProperty": "10,000",
      "uninsuredMotoristSingleAutoEntry": "Excluded",
      "uninsuredMotoristSingleLimit": "35,000",
      "personalInjury": "Combined Single Limit",
      "medicalPayments": "Combined Single Limit",
      "underinsuredMotorist": "Combined Single Limit",
      "uninsuredMotorist": "Combined Single Limit",
      "csl": "Yes",
      "nonOwnedCSL": "Yes",
      "overallPremium": "",
      "personalInjuryProtectionPremium": "",
      "pedPipProtectionPremium": "",
      "medicalPaymentsPremium": "",
      "underinsuredMotoristPremium": "",
      "uninsuredMotoristPremium": "",
      "hiredCSLPremium": "",
      "nonOwnedCSLPremium": ""
    },
    "vehicles": {
      "values": [
        {
          "yesNo": "No",
          "category": "Taxicabs and Limousines",
          "classification": "null",
          "vehicleCategory": "Taxicab - Owner-Driver",
          "vehicleType": "Car Service",
          "state": "Oregon",
          "vehicleState": "null",
          "vehicleWeight": "0 - 10,000",
          "fuelType": "Gas",
          "fleet": "Yes",
          "vin": "null",
          "make": "null",
          "model": "null",
          "modelYear": "null",
          "seating": "null",
          "wheelChair": "Yes",
          "plateNumber": "null",
          "garageZipCode": "null",
          "zoneCode": "null",
          "rateClassCode": "null",
          "baseName": "null",
          "baseType": "Black Car",
          "baseNumber": "null",
          "baseEffDate": "null",
          "baseExpDate": "null",
          "shl": "null",
          "garageAddress1": "null",
          "garageAddress2": "null",
          "garageZipCode2": "null",
          "garageCity": "null",
          "garageCounty": "null",
          "garageState": "Oregon",
          "garageCountry": "null",
        "overallPremium": "",
        "personalInjuryProtectionPremium": "",
        "pedPipProtectionPremium": "",
        "medicalPaymentsPremium": "",
        "underinsuredMotoristPremium": "",
        "uninsuredMotoristPremium": "",
        "hiredCSLPremium": "",
        "nonOwnedCSLPremium": ""
        }
      ]
    },
    "payments": {
      "payment": "100% DEPOSIT"
    },
    "reinsurance": {
      "reinsuranceType": "Price Forbes",
      "resInsAmmout": ""
    }
  }


server = '38.129.107.12' 
database = 'GREENMILE_POLICY_ADMIN' 
databaseTwo = 'POLICY_ADMIN'
databaseThree = 'ECMS'
username = 'elpisadmin' 
password = 'elpis!@#' 

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

cnxnTwo = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+databaseTwo+';UID='+username+';PWD='+ password)

cnxnThree = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+databaseThree+';UID='+username+';PWD='+ password)
# cursor = cnxn.cursor()

# #Sample select query
# cursor.execute("SELECT * FROM dbo.INSURED_COVERAGE;") 

# for key in greg:
#     for i in greg[key]:
#         if key != "vehicles":
#             print(key,i)
#         else:
#             print(greg[key][i][0])







sql_policy = "SELECT * FROM dbo.CS_POLICY_INSURED;"
sql_policy_ref = "SELECT * FROM dbo.POLICY;"
sql_drivers = "SELECT * FROM dbo.CS_DRIVER_DETAIL;"
sql_loss_history = "SELECT * FROM POL.POL_LOSSHISTORY;"
sql_vehicles = "SELECT * FROM dbo.CS_VEHICLE_DETAIL;"
sql_coverage = "SELECT * FROM dbo.INSURED_COVERAGE"

sql_endorsements = "SELECT * FROM POL.TRANS_ENDORSEMENTS"
sql_endorsements_drivers = "SELECT * FROM POL.TRANS_DRIVER_ACTIVITY"
sql_endorsements_contacts = "SELECT * FROM dbo.ECMS_ContactPerson"
sql_endorsements_contacts_name = "SELECT * FROM dbo.ECMS_ContactPersonName"

sql_endorsements_vehicles = "SELECT * FROM POL.TRANS_VEHICLE_ACTIVITY"
sql_endorsements_vehicles_data = "SELECT * FROM POL.POL_VEHICLE"


Data_policy = pd.read_sql(sql_policy, cnxn)
Data_policy_ref = pd.read_sql(sql_policy_ref, cnxn)
Data_drivers = pd.read_sql(sql_drivers, cnxn)
Data_loss_history = pd.read_sql(sql_loss_history, cnxnTwo)
Data_vehicles = pd.read_sql(sql_vehicles, cnxn)
Data_coverage = pd.read_sql(sql_coverage, cnxn)

Data_endorsements = pd.read_sql(sql_endorsements, cnxnTwo)
Data_endorsements_drivers = pd.read_sql(sql_endorsements_drivers, cnxnTwo)
Data_endorsements_contacts = pd.read_sql(sql_endorsements_contacts, cnxnThree)
Data_endorsements_contacts_name = pd.read_sql(sql_endorsements_contacts_name, cnxnThree)

Data_endorsements_vehicles = pd.read_sql(sql_endorsements_vehicles, cnxnTwo)
Data_endorsements_vehicles_data = pd.read_sql(sql_endorsements_vehicles_data, cnxnTwo)


Data_endorsements.to_csv("endorsements.csv")
Data_endorsements_drivers.to_csv("endorsements_drivers.csv")
Data_endorsements_contacts.to_csv("endorsements_contacts.csv")
Data_endorsements_contacts_name.to_csv("endorsements_contacts_name.csv")
Data_endorsements_vehicles.to_csv("endorsements_vehicles.csv")
Data_endorsements_vehicles_data.to_csv("endorsements_vehicles_data.csv")
#Data_drivers.to_csv("drivers_map.csv")
Data_vehicles.to_csv("vehicles_map.csv")

total_policies=[]
Data_coverage.to_csv("coverage.csv")
Data_policy.to_csv("policy.csv")

reconcile = pd.read_excel("PolicyInfoReport.xlsx")

reconcile_dup = pd.read_excel("kushreport2.xlsx")

reconcile_drivers = pd.read_excel("Driver_report.xlsx")


Data_drivers["FullName"] = Data_drivers["DRVFIRSTNAME"] + Data_drivers["DRVLASTNAME"]
reconcile_drivers["FullName"] = reconcile_drivers["DRVFIRSTNAME"] + reconcile_drivers["DRVLASTNAME"]

reconcile_drivers_two = reconcile_drivers[["FullName", "DRIVER DOB"]].copy()

Data_drivers = Data_drivers.merge(reconcile_drivers_two, on="FullName")

Data_drivers.to_csv("drivers_map.csv")

vehicle_policyInfo_mapping = {
      'overallPremium': 'LIAB_PREM',
      'personalInjuryProtectionPremium': 'PIP_PREM',
      'pedPipProtectionPremium': 'PEDPIP_PREM',
      'underinsuredMotoristPremium': 'UIM__PREM',
      'uninsuredMotoristPremium': 'UM__PREM',
      'medicalPaymentsPremium': 'MED_PREM'
      
      
}
premiums_vehicle = ['overallPremium', 'personalInjuryProtectionPremium', 'medicalPaymentsPremium', 'underinsuredMotoristPremium', 'uninsuredMotoristPremium', 'pedPipProtectionPremium']

my_list = [    '19AZT00014',    '19AZT00012',    '19AZT00005',    '19AZT00015',    '19TXT00023',    '19AZT00026',    '19AZT00025',    '19AZT00028',    '19CAT00029',    '19CAT00031',    '19CAT00032',    '19CAT00036',    '19TXT00039',    '19VAT00040',    '19AZT00046',    '19TXT00050',    '19VAT00053',    'P19ALT0004',    '19AZT00009',    '20PAT00067',    '20PAT00069',    '20CAL00077']

    
for i, row in Data_policy.iterrows():
      
    j=0
    k=0
    m=0
    n=0
    
    policy = policyJSON
    policyNumber = row["INSPOLICYNO"].strip()
    

    
    
    agent_mapping = {
      'QRSBRK': 'Quantum Risk Solutions (QRSBRK)',
      'PRABRK': 'Preferred Risk Associates (PRABRK)',
      'ABIBRK': 'American Business Insurance (ABIBRK)',
      'TIPSBRK': 'Transportation Insurance Placement Services (TIPSBRK)',
      'BRBRK': 'Big Rigs (BRBRK)',
      'CLUTBRK': 'Cluett Insurance Agency (CLUETT)',
      'CISBRK': 'Cornell Insurance Agency (CORN)',
      'LAGBRK': 'Laguna Pacific Insurance Services (LPIS)',
      'BROKER': 'None'
      }

    
    drivers = []
    
    
    
    vehicles = []
    
    
    
    policy["policy"]["name"] = row["INSFULLNAME"].strip()
    
    policy["policy"]["states"] = stateCodes[row["INSSTATE"].strip()]
    policy["policy"]["lineOfBusiness"] = "Commercial"
    policy["policy"]["policyLineItem"] = "Owner Operator"
    policy["policy"]["coverageTerm"] = "Annual"
    if 'T' in row["INSPOLICYNO"].strip():
      policy["policy"]["policyCategory"] = "Taxicabs and Limousines"
      policy["policy"]["classification"] = "Taxicab - All Other"
      policy["policy"]["secondaryCategory"] = "Taxi"
    elif 'L' in row["INSPOLICYNO"].strip():
      policy["policy"]["policyCategory"] = "Taxicabs and Limousines"
      policy["policy"]["classification"] = "Limousine - Seating 8 or Fewer"
      policy["policy"]["secondaryCategory"] = "Limo"
    elif 'N' in row["INSPOLICYNO"].strip():
      policy["policy"]["policyCategory"] = "Other Buses"
      policy["policy"]["classification"] = "Paratransit"
      policy["policy"]["secondaryCategory"] = "Paratransit"
    
    
    
    policyNumberDigits = row["INSPOLICYNO"].strip()[-3:]
    for a, rowTwo in Data_policy.iterrows():
      oldPolicyYearUnfixed = rowTwo["INSPOLICYNO"].strip()
      policyYearUnfixed = row["INSPOLICYNO"].strip()
      
      if 'P' in oldPolicyYearUnfixed[0]:
            oldPolicyYear = rowTwo["INSPOLICYNO"].strip()[1:3]
      else:
            oldPolicyYear = rowTwo["INSPOLICYNO"].strip()[:2]
            
      if 'P' in policyYearUnfixed[0]:
            policy["policy"]["policyNum"] = row["INSPOLICYNO"].strip()[1:]
            policyYear = row["INSPOLICYNO"].strip()[1:3]
      else:
            policy["policy"]["policyNum"] = row["INSPOLICYNO"].strip()
            policyYear = row["INSPOLICYNO"].strip()[:2]
            
      if policyNumberDigits in rowTwo["INSPOLICYNO"].strip() and int(policyYear) > int(oldPolicyYear):
            policy["policy"]["underwritingCode"] = "Renewal"
      else:
            policy["policy"]["underwritingCode"] = "New Business"
    
    
    policy["policy"]["agent"] = agent_mapping[row["INSBRKID"].strip()]
    policy["policy"]["effectiveDate"] = row["INSPOLICYEFFDT"].strftime('%m/%d/%Y')
    policy["policy"]["expirationDate"] = row["INSPOLICYEXPDT"].strftime('%m/%d/%Y')
    policy["insured"]["agent"] = agent_mapping[row["INSBRKID"].strip()]
    policy["insured"]["entity"] = row["INSTYPE"].strip()
    policy["insured"]["firstName"] = row["INSFIRSTNAME"].strip()
    policy["insured"]["lastName"] = row["INSLASTNAME"].strip()
    policy["insured"]["middleName"] = row["INSMIDDLENAME"].strip()
    policy["insured"]["dob"] = row["INSDOB"].strftime('%m/%d/%Y')
    policy["insured"]["address1"] = row["INSADDRESS1"].strip()
    policy["insured"]["address2"] = row["INSADDRESS2"].strip()
    policy["insured"]["city"] = row["INSCITY"].strip()
    policy["insured"]["state"] = stateCodes[row["INSSTATE"].strip()]
    policy["insured"]["zipCode"] = row["INSZIP"].strip()
    policy["insured"]["email"] = row["INSEMAILID"].strip()
    policy["insured"]["phoneNumber"] = row["INSOTHPHONE"].strip()
    policy["insured"]["taxIdNumber"] = row["INSTAXID"].strip()
    
    
    driversByPolicy = Data_drivers[Data_drivers["DRVPOLICYNO"]==policyNumber]
    
    
    for j, driverRow in driversByPolicy.iterrows():
        duplicateDrivers = False
        if len(driverRow["DRVFIRSTNAME"].strip()+" "+driverRow["DRVLASTNAME"].strip()) != 1:

          driversJSON = {
            "driverFirstName": "null",
            "driverMiddleName": "null",
            "driverLastName": "null",
            "states": "Oregon",
            "licenseNumber": "null",
            "licenseEffDate": "null",
            "licenseExpDate": "null",
            "driverEffDate": "null",
            "driverExpDate": "null",
            "driverBirthDate": "null"
          }
          for p in drivers:
            p = ast.literal_eval(p)
            if p["driverFirstName"] == driverRow["DRVFIRSTNAME"].strip() and p["driverLastName"] == driverRow["DRVLASTNAME"].strip():
              duplicateDrivers = True

          if duplicateDrivers == False:
            driversJSON["driverFirstName"] = driverRow["DRVFIRSTNAME"].strip()
            driversJSON["driverMiddleName"] = driverRow["DRIVERMIDDLENAME"].strip()      
            driversJSON["driverLastName"] = driverRow["DRVLASTNAME"].strip()
            driversJSON["states"] = driverRow["DRVLICSTATE"].strip()
            driversJSON["licenseNumber"] = driverRow["DRVLICENSENO"].strip()
            driversJSON["licenseEffDate"] = driverRow["DRVLICEFFDATE"].strftime('%m/%d/%Y')
            driversJSON["licenseExpDate"] = driverRow["DRVLICEXPDATE"].strftime('%m/%d/%Y')
            driversJSON["driverEffDate"] = driverRow["DRVEFFDATE"].strftime('%m/%d/%Y')
            driversJSON["driverExpDate"] = driverRow["DRVEXPDATE"].strftime('%m/%d/%Y')
            # driversJSON["driverNumber"] = int(driverRow["DRVVERID"])
            if type(driverRow['DRIVER DOB']) != float and type(driverRow['DRIVER DOB']) != str:
                              
                  driversJSON['driverBirthDate'] = driverRow['DRIVER DOB'].strftime("%m/%d/%Y")
            else:
                  driversJSON['driverBirthDate'] = "01/01/1900"
            
            
            # for m, driverRowTwo in reconcile_drivers.iterrows():
            #       if driverRowTwo["DRVFIRSTNAME"].strip() == driverRow["DRVFIRSTNAME"].strip() and driverRowTwo["DRVLASTNAME"].strip() == driverRow["DRVLASTNAME"].strip():
            #             print(driverRowTwo['DRIVER DOB'])
            #             if type(driverRowTwo['DRIVER DOB']) != float and type(driverRowTwo['DRIVER DOB']) != str:
                              
            #                   driversJSON['driverBirthDate'] = driverRowTwo['DRIVER DOB'].strftime("%m/%d/%Y")
            #             else:
            #                   driversJSON['driverBirthDate'] = "01/01/1900"
            drivers.append(str(driversJSON))

          
    
    policy["drivers"]["values"] = drivers
    
    
    
    vehiclesByPolicy = Data_vehicles[Data_vehicles["VEHPOLICYNO"]==policyNumber]
    
    
    for k, vehicleRow in vehiclesByPolicy.iterrows():
        duplicateVehicles = False


        vehiclesJSON = {
        ######## NOT INPUTTED YET #########
          "yesNo": "No",
          "category": "Taxicabs and Limousines",
          "classification": "null",
          "vehicleCategory": "Taxicab - Owner-Driver",
          "vehicleType": "Car Service",
        ###################################
          "state": "Oregon",
          "vehicleState": "null",
          ######## NOT INPUTTED YET #########
          "vehicleWeight": "0 - 10,000",
          "fuelType": "Gas",
          "fleet": "Yes",
          #################################
          "vin": "null",
          "make": "null",
          "model": "null",
          "modelYear": "null",
          "seating": "null",
          ######## NOT INPUTTED YET #########
          "wheelChair": "Yes",
          "plateNumber": "null",
          "garageZipCode": "null",
          "zoneCode": "null",
          "rateClassCode": "null",
          "baseName": "null",
          "baseType": "Black Car",
          "baseNumber": "null",
          "baseEffDate": "null",
          "baseExpDate": "null",
          "shl": "null",
          "garageAddress1": "null",
          "garageAddress2": "null",
          "garageZipCode2": "null",
          "garageCity": "null",
          "garageCounty": "null",
          "garageState": "Oregon",
          "garageCountry": "null",
          #################################
        "overallPremium": "",
        "personalInjuryProtectionPremium": "",
        "pedPipProtectionPremium": "",
        "medicalPaymentsPremium": "",
        "underinsuredMotoristPremium": "",
        "uninsuredMotoristPremium": "",
        "hiredCSLPremium": "",
        "nonOwnedCSLPremium": ""
        }
        
        for p in vehicles:
              p = ast.literal_eval(p)
              if p['vin'] == vehicleRow["VEHVIN"].strip():
                    if vehicleRow["VEHEXPDATE"].strftime('%m/%d/%Y') == p['baseExpDate']:
  
                      duplicateVehicles=True

        if duplicateVehicles == False:
          vehiclesJSON["state"] = stateCodes[vehicleRow["VEHREGSTATE"].strip()]
          vehiclesJSON["vehicleState"] = stateCodes[vehicleRow["VEHREGSTATE"].strip()]
          vehiclesJSON["vin"] = vehicleRow["VEHVIN"].strip()
          vehiclesJSON["make"] = vehicleRow["VEHMAKE"].strip()
          vehiclesJSON["model"] = vehicleRow["VEHMODEL"].strip()
          vehiclesJSON["modelYear"] = vehicleRow["VEHYEAR"].strip()
          vehiclesJSON["seating"] = str(vehicleRow["VEHSEATS"])
          vehiclesJSON["baseEffDate"] = vehicleRow["VEHEFFDATE"].strftime('%m/%d/%Y')
          vehiclesJSON["baseExpDate"] = vehicleRow["VEHEXPDATE"].strftime('%m/%d/%Y')
          
          coverageByVehicle = Data_coverage[Data_coverage["VEHICLE_ID"]==vehicleRow["VEHVEHICLEID"]]
          coverageByVehicle = coverageByVehicle[coverageByVehicle["COVERAGE_ANNUAL_PREMIUM"] > 0]
          coverageByVehicle.to_csv("test.csv")
          for m, coverageRow in coverageByVehicle.iterrows():
                
                
                
                
                
              
              if coverageRow["COVERAGE_CODE"]=="CSL" and coverageRow["COVERAGE_TERM_PREMIUM"] != 0:  
                  
                  vehiclesJSON["overallPremium"] = coverageRow["COVERAGE_TERM_PREMIUM"]
                  
              if coverageRow["COVERAGE_CODE"]=="BI" and coverageRow["COVERAGE_TERM_PREMIUM"] != 0:
                    
                  vehiclesJSON["overallPremium"] = coverageRow["COVERAGE_TERM_PREMIUM"]
                  
              if coverageRow["COVERAGE_CODE"]=="PIP" and coverageRow["COVERAGE_TERM_PREMIUM"] != 0:
                  vehiclesJSON["personalInjuryProtectionPremium"] = coverageRow["COVERAGE_TERM_PREMIUM"]
              
              else:
                  vehiclesJSON["personalInjuryProtectionPremium"] = 0
              
              if coverageRow["COVERAGE_CODE"]=="MED" and coverageRow["COVERAGE_TERM_PREMIUM"] != 0:
                  vehiclesJSON["medicalPaymentsPremium"] = coverageRow["COVERAGE_TERM_PREMIUM"]
              else:
                  vehiclesJSON["medicalPaymentsPremium"] = 0
                  
              
              if coverageRow["COVERAGE_CODE"]=="UIMCSL" and coverageRow["COVERAGE_TERM_PREMIUM"] != 0:
                  vehiclesJSON["underinsuredMotoristPremium"] = coverageRow["COVERAGE_TERM_PREMIUM"]
              
              if coverageRow["COVERAGE_CODE"]=="UIMBI" or coverageRow["COVERAGE_CODE"]=="UIM" and coverageRow["COVERAGE_TERM_PREMIUM"] != 0:
                  vehiclesJSON["underinsuredMotoristPremium"] = coverageRow["COVERAGE_TERM_PREMIUM"]
                  
              
              if coverageRow["COVERAGE_CODE"]=="UMCSL" and coverageRow["COVERAGE_TERM_PREMIUM"] != 0:
                  vehiclesJSON["uninsuredMotoristPremium"] = coverageRow["COVERAGE_TERM_PREMIUM"]
              
              if coverageRow["COVERAGE_CODE"]=="UMBI" or coverageRow["COVERAGE_CODE"]=="UM" and coverageRow["COVERAGE_TERM_PREMIUM"] != 0:
                  vehiclesJSON["uninsuredMotoristPremium"] = coverageRow["COVERAGE_TERM_PREMIUM"]
              if coverageRow["COVERAGE_CODE"]=="PEDPIP" and coverageRow["COVERAGE_TERM_PREMIUM"] != 0:
                  vehiclesJSON["pedPipProtectionPremium"] = coverageRow["COVERAGE_TERM_PREMIUM"]
                  
                  
                  
                  
                  
                  
                  
              if coverageRow["COVERAGE_CODE"]=="CSL" and coverageRow["COVERAGE_ANNUAL_PREMIUM"] != 0:  
                  
                  policy['coverage']["overallPremium"] = coverageRow["COVERAGE_ANNUAL_PREMIUM"]
                  
              if coverageRow["COVERAGE_CODE"]=="BI" and coverageRow["COVERAGE_ANNUAL_PREMIUM"] != 0:
                    
                  policy['coverage']["overallPremium"] = coverageRow["COVERAGE_ANNUAL_PREMIUM"]
                  
              if coverageRow["COVERAGE_CODE"]=="PIP" and coverageRow["COVERAGE_ANNUAL_PREMIUM"] != 0:
                  policy['coverage']["personalInjuryProtectionPremium"] = coverageRow["COVERAGE_ANNUAL_PREMIUM"]
              
              else:
                  policy['coverage']["personalInjuryProtectionPremium"] = 0
              
              if coverageRow["COVERAGE_CODE"]=="MED" and coverageRow["COVERAGE_ANNUAL_PREMIUM"] != 0:
                  policy['coverage']["medicalPaymentsPremium"] = coverageRow["COVERAGE_ANNUAL_PREMIUM"]
              else:
                  policy['coverage']["medicalPaymentsPremium"] = 0
                  
              
              if coverageRow["COVERAGE_CODE"]=="UIMCSL" and coverageRow["COVERAGE_ANNUAL_PREMIUM"] != 0:
                  policy['coverage']["underinsuredMotoristPremium"] = coverageRow["COVERAGE_ANNUAL_PREMIUM"]
              
              if coverageRow["COVERAGE_CODE"]=="UIMBI" or coverageRow["COVERAGE_CODE"]=="UIM" and coverageRow["COVERAGE_ANNUAL_PREMIUM"] != 0:
                  policy['coverage']["underinsuredMotoristPremium"] = coverageRow["COVERAGE_ANNUAL_PREMIUM"]
                  
              
              if coverageRow["COVERAGE_CODE"]=="UMCSL" and coverageRow["COVERAGE_ANNUAL_PREMIUM"] != 0:
                  policy['coverage']["uninsuredMotoristPremium"] = coverageRow["COVERAGE_ANNUAL_PREMIUM"]
              
              if coverageRow["COVERAGE_CODE"]=="UMBI" or coverageRow["COVERAGE_CODE"]=="UM" and coverageRow["COVERAGE_ANNUAL_PREMIUM"] != 0:
                  policy['coverage']["uninsuredMotoristPremium"] = coverageRow["COVERAGE_ANNUAL_PREMIUM"]
              if coverageRow['COVERAGE_CODE']=='PEDPIP' and coverageRow['COVERAGE_ANNUAL_PREMIUM'] != 0:
                  policy['coverage']["pedPipProtectionPremium"] = coverageRow["COVERAGE_ANNUAL_PREMIUM"]
                  
                  
                  
                  
                  
                  
                  
          vehicles.append(str(vehiclesJSON))
        matches = ["N/A/N/A", "N/A", "Opt Out", "Opt Out/Opt Out", "No DED"]
        for n, coverageRow in coverageByVehicle.iterrows():
            
            if coverageRow["COVERAGE_CODE"]=="CSL":
                
                policy["coverage"]["csl"] = "Yes"
                #policy["coverage"]["combinedSectionLimit"] = 0 if coverageRow["LIMIT_CODE1"] is None or "N/A/N/A" or "N/A" or "Opt Out" or "No DED" else coverageRow["LIMIT_CODE1"].strip("- No DED")*1000 if "No DED" in coverageRow["LIMIT_CODE1"] else coverageRow["LIMIT_CODE1"]*1000
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["combinedSectionLimit"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["combinedSectionLimit"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      policy["coverage"]["combinedSectionLimit"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED"))
                      policy['coverage']['combinedSectionEntry'] = '17'
                else:
                      policy["coverage"]["combinedSectionLimit"] = float(coverageRow["LIMIT_CODE1"])*1000
                      policy['coverage']['combinedSectionEntry'] = '17'
                      
                policy["coverage"]["splitSectionBodyPerPerson"] = 0
                policy["coverage"]["splitSectionBodyPerAccidentOptions"] = 0
                policy["coverage"]["splitSectionPropertyDamageOptions"] = 0
      
                
                
                
            if coverageRow["COVERAGE_CODE"]=="BI":
                policy["coverage"]["csl"] = "No"
                policy["coverage"]["combinedSectionLimit"] = 0 
                
                
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["splitSectionBodyPerPerson"] = 0
                      policy["coverage"]["splitSectionBodyPerAccidentOptions"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["splitSectionBodyPerPerson"] = 0
                      policy["coverage"]["splitSectionBodyPerAccidentOptions"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      
                      policy["coverage"]["splitSectionBodyPerPerson"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED").split("/")[0])
                      policy["coverage"]["splitSectionBodyPerAccidentOptions"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED").split("/")[1])*1000
                      
                      policy['coverage']['splitSectionAutoEntryOptions'] = "17"
                      policy['coverage']['overall'] = "Split Limit"
     
                else:
                      
                      policy["coverage"]["splitSectionBodyPerPerson"] = float(coverageRow["LIMIT_CODE1"].split("/")[0])*1000
                      policy["coverage"]["splitSectionBodyPerAccidentOptions"] = float(coverageRow["LIMIT_CODE1"].split("/")[1])*1000
                      policy['coverage']['splitSectionAutoEntryOptions'] = "17"
                      policy['coverage']['overall'] = "Split Limit"
                      
            if coverageRow["COVERAGE_CODE"]=="PD":
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["splitSectionPropertyDamageOptions"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["splitSectionPropertyDamageOptions"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      policy["coverage"]["splitSectionPropertyDamageOptions"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED"))
                else:
                      policy["coverage"]["splitSectionPropertyDamageOptions"] = float(coverageRow["LIMIT_CODE1"])*1000
            
            if float(policy['coverage']['combinedSectionLimit']) > 0:
                  policy['coverage']['overall'] = "Combined Single Limit"
                  policy['coverage']['splitSectionAutoEntryOptions'] = "Excluded"
            
            if coverageRow["COVERAGE_CODE"]=="PIP":
                #policy["coverage"]["pIProtectionSingleLimit"] = 0 if coverageRow["LIMIT_CODE1"] is None or "N/A/N/A" or "N/A" or "Opt Out" or "Opt Out/Opt Out" or "No DED" else coverageRow["LIMIT_CODE1"].strip("- No DED")*1000 if "No DED" in coverageRow["LIMIT_CODE1"] else coverageRow["LIMIT_CODE1"]*1000
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["pIProtectionSingleLimit"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["pIProtectionSingleLimit"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      policy["coverage"]["pIProtectionSingleLimit"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED"))*1000
                      policy['coverage']['pIProtectionSingleEntry'] = "17"
                else:
                      policy["coverage"]["pIProtectionSingleLimit"] = float(coverageRow["LIMIT_CODE1"])*1000
                      policy['coverage']['pIProtectionSingleEntry'] = "17"
                
                policy["coverage"]["pIProtectionSplitBodyPerPerson"] = 0
                policy["coverage"]["pIProtectionSplitBodyPerAccident"] = 0
                policy["coverage"]["pIProtectionSplitPropertyDamage"] = 0
                
            else:
                policy["coverage"]["pIProtectionSingleLimit"] = 0
                policy["coverage"]["pIProtectionSplitBodyPerPerson"] = 0
                policy["coverage"]["pIProtectionSplitBodyPerAccident"] = 0
                policy["coverage"]["pIProtectionSplitPropertyDamage"] = 0
               
            
            if coverageRow["COVERAGE_CODE"]=="MED":
                #policy["coverage"]["medicalSingleLimit"] = 0 if coverageRow["LIMIT_CODE1"] is None or "N/A/N/A" or "N/A" or "Opt Out" or "Opt Out/Opt Out" or "No DED" else coverageRow["LIMIT_CODE1"].strip("- No DED") if "No DED" in coverageRow["LIMIT_CODE1"] else coverageRow["LIMIT_CODE1"]
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["medicalSingleLimit"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["medicalSingleLimit"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      policy["coverage"]["medicalSingleLimit"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED"))
                      policy['coverage']['medicalSingleEntry'] = "17"
                else:
                      policy["coverage"]["medicalSingleLimit"] = float(coverageRow["LIMIT_CODE1"])*1000
                      policy['coverage']['medicalSingleEntry'] = "17"
                
                
                policy["coverage"]["medicalSplitBodyPerPerson"] = 0
                policy["coverage"]["medicalSplitBodyPerAccident"] = 0
                policy["coverage"]["medicalSplitPropertyDamage"] = 0
                
            else:
                policy["coverage"]["medicalSingleLimit"] = 0
                policy["coverage"]["medicalSplitBodyPerPerson"] = 0
                policy["coverage"]["medicalSplitBodyPerAccident"] = 0
                policy["coverage"]["medicalSplitPropertyDamage"] = 0
                
                
            
            if coverageRow["COVERAGE_CODE"]=="UIMCSL":
                  
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["underinsuredMotoristSingleLimit"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["underinsuredMotoristSingleLimit"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      policy["coverage"]["underinsuredMotoristSingleLimit"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED"))
                      policy['coverage']['underinsuredMotoristSingleAutoEntry'] = "17"
                else:
                      policy["coverage"]["underinsuredMotoristSingleLimit"] = float(coverageRow["LIMIT_CODE1"])*1000
                      policy['coverage']['underinsuredMotoristSingleAutoEntry'] = "17"
                      
                
                policy["coverage"]["underMotoristBodyPerPerson"] = 0
                policy["coverage"]["underMotoristBodyPerAccident"] = 0
                policy["coverage"]["underMotoristProperty"] = 0
                
            
            if coverageRow["COVERAGE_CODE"]=="UIMBI" or coverageRow["COVERAGE_CODE"]=="UIM":
                policy["coverage"]["underinsuredMotoristSingleLimit"] = 0
                
                
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["underMotoristBodyPerPerson"] = 0
                      policy["coverage"]["underMotoristBodyPerAccident"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["underMotoristBodyPerPerson"] = 0
                      policy["coverage"]["underMotoristBodyPerAccident"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      
                      policy["coverage"]["underMotoristBodyPerPerson"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED").split("/")[0])
                      policy["coverage"]["underMotoristBodyPerAccident"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED").split("/")[1])
                      policy['coverage']['underMotoristAuto'] = "17"
                      policy['coverage']['underinsuredMotorist'] = "Split Limit"
                      
                else:
                      
                      policy["coverage"]["underMotoristBodyPerPerson"] = float(coverageRow["LIMIT_CODE1"].split("/")[0])*1000
                      policy["coverage"]["underMotoristBodyPerAccident"] = float(coverageRow["LIMIT_CODE1"].split("/")[1])*1000
                      policy['coverage']['underMotoristAuto'] = "17"
                      policy['coverage']['underinsuredMotorist'] = "Split Limit"
              
                
            if coverageRow["COVERAGE_CODE"]=="UIMPD":
                  
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["underMotoristProperty"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["underMotoristProperty"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      policy["coverage"]["underMotoristProperty"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED"))
                elif "/" in coverageRow["LIMIT_CODE1"]:
                      policy["coverage"]["underMotoristProperty"] = 25000
                else:
                      policy["coverage"]["underMotoristProperty"] = float(coverageRow["LIMIT_CODE1"])*1000
                
                  
                #policy["coverage"]["underMotoristProperty"] = 0 if coverageRow["LIMIT_CODE1"] is None or "N/A/N/A" or "N/A" or "Opt Out" or "Opt Out/Opt Out" or "No DED" else coverageRow["LIMIT_CODE1"].strip("- No DED")*1000 if "No DED" in coverageRow["LIMIT_CODE1"] else coverageRow["LIMIT_CODE1"].split("/")[0]*1000

            if coverageRow["COVERAGE_CODE"]=="UMCSL":
                  
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["uninsuredMotoristSingleLimit"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["uninsuredMotoristSingleLimit"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      policy["coverage"]["uninsuredMotoristSingleLimit"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED"))*1000
                      policy['coverage']['uninsuredMotoristSingleAutoEntry'] = "17"
                else:
                      policy["coverage"]["uninsuredMotoristSingleLimit"] = float(coverageRow["LIMIT_CODE1"])*1000
                      policy['coverage']['uninsuredMotoristSingleAutoEntry'] = "17"

                        
                policy["coverage"]["unMotoristBodyPerPerson"] = 0
                policy["coverage"]["unMotoristBodyPerAccident"] = 0
                policy["coverage"]["unMotoristProperty"] = 0
                
            
            if coverageRow["COVERAGE_CODE"]=="UMBI" or coverageRow["COVERAGE_CODE"]=="UM":
                policy["coverage"]["uninsuredMotoristSingleLimit"] = 0
                
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["unMotoristBodyPerPerson"] = 0
                      policy["coverage"]["unMotoristBodyPerAccident"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["unMotoristBodyPerPerson"] = 0
                      policy["coverage"]["unMotoristBodyPerAccident"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      
                      policy["coverage"]["unMotoristBodyPerPerson"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED").split("/")[0])*1000
                      policy["coverage"]["unMotoristBodyPerAccident"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED").split("/")[1])*1000
                      policy['coverage']['unMotoristAuto'] = "17"
                      policy['coverage']['uninsuredMotorist'] = "Split Limit"
                      
                      
                else:
                      
                      policy["coverage"]["unMotoristBodyPerPerson"] = float(coverageRow["LIMIT_CODE1"].split("/")[0])*1000
                      policy["coverage"]["unMotoristBodyPerAccident"] = float(coverageRow["LIMIT_CODE1"].split("/")[1])*1000
                      policy['coverage']['unMotoristAuto'] = "17"
                      policy['coverage']['uninsuredMotorist'] = "Split Limit"
            
      
                      
                      
                
            if coverageRow["COVERAGE_CODE"]=="UMPD":
                  
                if (coverageRow["LIMIT_CODE1"] == None):
                      policy["coverage"]["unMotoristProperty"] = 0
                elif any(x in coverageRow["LIMIT_CODE1"] for x in matches):
                      policy["coverage"]["unMotoristProperty"] = 0
                elif "No DED" in coverageRow["LIMIT_CODE1"]:
                      policy["coverage"]["unMotoristProperty"] = float(coverageRow["LIMIT_CODE1"].strip("- No DED"))*1000
                elif "/" in coverageRow["LIMIT_CODE1"]:
                      policy["coverage"]["unMotoristProperty"] = 25000
                else:
                      policy["coverage"]["unMotoristProperty"] = float(coverageRow["LIMIT_CODE1"])*1000
            if coverageRow['COVERAGE_CODE'] == "PEDPIP":
                  policy['coverage']['pedPipSingleLimit'] = "Yes"
                    
    
    policy["vehicles"]["values"] = vehicles  
    
    

    reconcile_two = reconcile[(reconcile['Policy__'].str.strip())==(policy['policy']['policyNum'].strip())].reset_index()


    if len(reconcile_two) > 0:
      if float(str(reconcile_two['CSL'][0]).replace(",","")) > 0.0:
        
        policy['coverage']['combinedSectionLimit'] = str(reconcile_two['CSL'][0]).replace(",","")
      else:
        policy['coverage']['splitSectionBodyPerPerson'] = str(reconcile_two['BI'][0]).replace(",","")
        policy['coverage']['splitSectionBodyPerAccidentOptions'] = str(reconcile_two['BI_PER_ACC'][0]).replace(",","")
        policy['coverage']['splitSectionPropertyDamageOptions'] = str(reconcile_two["PD"][0]).replace(",","")
        
      if "N/A" not in str(reconcile_two['UIMCSL'][0]):
        if float(str(reconcile_two['UIMCSL'][0]).replace(",","")) > 0.0:
              
          policy['coverage']['underinsuredMotoristSingleLimit'] = str(reconcile_two['UIMCSL'][0]).replace(",","")
          policy['coverage']['underinsuredMotorist'] = "Combined Single Limit"
          policy['coverage']['underinsuredMotoristSingleAutoEntry'] = "17"
          policy['coverage']['underMotoristAuto'] = "Excluded"
          
          
          
        else:
          policy['coverage']['underMotoristBodyPerPerson'] = str(reconcile_two['UIMBI'][0]).replace(",","")
          policy['coverage']['underMotoristBodyPerAccident'] = str(reconcile_two['UIMBI_PER_ACC'][0]).replace(",","")
          policy['coverage']['underMotoristProperty'] = str(reconcile_two["UIMPD"][0]).replace(",","")
          policy['coverage']['underinsuredMotorist'] = "Split Limit"
          policy['coverage']['underMotoristAuto'] = "17"
          policy['coverage']['underinsuredMotoristSingleAutoEntry'] = "Excluded"
        
      if "N/A" not in str(reconcile_two['UMCSL'][0]):
        if float(str(reconcile_two['UMCSL'][0]).replace(",","")) > 0.0:
              
          policy['coverage']['uninsuredMotoristSingleLimit'] = str(reconcile_two['UMCSL'][0]).replace(",","")
          policy['coverage']['uninsuredMotoristSingleAutoEntry'] = "17"
          policy['coverage']['uninsuredMotorist'] = "Combined Single Limit"
          policy['coverage']['unMotoristAuto'] = "Excluded"
          
        else:
          policy['coverage']['unMotoristBodyPerPerson'] = str(reconcile_two['UMBI'][0]).replace(",","")
          policy['coverage']['unMotoristBodyPerAccident'] = str(reconcile_two['UMBI_PER_ACC'][0]).replace(",","")
          policy['coverage']['unMotoristProperty'] = str(reconcile_two["UMPD"][0]).replace(",","")
          policy['coverage']['uninsuredMotoristSingleAutoEntry'] = "Excluded"
          policy['coverage']['uninsuredMotorist'] = "Split Limit"
          policy['coverage']['unMotoristAuto'] = "17"

      if "N/A" not in str(reconcile_two['PIPLMT'][0]):
          policy['coverage']['pIProtectionSingleLimit'] = str(reconcile_two['PIPLMT'][0]).replace(",","")
      else:
          policy['coverage']['pIProtectionSingleEntry'] = "Excluded"
          
          
      
      if "N/A" not in str(reconcile_two['MED'][0]):
          policy['coverage']['medicalSingleLimit'] = str(reconcile_two['MED'][0]).replace(",","")
      else:
          policy['coverage']['medicalSplitPropertyDamage'] = 0
          policy['coverage']['medicalSingleEntry'] = "Excluded"
          
      if "N/A" not in str(reconcile_two['PEDPIP'][0]) and '250' in str(reconcile_two['PEDPIP'][0]):
          policy['coverage']['pedPipSingleLimit'] = "Yes"
          policy['coverage']['pedPipProtectionPremium'] = str(abs(reconcile_two['PEDPIP_PREM'][0]))
      else:
          policy['coverage']['pedPipSingleLimit'] = "No"
          policy['coverage']['pedPipProtectionPremium'] = '0'
      
          

      policy['payments']['paymentType'] = str(reconcile_two['CR_Install_Freq_Code'][0])
      
      
      reconcile_cancel = reconcile_two[reconcile_two['Tran_Code'].isin(["Cancel", "Cancellation"])]
      
      
      if len(reconcile_cancel) > 0:
            print(reconcile_cancel, policy['policy']['policyNum'])
            policy['cancellation']['isCancelled'] = 'Yes'
            policy['cancellation']['cancellationDate'] = reconcile_cancel['Endt_Eff'].unique()[0]
            
            for index, rowCancel in reconcile_cancel.iterrows():
                  for j in policy['vehicles']['values']:
                        vehicle = ast.literal_eval(j)
                        if rowCancel['Vin'] == vehicle['vin']:
                            
                              for premium in premiums_vehicle:
                                
                                    if vehicle[premium] == '':
                                          vehicle[premium] = 0
                                    if rowCancel[vehicle_policyInfo_mapping[premium]] == '':
                                          rowCancel[vehicle_policyInfo_mapping[premium]] = 0
                                    if float(vehicle[premium]) + float(rowCancel[vehicle_policyInfo_mapping[premium]]) < 0:
                                        vehicle[premium] = 0
                                    else:
                                        vehicle[premium] = format((float(vehicle[premium]) + float(rowCancel[vehicle_policyInfo_mapping[premium]])), '.2f')
                        
                        veh_index = policy['vehicles']['values'].index(j)
                        
                        policy['vehicles']['values'][veh_index] = str(vehicle)
      else:
            policy['cancellation']['isCancelled'] = 'No'                
        
    total_policies.append(str(policy)) 
    

# for col in Data.columns:
#     print(col)
final_policies = [*set(total_policies)]
final_policies_three = []
policy_numbers=[]

for i in range(0,len(final_policies)):
      policy = ast.literal_eval(final_policies[i])
      if policy['policy']['policyNum'] not in policy_numbers:
            policy_numbers.append(policy['policy']['policyNum'])
            final_policies_three.append(final_policies[i])


final_policies_two = []

for i in range(0,len(final_policies_three)):
      policy = ast.literal_eval(final_policies_three[i])
      reconcile_dup_two = reconcile_dup[(reconcile_dup['Policy #'].str.strip())==(policy['policy']['policyNum'].strip())].reset_index()
      if len(reconcile_dup_two) > 0:
            if float(str(reconcile_dup_two['uim csl'][0]).replace(",","")) > 0:
                  policy['coverage']['underinsuredMotoristSingleLimit'] = str(reconcile_dup_two['uim csl'][0]).replace(",","")
                  policy['coverage']['underinsuredMotorist'] = "Combined Single Limit"
                  policy['coverage']['underinsuredMotoristSingleAutoEntry'] = "17"
                  policy['coverage']['underMotoristAuto'] = "Excluded"
                  policy['coverage']['underMotoristBodyPerPerson'] = 0
                  policy['coverage']['underMotoristBodyPerAccident'] = 0
                  policy['coverage']['underMotoristProperty'] = 0
                              
            elif float(str(reconcile_dup_two['uimbi'][0]).replace(",","")) > 0:
                  policy['coverage']['underMotoristBodyPerPerson'] = str(reconcile_dup_two['uimbi'][0]).replace(",","")
                  policy['coverage']['underMotoristBodyPerAccident'] = str(reconcile_dup_two['uimbi per acc'][0]).replace(",","")
                  policy['coverage']['underMotoristProperty'] = str(reconcile_dup_two["uimpd"][0]).replace(",","")
                  policy['coverage']['underinsuredMotoristSingleLimit'] = 0
                  policy['coverage']['underinsuredMotorist'] = "Split Limit"
                  policy['coverage']['underinsuredMotoristSingleAutoEntry'] = "Excluded"
                  policy['coverage']['underMotoristAuto'] = "17"
            
            if float(str(reconcile_dup_two['umcsl'][0]).replace(",","")) > 0:
                  policy['coverage']['uninsuredMotoristSingleLimit'] = str(reconcile_dup_two['umcsl'][0]).replace(",","")
                  policy['coverage']['uninsuredMotorist'] = "Combined Single Limit"
                  policy['coverage']['uninsuredMotoristSingleAutoEntry'] = "17"
                  policy['coverage']['unMotoristAuto'] = "Excluded"
                  policy['coverage']['unMotoristBodyPerPerson'] = 0
                  policy['coverage']['unMotoristBodyPerAccident'] = 0
                  policy['coverage']['unMotoristProperty'] = 0
                              
            elif float(str(reconcile_dup_two['umbi'][0]).replace(",","")) > 0:
                  policy['coverage']['unMotoristBodyPerPerson'] = str(reconcile_dup_two['umbi'][0]).replace(",","")
                  policy['coverage']['unMotoristBodyPerAccident'] = str(reconcile_dup_two['umbi per acc'][0]).replace(",","")
                  policy['coverage']['unMotoristProperty'] = str(reconcile_dup_two["umpd"][0]).replace(",","")
                  policy['coverage']['uninsuredMotoristSingleLimit'] = 0
                  policy['coverage']['uninsuredMotorist'] = "Split Limit"
                  policy['coverage']['uninsuredMotoristSingleAutoEntry'] = "Excluded"
                  policy['coverage']['unMotoristAuto'] = "17"
            

      final_policies_two.append(str(policy))
# i=0
# j=0
# while i < len(final_policies):
#       j=0
#       while j < len(total_policies):
#         duplicatePolicy = False
#         policy_one=ast.literal_eval(total_policies[i])
#         policy_two=ast.literal_eval(total_policies[j])
#         if policy_one['policy']['policyNum'] == policy_two['policy']['policyNum']:
#           if i != j:
#               print(policy_one['policy']['policyNum'])
#               duplicatePolicy=True
        
#         if duplicatePolicy == True:
#           final_policies.remove(final_policies[j])
#           i=0
#           j=0
#           break
#         else:
#           j+=1
#       i+=1







Data_vehicles.to_csv('vehicles_ref.csv')

policy_col = ast.literal_eval(final_policies[0])

cols = list(policy_col['policy'].keys())

cols_coverage = list(policy_col['coverage'].keys())
cols_insured = list(policy_col['insured'].keys())



cols = [*set(cols+cols_insured+cols_coverage+["totalVehicles", "totalActiveVehicles", "totalPremium", "paymentSchedule"])]
cols = cols.sort()
df = pd.DataFrame(columns=cols)

premiums = ['overallPremium', 'personalInjuryProtectionPremium', 'medicalPaymentsPremium', 'underinsuredMotoristPremium', 'uninsuredMotoristPremium', 'hiredCSLPremium', 'nonOwnedCSLPremium']
for i in range(0,len(final_policies)):
      total_premium = 0
      total_vehicles = 0
      total_active_vehicles = 0
      policy_one = ast.literal_eval(final_policies[i])

      policy = policy_one['policy']
      insured = policy_one['insured']
      coverage = policy_one['coverage']
      paymentSchedule = policy_one['payments']['paymentType']
      for j in policy_one['vehicles']['values']:
            
        vehicles_one = ast.literal_eval(j)
        total_vehicles+=1
        if policy['expirationDate'] == vehicles_one['baseExpDate']:
          total_active_vehicles+=1
        vehicle_premium = 0
        for k in premiums:
          if type(vehicles_one[k]) != str:
            vehicle_premium += float(vehicles_one[k])
        total_premium+=vehicle_premium
      
      
      vehicles = {"totalPremium": total_premium, "totalVehicles": total_vehicles, "totalActiveVehicles": total_active_vehicles, "paymentSchedule": paymentSchedule}
        
        
      
      policy.update(insured)
      policy.update(coverage)
      policy.update(vehicles)
      duplicatePolicy = False
      for index, row in df.iterrows():
        if row['policyNum'] == policy['policyNum']:
              duplicatePolicy=True
        
      if duplicatePolicy == False:
        df = df.append(policy, ignore_index=True)



# import pygsheets

# gc = pygsheets.authorize(service_file='datastudiosurya.json')

# sh = gc.open('PolicyData')

# #select the first sheet 
# wks = sh[0]

# #update the first sheet with df, starting at cell B2.  
# wks.set_dataframe(df,(1,1))



# import firebase_admin
# from firebase_admin import credentials
# from firebase_admin import firestore

# # Use a service account
# cred = credentials.Certificate('delta-pagoda-337917-firebase-adminsdk-px8kv-f7e25ff3a4.json')
# firebase_admin.initialize_app(cred)



# db = firestore.client()

# policies_ref = db.collection(u'policies')

# for i in range(0,50):

#       policy_one = ast.literal_eval(final_policies_two[i])

#       vehicles=[]
#       drivers=[]
#       for j in policy_one['vehicles']['values']:
#             vehicle = ast.literal_eval(j)
#             try:
#                   del vehicle['pedPipProtectionPremium']
#             except KeyError:
#                   pass
#             try:
#                   del vehicle['pedPipSingleLimit']
#             except KeyError:
#                   pass
#             vehicles.append(vehicle)
            
#       for j in policy_one['drivers']['values']:
#             driver = ast.literal_eval(j)
#             drivers.append(driver)

#       policy_one['vehicles']['values'] = vehicles
#       policy_one['drivers']['values'] = drivers

#       policyNum = policy_one['policy']['policyNum']

#       try:
#             del policy_one['coverage']['pedPipProtectionPremium']
#       except KeyError:
#             pass

#       db.collection(u'policies').document(u'{}'.format(policyNum)).set(policy_one)

# docs = policies_ref.stream()

# for doc in docs:
#     print(f'{doc.id} => {doc.to_dict()}')


final_policies_pop = []
for i in range(0,len(final_policies_two)):
      policy_one = ast.literal_eval(final_policies_two[i])

      vehicles=[]
      drivers=[]
      for i in policy_one['vehicles']['values']:
            vehicle = ast.literal_eval(i)
            vehicles.append(vehicle)
            
      for i in policy_one['drivers']['values']:
            driver = ast.literal_eval(i)
            drivers.append(driver)

      policy_one['vehicles']['values'] = vehicles
      policy_one['drivers']['values'] = drivers
      
   
      final_policies_pop.append(policy_one)

with open('policies_missing.txt', 'w') as f:
      f.write(str(final_policies_pop))




#################################################################
#################################################################
######################### ENDORSEMENTS ##########################
#################################################################
#################################################################





