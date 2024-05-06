'''Update GPR Address
	1. Find recent (past 90 days) Properties with Missing Road Name or unknown suburb
	2. Get List of lots from those Properties
	3. Run Lots through GURAS Extract (Lot -> propid -> GURAS)
	4. For all 1 to 1 matches for Lots -> GURAS, update GPR address table
	
	v1 - First version
		- one to one match is too strict, some properties have multiple lots that refer to same address
		- Exception report does not show GPR properties with no GURAS match
	v2 - Final count distinguishes between unsuccessful updates (either not 1-to-1 match or no GURAS record found)
		- If single GURAS record matches multiple GPR Addresses, this is still accepted
		- Removed 'chained assignment' warning
	v3 - Expand search to pick up more properties
		- Added Automatically created properties (CLID properties etc.)
		- Add error handling for unmatched suburbs
	v4  - No GURAS Address results added to Exceptions report
	v5 	- Added Delay to prevent REST service timeout error
	v6	- Filtered GURAS results to include Principal Address type 1 only
	TO ADD:
	 - Expand criteria for update to allow updates for multi-addresses as long as each field is = OR one is Null. IF field is null take value of other rows
'''

import logging
import sys
#logging.basicConfig(level=logging.DEBUG)
username = sys.argv[1]
logging.basicConfig(filename="log.txt",
					level=logging.INFO,
					format="%(asctime)s - {} - %(message)s".format(username),
					datefmt='%d/%m/%Y %H:%M:%S')
logging.debug("Importing Python Packages...")
logging.info("[START] GPR Address Update process started")

import cx_Oracle
import requests
import json
import pandas as pd
import config
#import os
import time
from datetime import datetime

#Turn off Chained assignment warning line 344 'A value is trying to be set on a copy of a slice from a DataFrame'
pd.options.mode.chained_assignment = None

today = datetime.now()

logging.debug("Python packages imported successfully")

def loadingBar(p: int, msg: str) -> str:
	
	progress = ""
	togo = "          "
	
	togo = togo[:-p] #reduce empty space based on progress
	
	for i in range(p):
		progress += "■"

	print("[{}{}] {}                            ".format(progress, togo, msg), end="\r")

def ifnull(var, val):
	if var is None:
		return val
	elif pd.isna(var):
		return val
	return var
	
def ifnullInt(var, val):
	if var is None:
		return val
	elif pd.isna(var):
		return val
	return int(var)

def getUnique(propid, sppropid):
	if pd.notna(sppropid):
		return int(sppropid)
	elif pd.notna(propid):
		return int(propid)
	else:
		return 0
		
def getSuburbID(suburbname, postcode):
	c.execute("select suburb_id from suburb where upper(name) = '{}' and postcode = {}".format(suburbname, ifnullInt(postcode,0)))
	result = c.fetchone()
	
	#If no GPR suburb match returned
	if pd.notna(result):
		return int(result[0])
	else:
		return 0
	
def connectDB():
	#Connects to GPR Database
	connection = None

	oc_attempts = 0

	while oc_attempts < 2:
		if oc_attempts == 0:
			logging.debug("Trying DPE IP: {}".format(config.dsnDPE))
			dsn = config.dsnDPE
		else:
			dsn = config.dsnDCS
			logging.debug("Trying DCS IP: {}".format(config.dsnDCS))
			
		try:
			connection = cx_Oracle.connect(
				config.username,
				config.password,
				dsn,
				encoding=config.encoding)

			# show the version of the Oracle Database
			logging.debug("{} Connection Successful!".format(connection.version))
			oc_attempts = 2
		except cx_Oracle.Error as error:
			logging.info(error)
			oc_attempts += 1
			
	return connection

def dropTables(tables, c):
	#Check if tables exists, Drop if it does
	for table in tables:
		#Check if table exists
		query = "select * from all_tables where table_name = UPPER('{}')".format(table)
		c.execute(query)
		result = c.fetchone()
		logging.debug("Checking if table {} exists...".format(table))

		if result:
			logging.debug("Table {} exists".format(table))
			query = "drop table {}".format(table)
			c.execute(query)
			logging.debug("{} dropped successfully".format(table))
		else:
			logging.debug("Table {} doesn't exist".format(table))
	
def getRESTData(baseURL, params, serviceName):
	
	retries = 0
	success = False
	r_code = 0
	#cafile = 'C:\TMP\Python\swg.dec.int.cer'
	while not success:
		try:
			response = requests.get(url=baseURL, params=params)
			success = True
		except requests.exceptions.RequestException as e:
			print(e)
			retries += 1
			if retries > 9:
				while True:
					select = input("\nRequest to {} service failed 10 times, Do you want to try again? y/n\n".format(serviceName))
					if select == "y":
						retries = 0
						break
					elif select == "n":
						print("GPR Address update process Aborted!!")
						sys.exit()
					else:
						print("Invalid selection. Please enter y or n")
		
		if response:
			r_code = response.status_code
		else:
			r_code = 0
		
		while r_code != 200 and success:
			print("Response code: {}".format(response.status_code))
			select2 = input("\nInvalid response received, run query again? y/n\n")
			if select2 == "y":
				retries = 0
				success = False
				break
			elif select2 == "n":
				print("GPR Address update process Aborted!!")
				logging.info("GPR Address update aborted by User")
				sys.exit()
			else:
				print("Invalid selection. Please enter y or n")
	
	return json.loads(response.text)
	
if __name__ == "__main__":
	
	loadingBar(1,"10% - Connecting to GPR Database...")
	
	#connect to DB
	connection = connectDB()
	c = connection.cursor()
	
	#Table names
	au_property = "au_address_gpr_property" #address to store properties with addresses that require update
	#au_lots = "au_address_gpr_lots" #current lots in properties that require address update (ptlotsecpn format)
	
	tables = [au_property]
	
	loadingBar(2,"20% - Dropping and creating new table...")
	
	dropTables(tables, c) #Create or drop tables 
	
	#Get GPR Addresses missing a street name and/or Unknown Suburb from the past 90 days
	c.execute("create table {} as\
				select  distinct p.property_id, p.property_no,\
				nvl2(a.name,a.name,'Private Party') as current_responsible_party,\
				nvl2(p.end_date,'EXPIRED','CURRENT') as gpr_property_status,\
				ad.address_id,\
				nvl2(ad.building_name,ad.building_name || ',','') ||\
				nvl2(ad.level_type,ad.level_type || ad.level_no_prefix || ' ' || ad.level_no || ' ' || ad.level_no_suffix || ',','') ||\
				nvl2(ad.unit_type ,ad.unit_type || ad.unit_no_prefix || ' ' || ad.unit_no  || nvl2(ad.unit_no_suffix, ' ' || ad.unit_no_suffix,'') || '/','') ||\
				nvl2(ad.lot_no,'Lot ' || ad.lot_no || ', ','') ||\
				nvl2(ad.house_no_1_prefix,ad.house_no_1_prefix || ' ','') ||\
				nvl2(ad.house_no_1 ,ad.house_no_1,'') ||\
				nvl2(ad.house_no_1_suffix,ad.house_no_1_suffix,'') ||\
				nvl2(ad.house_no_2,'-','')||\
				nvl2(ad.house_no_2_prefix,ad.house_no_2_prefix || ' ','') ||\
				nvl2(ad.house_no_2 ,ad.house_no_2,'') ||\
				nvl2(ad.house_no_2_suffix,ad.house_no_2_suffix || ' ','') ||\
				nvl2(ad.house_no_1, ' ','') ||\
				nvl2(ad.road_1_name ,ad.road_1_name || ' ','') ||\
				nvl2(ad.road_1_type ,ad.road_1_type || nvl2(ad.road_1_suffix ,' ' || ad.road_1_suffix ,'') ,'') ||\
				nvl2(ad.road_2_name, ' / ' || ad.road_2_name || ' ' || ad.road_2_type || nvl2(ad.road_2_suffix ,' ' || ad.road_2_suffix, ''),'') ||\
				nvl2(ad.road_1_name,nvl2(ad.location_descriptor ,', ',''),'') ||\
				nvl2(ad.location_descriptor ,ad.location_descriptor,'') as address,\
				nvl2(s.name, trim(trailing ' ' from s.name) || ' ' || s.postcode,'') as Suburb_AND_Postcode,\
				l.plan_type || '/' || l.lot_no || '/' || l.section_no || '/' || l.plan_no ptlotsecpn\
				from    agency a, agency a2, responsibility_change_event rce, responsibility_change rc, responsibility r,\
						responsibility r2, property p, address ad, suburb s, responsibility_change_type rct, property_lot pl, lot l\
				where   rce.create_date > '1-JUN-2011'\
				and     ((rce.settlement_date  >'1-JUN-2011' and rce.settlement_date < SYSDATE)\
				or      rce.settlement_date is null)\
				and     rce.responsibility_change_event_id = rc.responsibility_change_event_id\
				and     rc.to_responsibility_id = r.responsibility_id\
				and     rce.responsibility_change_type_id = rct.responsibility_change_type_id\
				and     r.property_id = p.property_id\
				and     p.address_id = ad.address_id\
				and     ad.suburb_id = s.suburb_id\
				and     r.agency_id = a.agency_id (+)\
				and     rc.from_responsibility_id = r2.responsibility_id\
				and     r2.agency_id = a2.agency_id (+)\
				and     (ad.house_no_1 is null and ad.lot_no is null and ad.road_1_name is null and ad.location_descriptor is null)\
				and     rce.dealing_no is not null\
				and     rce.create_date > (SYSDATE - 90)\
				and     p.property_id = pl.property_id\
				and     pl.lot_id = l.lot_id\
				and     pl.end_date is null\
				and     l.end_date is null\
			UNION ALL\
			select p.property_id, p.property_no,\
				a.name current_responsible_party,\
				nvl2(p.end_date,'EXPIRED','CURRENT') as gpr_property_status,\
				ad.address_id,\
				nvl2(ad.building_name,ad.building_name || ',','') ||\
				nvl2(ad.level_type,ad.level_type || ad.level_no_prefix || ' ' || ad.level_no || ' ' || ad.level_no_suffix || ',','') ||\
				nvl2(ad.unit_type ,ad.unit_type || ad.unit_no_prefix || ' ' || ad.unit_no  || nvl2(ad.unit_no_suffix, ' ' || ad.unit_no_suffix,'') || '/','') ||\
				nvl2(ad.lot_no,'Lot ' || ad.lot_no || ', ','') ||\
				nvl2(ad.house_no_1_prefix,ad.house_no_1_prefix || ' ','') ||\
				nvl2(ad.house_no_1 ,ad.house_no_1,'') ||\
				nvl2(ad.house_no_1_suffix,ad.house_no_1_suffix,'') ||\
				nvl2(ad.house_no_2,'-','')||\
				nvl2(ad.house_no_2_prefix,ad.house_no_2_prefix || ' ','') ||\
				nvl2(ad.house_no_2 ,ad.house_no_2,'') ||\
				nvl2(ad.house_no_2_suffix,ad.house_no_2_suffix || ' ','') ||\
				nvl2(ad.house_no_1, ' ','') ||\
				nvl2(ad.road_1_name ,ad.road_1_name || ' ','') ||\
				nvl2(ad.road_1_type ,ad.road_1_type || nvl2(ad.road_1_suffix ,' ' || ad.road_1_suffix ,'') ,'') ||\
				nvl2(ad.road_2_name, ' / ' || ad.road_2_name || ' ' || ad.road_2_type || nvl2(ad.road_2_suffix ,' ' || ad.road_2_suffix, ''),'') ||\
				nvl2(ad.road_1_name,nvl2(ad.location_descriptor ,', ',''),'') ||\
				nvl2(ad.location_descriptor ,ad.location_descriptor,'') as address,\
				nvl2(s.name, trim(trailing ' ' from s.name) || ' ' || s.postcode,'') as Suburb_AND_Postcode,\
				l.plan_type || '/' || l.lot_no || '/' || l.section_no || '/' || l.plan_no ptlotsecpn\
			from agency a, responsibility r, property p, property_lot pl, lot l, address ad, suburb s\
			where   r.property_id = p.property_id\
			and     p.address_id = ad.address_id\
			and     ad.suburb_id = s.suburb_id\
			and     r.agency_id = a.agency_id (+)\
			and     (ad.house_no_1 is null and ad.lot_no is null and ad.road_1_name is null and ad.location_descriptor is null)\
			and     p.create_date > (SYSDATE - 90)\
			and     p.create_user in ('ADAPTER')\
			and     p.property_id = pl.property_id\
			and     pl.lot_id = l.lot_id\
			and     pl.end_date is null\
			and     l.end_date is null\
			and     r.end_date is null\
			and     p.end_date is null".format(au_property))
	logging.debug("Table {} created".format(au_property))
	
	s_lots = pd.read_sql("select distinct ptlotsecpn from {}".format(au_property),connection)
	
	#Track Addresses updated/exceptions TO-DO Handle records with no PropID/GURAS matches
	addr_update = 0
	addr_total = len(pd.read_sql("select distinct address_id from {}".format(au_property),connection))
	
	loadingBar(3,"30% - Querying Prop ID Service...")
	
	#Only process if there are lots to query
	if len(s_lots) > 0:
	
		#EXTRACT PROPIDs
		baseURL = "https://maps.six.nsw.gov.au/arcgis/rest/services/sixmaps/Guras/MapServer/10/query"
		
		#initialise list of Propid Results
		propIDResults = list()
		
		#initialist ptlotsecpn string
		lotstring = ''
		
		#Go through all lots to get PropIDs
		for i, row in s_lots.iterrows():
			if lotstring == '':
				lotstring += "'{}'".format(row["PTLOTSECPN"])
			else:
				lotstring += ",'{}'".format(row["PTLOTSECPN"])
			
			#Every 200 lots query service
			if (i + 1) % 200 == 0 or (i + 1) == len(s_lots):
				params = {
					'f':'json',
					'returnGeometry':'false',
					'OutFields':'ptlotsecpn,propid,sppropid',
					'where':'ptlotsecpn in ({})'.format(lotstring)
				}
				
				jsonResult = getRESTData(baseURL, params, "PropID GURAS Service")
				
				#Delay calls to rest service
				time.sleep(2)
				
				if jsonResult.get('features'):
					#iterate through all features in JSON response and add to Result list
					for jr in range(len(jsonResult['features'])):
						if jsonResult['features'][jr]['attributes']['propid']:
							propIDResults.append(jsonResult['features'][jr])
						
				#reset
				lotstring = ''
		
		#Add Unique PropID column
		df_propID = pd.json_normalize(propIDResults)
		df_propID["uniqueID"] = df_propID.apply(lambda x : getUnique(x['attributes.propid'],x['attributes.sppropid']), axis = 1)
		
		#Get GURAS Address
		baseURL = "https://maps.six.nsw.gov.au/arcgis/rest/services/sixmaps/Guras/MapServer/9/query"
		pidstring = ''
		gurasResults = list()
		
		loadingBar(4,"40% - Querying GURAS service...")
		
		for i, row in df_propID.iterrows():
			if pidstring == '':
				pidstring += "{}".format(row["attributes.propid"])
			else:
				pidstring += ",{}".format(row["attributes.propid"])
			
			#Every 200 propids query service
			if (i + 1) % 200 == 0 or (i + 1) == len(df_propID):
				params = {
					'f':'json',
					'returnGeometry':'false',
					'OutFields':'*',
					'where':'propid in ({}) and principaladdresstype = 1'.format(pidstring)
				}
				
				jsonResult = getRESTData(baseURL, params, "GURAS Address Service")
				
				if jsonResult.get('features'):
					#iterate through all features in JSON response and add to Result list
					for jr in range(len(jsonResult['features'])):
						gurasResults.append(jsonResult['features'][jr])
						
				#reset
				pidstring = ''
		
		loadingBar(5,"50% - Transforming GURAS results...")
		
		#Store Results into dataframe
		df_GURAS = pd.json_normalize(gurasResults)
		df_GURAS["uniqueID"] = df_GURAS.apply(lambda x : getUnique(x['attributes.propid'],x['attributes.sppropid']), axis = 1)
		
		#Get original data set to match address data
		gpr_prop = pd.read_sql("select distinct property_id, property_no, current_responsible_party, gpr_property_status, address_id, address, suburb_and_postcode, ptlotsecpn from {}".format(au_property),connection) #Store starting dataset in dataframe
		df_prop_merged = df_propID.merge(df_GURAS, how='inner', on='uniqueID') #merge Propid and Address datasets
		df_prop_merged = df_prop_merged.rename(columns={'attributes.ptlotsecpn': 'PTLOTSECPN'})
		df_merged = gpr_prop.merge(df_prop_merged, how='inner', on='PTLOTSECPN')
		
		#v2 - Remove ptlotsecpn column to remove duplicates
		del df_merged['PTLOTSECPN']
		
		#Remove Duplicates
		df_m_dd = df_merged.drop_duplicates()
		
		loadingBar(6,"60% - Matching GURAS -> GPR Data...")
		
		#v4 - Retrieve no GURAS address records
		df_og_addr = pd.read_sql("select * from {}".format(au_property),connection) #Get original Address data
		df_og_merge = df_og_addr.merge(df_m_dd, on='ADDRESS_ID', how='outer', indicator=True)
		df_no_guras = df_og_merge[df_og_merge['_merge']=='left_only']
		
		#Count occurences TO-DO Handle multiple occurances properly (Ignore Lot ref)
		df_m_dd["property_id_count"] = df_m_dd.groupby("PROPERTY_ID")["PROPERTY_ID"].transform("size")
		df_m_dd["guras_prop_id_count"] = df_m_dd.groupby("uniqueID")["uniqueID"].transform("size")
		
		#Filter to 1 to 1 matches only
		df_m_dd_1 = df_m_dd.loc[(df_m_dd["property_id_count"] == 1)]
		df_m_dd_o = df_m_dd.loc[~(df_m_dd["property_id_count"] == 1)]
		
		#ADD CODE HERE TO HANDLE ONE TO MANY MATCHES WHERE MAJORITY OF FIELDS MATCH
		
		#Store exceptions
		df_exceptions = pd.DataFrame(df_m_dd_o)
		
		#Add column for reason
		df_exceptions["Exception_Reason"] = "Not a 1-to-1 match"
		
		loadingBar(7,"70% - Updating GPR address data...")
		
		#Update Addresses
		for i, row in df_m_dd_1.iterrows():
			#Set Address fields for update
			
			ADDRESS_ID = row["ADDRESS_ID"]
			
			HOUSE_NO_1_PREFIX = ifnull(row["attributes.housenumberfirstprefix"],'')
			HOUSE_NO_1 = ifnullInt(row["attributes.housenumberfirst"],'')
			HOUSE_NO_1_SUFFIX = ifnull(row["attributes.housenumberfirstsuffix"],'')
			HOUSE_NO_2_PREFIX = ifnull(row["attributes.housenumbersecondprefix"],'')
			HOUSE_NO_2 = ifnullInt(row["attributes.housenumbersecond"],'')
			HOUSE_NO_2_SUFFIX = ifnull(row["attributes.housenumbersecondsuffix"],'')
			ROAD_1_NAME = ifnull(row["attributes.roadname"],'').replace("'","''").title()
			ROAD_1_SUFFIX = ifnull(row["attributes.roadsuffix"],'').title()
			ROAD_1_TYPE = ifnull(row["attributes.roadtype"],'')
			UNIT_TYPE = ifnull(row["attributes.unittype"],'')
			UNIT_NO_PREFIX = ifnull(row["attributes.unitnumberprefix"],'')
			UNIT_NO = ifnullInt(row["attributes.unitnumber"],'')
			UNIT_NO_SUFFIX = ifnull(row["attributes.unitnumbersuffix"],'')
			LEVEL_TYPE = ifnull(row["attributes.leveltype"],'')
			LEVEL_NO_PREFIX = ifnull(row["attributes.levelnumberprefix"],'')
			LEVEL_NO = ifnull(row["attributes.levelnumber"],'')
			LEVEL_NO_SUFFIX = ifnull(row["attributes.levelnumbersuffix"],'')
			BUILDING_NAME = ifnull(row["attributes.buildingname"],'').replace("'","''").title()
			LOCATION_DESCRIPTOR = ifnull(row["attributes.locationdescription"],'').replace("'","''").title()
			ROAD_2_NAME = ifnull(row["attributes.secondroadname"],'').replace("'","''").title()
			ROAD_2_TYPE = ifnull(row["attributes.secondroadtype"],'')
			ROAD_2_SUFFIX = ifnull(row["attributes.secondroadsuffix"],'').title()
			
			#Find Suburb ID
			SUBURB_ID = getSuburbID(row["attributes.suburbname"].upper(),row["attributes.postcode"])
			
			#If Road name exists, change address type to 'Street' type = 3
			if len(ROAD_1_NAME) > 0:
				ADDRESS_TYPE_ID = 3
			else:
				ADDRESS_TYPE_ID = 6
			
			#Get Next Version
			c.execute("select version_no from address where address_id = {}".format(ADDRESS_ID))
			vResult = c.fetchone()
			VERSION_NO = int(vResult[0]) + 1
			
			#VALIDATE DATA
			#Road types
			df_roadTypes = pd.read_sql("select name from road_type", connection)
			#pd.DataFrame(["STREETS","ROADS"],columns=['NAME'])# USE THIS TO TEST ROAD TYPES
			rt1Valid = 0 #Track if data is valid 
			rt2Valid = 0
			sbValid = 0
			if len(ROAD_1_TYPE) > 0:
				for ii, row2 in df_roadTypes.iterrows():
					if row2["NAME"].upper() == ROAD_1_TYPE.upper():
						rt1Valid = 1
						ROAD_1_TYPE = row2["NAME"] #Ensures road 1 type value matches reference table
			else:
				rt1Valid = 1 #Empty road type value are valid
			
			if len(ROAD_2_TYPE) > 0:
				for ii, row2 in df_roadTypes.iterrows():
					if row2["NAME"].upper() == ROAD_2_TYPE.upper():
						rt2Valid = 1
						ROAD_2_TYPE = row2["NAME"] #Ensures road 1 type value matches reference table
			else:
				rt2Valid = 1 #Empty road type value are valid
			
			#Unit types
			df_unitTypes = pd.read_sql("select name from unit_type", connection)
			utValid = 0
			if len(str(UNIT_NO)) > 0 and len(UNIT_TYPE) > 0:
				for ii, row2 in df_unitTypes.iterrows():
					if row2["NAME"].upper() == UNIT_TYPE.upper():
						utValid = 1
						UNIT_TYPE = row2["NAME"] #Ensures Unit type value matches reference table
				
				#If no matches are found, refer to look ups #######Currently hardcoded, do better solution in future 
				if utValid == 0:
					if UNIT_TYPE == "U":
						UNIT_TYPE = "Unit"
						utValid = 1
				
			elif len(str(UNIT_NO)) > 0 and len(UNIT_TYPE) == 0:
				#Unit type field empty, default to 'Unit'
				UNIT_TYPE = "Unit"
				utValid = 1
			elif len(str(UNIT_NO)) == 0 and len(UNIT_TYPE) == 0:
				#No Unit data
				utValid = 1
				
			#Level Types
			df_levelTypes = pd.read_sql("select name from level_type", connection)
			ltValid = 0
			if len(str(LEVEL_NO)) > 0 and len(LEVEL_TYPE) > 0:
				for ii, row2 in df_levelTypes.iterrows():
					if row2["NAME"].upper() == LEVEL_TYPE.upper():
						ltValid = 1
						LEVEL_TYPE = row2["NAME"] #Ensures level type value matches reference table
				
			elif len(str(LEVEL_NO)) > 0 and len(LEVEL_TYPE) == 0:
				#Level type field empty, default to 'Level'
				LEVEL_TYPE = "Level"
				ltValid = 1
			elif len(str(LEVEL_NO)) == 0 and len(LEVEL_TYPE) == 0:
				#No Level data
				ltValid = 1
				
			#Check suburb
			if SUBURB_ID > 0:
				sbValid = 1
			
			#Only update Database if all Values are valid
			if rt1Valid == 1 and rt2Valid == 1 and utValid == 1 and ltValid == 1 and sbValid == 1:
				#print("VALID: {}".format(row))
				#VALID ADDRESS, UPDATE GPR
				c.execute("update address set HOUSE_NO_1_PREFIX = '{}',HOUSE_NO_1 = '{}',HOUSE_NO_1_SUFFIX = '{}',HOUSE_NO_2_PREFIX = '{}',HOUSE_NO_2 = '{}',HOUSE_NO_2_SUFFIX = '{}',ROAD_1_NAME = '{}',ROAD_1_SUFFIX = '{}',ROAD_1_TYPE = '{}',UNIT_TYPE = '{}',UNIT_NO_PREFIX = '{}',UNIT_NO = '{}',UNIT_NO_SUFFIX = '{}',LEVEL_TYPE = '{}',LEVEL_NO_PREFIX = '{}',LEVEL_NO = '{}',LEVEL_NO_SUFFIX = '{}',BUILDING_NAME = '{}',LOCATION_DESCRIPTOR = '{}',ROAD_2_NAME = '{}',ROAD_2_TYPE = '{}',ROAD_2_SUFFIX = '{}',SUBURB_ID = '{}',ADDRESS_TYPE_ID='{}',VERSION_NO = {}, UPDATE_USER = 'PYTHON', UPDATE_DATE = CURRENT_TIMESTAMP where address_id = {}".format(HOUSE_NO_1_PREFIX,HOUSE_NO_1,HOUSE_NO_1_SUFFIX,HOUSE_NO_2_PREFIX,HOUSE_NO_2,HOUSE_NO_2_SUFFIX,ROAD_1_NAME,ROAD_1_SUFFIX,ROAD_1_TYPE,UNIT_TYPE,UNIT_NO_PREFIX,UNIT_NO,UNIT_NO_SUFFIX,LEVEL_TYPE,LEVEL_NO_PREFIX,LEVEL_NO,LEVEL_NO_SUFFIX,BUILDING_NAME,LOCATION_DESCRIPTOR,ROAD_2_NAME,ROAD_2_TYPE,ROAD_2_SUFFIX,SUBURB_ID,ADDRESS_TYPE_ID,VERSION_NO,ADDRESS_ID))
				#print("update address set HOUSE_NO_1_PREFIX = '{}',HOUSE_NO_1 = '{}',HOUSE_NO_1_SUFFIX = '{}',HOUSE_NO_2_PREFIX = '{}',HOUSE_NO_2 = '{}',HOUSE_NO_2_SUFFIX = '{}',ROAD_1_NAME = '{}',ROAD_1_SUFFIX = '{}',ROAD_1_TYPE = '{}',UNIT_TYPE = '{}',UNIT_NO_PREFIX = '{}',UNIT_NO = '{}',UNIT_NO_SUFFIX = '{}',LEVEL_TYPE = '{}',LEVEL_NO_PREFIX = '{}',LEVEL_NO = '{}',LEVEL_NO_SUFFIX = '{}',BUILDING_NAME = '{}',LOCATION_DESCRIPTOR = '{}',ROAD_2_NAME = '{}',ROAD_2_TYPE = '{}',ROAD_2_SUFFIX = '{}',SUBURB_ID = '{}',ADDRESS_TYPE_ID='{}',VERSION_NO = {}, UPDATE_USER = 'PYTHON', UPDATE_DATE = CURRENT_TIMESTAMP where address_id = {}".format(HOUSE_NO_1_PREFIX,HOUSE_NO_1,HOUSE_NO_1_SUFFIX,HOUSE_NO_2_PREFIX,HOUSE_NO_2,HOUSE_NO_2_SUFFIX,ROAD_1_NAME,ROAD_1_SUFFIX,ROAD_1_TYPE,UNIT_TYPE,UNIT_NO_PREFIX,UNIT_NO,UNIT_NO_SUFFIX,LEVEL_TYPE,LEVEL_NO_PREFIX,LEVEL_NO,LEVEL_NO_SUFFIX,BUILDING_NAME,LOCATION_DESCRIPTOR,ROAD_2_NAME,ROAD_2_TYPE,ROAD_2_SUFFIX,SUBURB_ID,ADDRESS_TYPE_ID,VERSION_NO,ADDRESS_ID))
				addr_update+=1
			else:
				#Handle INVALID Addresses
				df_exc_app = pd.DataFrame(row.to_dict(),index=[i]) #convert current row to dict then to dataframe
				if sbValid == 0:
					df_exc_app["Exception_Reason"] = "GURAS Suburb not matched to GPR Suburb"
				else:
					df_exc_app["Exception_Reason"] = "Invalid Unit type, Level type or Road type"

				df_exceptions = df_exceptions.append(df_exc_app)
		
		loadingBar(8,"80% - Updating GPR address data...")
		
		#Commit updates
		c.execute("commit")
		
		#Re-organise columns to place 'Exception Reason' first
		df_exceptions = df_exceptions[['Exception_Reason','PROPERTY_ID', 'PROPERTY_NO', 'CURRENT_RESPONSIBLE_PARTY', 'GPR_PROPERTY_STATUS', 'ADDRESS_ID', 'ADDRESS', 'SUBURB_AND_POSTCODE', 'attributes.propid_x', 'attributes.sppropid_x', 'uniqueID', 'attributes.objectid', 'attributes.createdate', 'attributes.gurasid', 'attributes.addresstype', 'attributes.ruraladdress', 'attributes.principaladdresstype', 'attributes.addressstringtype', 'attributes.principaladdresssiteoid', 'attributes.officialaddressstringoid', 'attributes.roadside', 'attributes.housenumberfirstprefix', 'attributes.housenumberfirst', 'attributes.housenumberfirstsuffix', 'attributes.housenumbersecondprefix', 'attributes.housenumbersecond', 'attributes.housenumbersecondsuffix', 'attributes.roadname', 'attributes.roadtype', 'attributes.roadsuffix', 'attributes.unittype', 'attributes.unitnumberprefix', 'attributes.unitnumber', 'attributes.unitnumbersuffix', 'attributes.leveltype', 'attributes.levelnumberprefix', 'attributes.levelnumber', 'attributes.levelnumbersuffix', 'attributes.addresssitename', 'attributes.buildingname', 'attributes.locationdescription', 'attributes.privatestreetname', 'attributes.privatestreettype', 'attributes.privatestreetsuffix', 'attributes.secondroadname', 'attributes.secondroadtype', 'attributes.secondroadsuffix', 'attributes.suburbname', 'attributes.state', 'attributes.postcode', 'attributes.council', 'attributes.deliverypointid', 'attributes.deliverypointbarcode', 'attributes.addressconfidence', 'attributes.contributororigin', 'attributes.contributorid', 'attributes.contributoralignment', 'attributes.routeoid', 'attributes.gnafprimarysiteid', 'attributes.containment', 'attributes.propid_y', 'attributes.sppropid_y', 'property_id_count', 'guras_prop_id_count']]
		#print(list(df_exceptions.columns.values))
		
		loadingBar(9,"90% - Exporting Exceptions report...")
		
		#Count of exceptions
		addr_excp = df_exceptions["ADDRESS_ID"].nunique()
		
		#Export Exceptions
		er_name = today.strftime("%Y%m%d_%H%M%S")
		no_guras_columns = ["PROPERTY_ID_x","PROPERTY_NO_x","CURRENT_RESPONSIBLE_PARTY_x","GPR_PROPERTY_STATUS_x","ADDRESS_ID","ADDRESS_x","SUBURB_AND_POSTCODE_x","PTLOTSECPN"]
		
		with pd.ExcelWriter("Exception Reports\AddressUpdateExceptions_{}.xlsx".format(er_name)) as writer:
			df_exceptions.to_excel(writer, sheet_name="GURAS-GPR Exceptions")
			df_no_guras.to_excel(writer, columns=no_guras_columns, sheet_name="No GURAS")
			
		logging.info("[INFO] {} x Updated, {} x Exceptions, {} x No GURAS record found".format(addr_update,addr_excp, addr_total - addr_update - addr_excp))
		logging.info("[PROCESS] Exception report generated: Exception Reports\AddressUpdateExceptions_{}.xlsx".format(today.strftime("%Y%m%d_%H%M%S")))
		
		print("                                                       ")
		print("-----------------------------------------")
		print(" Address update process complete!")
		print("-----------------------------------------")
		print("   {} x Addresses updated".format(addr_update))
		print("   {} x Addresses unable to be updated".format(addr_excp))
		print("   {} x No GURAS records matched".format(addr_total - addr_update - addr_excp))
		print("        - Exception report generated: Exception Reports\AddressUpdateExceptions_{}.xlsx".format(today.strftime("%Y%m%d_%H%M%S")))
			
	else:
		print("-----------------------------------------")
		print(" No lots to query address data. Exiting.")
		print("-----------------------------------------")
		logging.info("[INFO] No Lots to query address data. Property address update will be stopped")
			
	#Done Close connection
	connection.close()
	
	logging.info("[FINISH] GPR Address Update process finished")