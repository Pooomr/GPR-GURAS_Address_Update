# GPR-GURAS_Address_Update
## Background
This Process runs the python script ‘GPR Address Update’ which attempts to update any ‘incomplete’ address record in GPR by extracting the GURAS (Geo-coded Urban and Rural Address system) address data from the REST API endpoint: https://maps.six.nsw.gov.au/arcgis/rest/services/sixmaps/Guras/MapServer. 
A GPR Property address is considered for update if:
-	(House Number, Lot Number, Road Name AND Location Descriptors are all null) AND/OR Suburb is unknown
-	Property has been created within the last 3 months
-	Property has a transaction attached (recent Acquisition/Disposal) OR Property has been created automatically (not by a user)
A GPR Property Address will only be updated if it is matched to a single GURAS Property record (1-to-1), all exceptions will be provided in a final excel output. The exceptions will need to be investigated and the GPR Address manually updated accordingly.
## Steps to follow
1.	Navigate to G:\Strategy\GPR\10. Support Applications & Tools\Python\GPR Address Update
2.	Double click on ‘Run GPR Address Update.bat’ to start batch file
![image](https://github.com/Pooomr/GPR-GURAS_Address_Update/assets/140774543/12ccff42-c0b7-4df6-9573-b0504dc027e7)
3.	Process will start, it should take 3-4 minutes  
![image](https://github.com/Pooomr/GPR-GURAS_Address_Update/assets/140774543/1cbeb01f-10d6-4f76-ab3d-6712b35ddf78)
4.	Once complete, an overview of the results will be displayed. The next steps will be focussed on resolving the ‘Addresses unable to be updated’  
![image](https://github.com/Pooomr/GPR-GURAS_Address_Update/assets/140774543/19e1a1b7-fca4-4f56-a03d-2c9088490cbe)

5.	Any address that was unable to be matched to GPR 1-to-1 or had data issues will be included in the exceptions report. Navigate to: G:\Strategy\GPR\10. Support Applications & Tools\Python\GPR Address Update\Exception Reports
 ![image](https://github.com/Pooomr/GPR-GURAS_Address_Update/assets/140774543/b7926c6e-bb2e-47ad-899c-682683620be2)

6.	The Files are named based on the date and time they were generated (format: ‘YYYYMMDD_HHmmSS’) look for the latest file and open it.
 ![image](https://github.com/Pooomr/GPR-GURAS_Address_Update/assets/140774543/efffa9d3-dbb1-44a2-b398-f5c119e0776a)

7.	The Exceptions report displays all the GURAS Records that were not able to be used to update GPR. The columns to note are:

| **Column** | **Description** |
|-----------|--------------|
| B | Reason why GURAS record is in exception report |
|C-I	| GPR Property data that relates to the GURAS record |
| J-BK |	GURAS Data |
| R	| Address type, 1 = Primary address record|

Use best judgement to determine how to update the GPR address record, e.g. if there are 2 GURAS records (1 George St, 3 George St) it would generally be ok to use (1-3 George St) as the GPR address. 
If there are too many variations and/or suburbs and there is no reasonable way to update the GPR address without losing information, put a description such as ‘(Multiple Addresses)’ in the location descriptor field.
 
![image](https://github.com/Pooomr/GPR-GURAS_Address_Update/assets/140774543/91e7aae1-94d4-4e1f-a222-ea7ac69253cb)

