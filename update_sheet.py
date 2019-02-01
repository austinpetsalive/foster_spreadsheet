import os
import httplib2
import json
import urllib.request
import config
import datetime
import dateutil
import time
from dateutil import relativedelta
# pip install --upgrade google-api-python-client
from oauth2client.file import Storage
from apiclient.discovery import build
from oauth2client.client import OAuth2WebServerFlow
from apiclient import errors
from pathlib import Path
import requests
import db_functions
import sl_config

#Connect to the DB
cnxn, cursor =db_functions.open(sl_config.dbhost,sl_config.dbuser,sl_config.dbpassword,sl_config.database,sl_config.ssl_ca,sl_config.ssl_cert,sl_config.ssl_key)

OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive'
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'
CREDS_FILE = os.path.join(config.GOOGLE_CREDENTIAL_PATH, 'credentials.json')

storage = Storage(CREDS_FILE)
credentials = storage.get()

if credentials is None:
	# Run through the OAuth flow and retrieve credentials
	flow = OAuth2WebServerFlow(config.GOOGLE_CLIENT_ID, config.GOOGLE_CLIENT_SECRET, OAUTH_SCOPE, REDIRECT_URI)
	authorize_url = flow.step1_get_authorize_url()
	print ('Go to the following link in your browser: ' + authorize_url)
	code = input('Enter verification code: ').strip()
	credentials = flow.step2_exchange(code)
	storage.put(credentials)

# Create an httplib2.Http object and authorize it with our credentials
http = httplib2.Http()
http = credentials.authorize(http)

service = build('sheets', 'v4', http=http)

tab="Tracking!"
value_input_option="RAW"

start = 3
end = 500
count = start
blank =0
prev_animalid=""
while count <= end and blank <30:
	range_name=tab+'B'+str(count)+':B'+str(count)
	result = service.spreadsheets().values().get(
		spreadsheetId=config.spreadsheet_id, range=range_name).execute()
		
	id = result.get('values')
	try:
		animalid=id[0][0]
		blank=0
		if prev_animalid==animalid:
			print("user has changed rows too much to continue processing")
			exit()
	except:
		animalid=""
		blank+=1
	print(animalid)
	print (count, animalid)

	query = "select * from Animals where AnimalID='"+animalid+"'"
	result = cursor.execute(query)
	columns = [column[0] for column in cursor.description]
	row = cursor.fetchone()
	if (len(animalid)>1) and (row is not None):
			animal=(dict(zip(columns, row)))
			internalid=animal['AnimalInternal-ID']
			print(internalid)
			dob = datetime.datetime.fromtimestamp(animal['AnimalDOBUnixTime'])
			print(dob)
			now = datetime.datetime.now()
			diff = dateutil.relativedelta.relativedelta(now, dob)
			days = now - dob
			if diff.years:
				age = str(diff.years) + ' years '+str(diff.months)+" months"
			elif diff.months and diff.months >= 4:
				age = str(diff.months) + ' months '+str(diff.days)+" days"
			elif days.days >= 7:
				weeks = int(days.days/7)
				age = str(weeks)+" weeks "+str(days.days - weeks*7)+ " days"
			else:
				age = str(days.days)+" days"
			weeks = ((now - dob).days)/7
			print (age)
			print (weeks)
			atts=""
			query = "select * from AnimalsAttributes where `AnimalInternal-ID` = '"+str(internalid)+"'"
			result = cursor.execute(query)
			for attribute in cursor.fetchall():
				if attribute[2]=='Behavior Consult':
					if len(atts)>1:
						atts =atts+"/"
					atts=atts+"BC"
				elif attribute[2]=='Medical Consult':
					if len(atts)>1:
						atts =atts+"/"
					atts=atts+"MC"
			
			range_name1 = tab+'A'+str(count)+':I'+str(count)
			print(range_name1)			
			values1 = [
				[
					animal['AnimalName'],animalid,atts,animal['AnimalBreed'],dob.strftime('%m/%d/%Y'),age,weeks,animal['AnimalSex'],animal['AnimalAltered']
				],
				# Additional rows ...
			]
			print (values1)
			personinternalid=0
			queryevent = "SELECT `PersonInternal-ID` FROM `Events` where EventType='Outcome.Foster' and `AnimalInternal-ID`="+str(internalid)+" ORDER BY EventTime DESC limit 1"
			result = cursor.execute(queryevent)			
			lastfoster=cursor.fetchone()
			if lastfoster is not None:
				personinternalid=lastfoster[0]
			range_name2 = tab+'O'+str(count)+':Q'+str(count)
			

			if ((personinternalid >0) and "Foster" in animal['AnimalStatus']):
				person=""
				queryperson = "select PersonEmail, PersonPhone from People where `PersonInternal-ID`="+str(personinternalid)
				result = cursor.execute(queryperson)			
				person=cursor.fetchone()
				if person is not None:
					email = person[0]
					phone = str(person[1])
				else:
					email = "unknown"
					phone = "unknown"
				print(range_name2)	
				try:
					fostername = animal['AnimalAssociatedPersonFirstName']+" "+animal['AnimalAssociatedPersonLastName']
				except:
					fostername = "unknown"		
				values2 = [
					[
						fostername,email,phone
					],
					# Additional rows ...
				]

			else:
				values2 = [
					[
						"unknown","unknown","unknown"
					],
					# Additional rows ...
				]
			att_list=""
			scores=""
			query = "select * from AnimalsAttributes where `AnimalInternal-ID` = '"+str(internalid)+"'"
			result = cursor.execute(query)
			for attribute in cursor.fetchall():
				if "SCORE" not in attribute[2] and "ENERGY" not in attribute[2] :
					if len(att_list)>0:
						att_list=att_list+"\n"
					att_list=att_list+attribute[2]
				else:
					if len(scores)>0:
						scores=scores+"\n"				
					scores=scores+attribute[2]

			range_name3 = tab+'AH'+str(count)+':AI'+str(count)
			
			print(range_name3)		
			values3 = [
				[
					internalid, personinternalid
				],
				# Additional rows ...
			]

			range_name4 = tab+'W'+str(count)+':Z'+str(count)
			
			print(range_name4)		
			print(scores)
			try:
				price=animal['AnimalAdoptionFeeGroupPrice']
			except:
				price=""
			values4 = [
				[
					animal['AnimalStatus'], price, scores, att_list
				],
				# Additional rows ...
			]		

			now = datetime.datetime.now()
			values5 = [
				[
					"R"+str(count)+" "+str(datetime.datetime.now())
				],
				# Additional rows ...
			]		
			data = [
			{
				'range': range_name1,
				'values': values1
			},
			{
				'range': range_name2,
				'values': values2
			},	
			{
				'range': range_name3,
				'values': values3
			},	
			{
				'range': range_name4,
				'values': values4
			},		
			{
				'range': tab+'A1',
				'values': values5
			},	
			# Additional ranges to update ...
			]
			body = {
				'valueInputOption': value_input_option,
				'data': data
			}
			time.sleep(1)	
			result = service.spreadsheets().values().get(
				spreadsheetId=config.spreadsheet_id, range=range_name).execute()	
			id = result.get('values')
			current_animalid=id[0][0]
			if current_animalid == animalid:
				result = service.spreadsheets().values().batchUpdate(
				spreadsheetId=config.spreadsheet_id, body=body).execute()
				print('{0} cells updated.'.format(result.get('updatedCells')));
				#exit()
			else:
				print("row changed during update")
	else:
		print("animalid not found",animalid)			
	count+=1	
	prev_animalid=animalid

		
now = datetime.datetime.now()
print(now)
range_name = tab+'A1'
print(range_name)		
values = [
	[
		"Last full update "+str(now)
	],
	# Additional rows ...
]
body = {
	'values': values
}
result = service.spreadsheets().values().update(
spreadsheetId=config.spreadsheet_id, range=range_name,
valueInputOption=value_input_option, body=body).execute()
print('{0} cells updated.'.format(result.get('updatedCells')));
	
#Close the DB connection			
db_functions.close(cnxn, cursor)
now = datetime.datetime.now()
current_timestamp = (now.strftime('%Y-%m-%d %H:%M:%S'))
print("success: "+current_timestamp)


