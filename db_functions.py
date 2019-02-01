import mysql.connector
from mysql.connector.constants import ClientFlag

def open(host,user,password,database,ssl_ca,ssl_cert,ssl_key):
	dbparams = {
	'host':host,
	'user':user,
	'password':password,
	'database':database,
	'client_flags':[ClientFlag.SSL],
	'ssl_ca':ssl_ca,
	'ssl_cert':ssl_cert,
	'ssl_key':ssl_key,
	}
	cnxn = mysql.connector.connect(**dbparams)
	return (cnxn, cnxn.cursor(buffered=True))	
def close(cnxn,cursor):
	cursor.close()
	del cursor
	cnxn.close()
def escape(value):
	return str(value).replace("'","''").replace('"','\"')