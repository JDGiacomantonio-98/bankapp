from mysql.connector import connect, Error as SQLError
from os import getenv,path, mkdir, listdir, rename

sql_db_instance = "bankapp"
sql_db_env = "dev"

app_uri = path.join(getenv('USERPROFILE'),'Desktop','code-workbench',sql_db_instance)

ddl_location= path.join(app_uri,sql_db_env,'datalake','DDL')
dml_staging_location= path.join(app_uri,sql_db_env,'datalake','DML','STAGING')
dml_ingested_location= path.join(app_uri,sql_db_env,'datalake','DML','INGESTED')

for i in range(0, dml_ingested_location.split(app_uri)[1].count("\\")):
	mkdir(path.join(app_uri,"\\".join(dml_ingested_location.split(app_uri+"\\")[1].split("\\")[0:i+1]))) if not(path.isdir(path.join(app_uri,"\\".join(dml_ingested_location.split(app_uri+"\\")[1].split("\\")[0:i+1])))) else next

try:
	conn = connect(
		host="127.0.0.1",
		user="root",
		passwd=input("db login psw: "),
		database=sql_db_instance
		)
except SQLError as e:
	print("MySQL error: ", e)
	if e.errno == 1049:
		if input(f"Create {sql_db_instance} db from scratch? (y/n): ") in "yY":
			try:
				conn = connect(
					host="127.0.0.1",
					user="root",
					passwd=input("insert psw: ")
				)
			except SQLError as e:
				print("MySQL: ", e)
				quit()
			else:
				cursor = conn.cursor()
				cursor.execute(f"CREATE DATABASE {sql_db_instance};") # creates db
				cursor.execute(f"USE {sql_db_instance}")

				# creates all tables
				for file in listdir(ddl_location+"\\TABLES"):
					with open(ddl_location+"\\TABLES\\"+file,"r") as ddl:
						cursor.execute(ddl.read())

				# populates all tables for which dml exists
				for file in listdir(dml_ingested_location):
					with open(dml_ingested_location+"\\"+file,"r") as dml:
						for row in dml.readlines()[:-1]:
							cursor.execute(row)
						conn.commit()
							
				for file in listdir(dml_staging_location):
					with open(dml_staging_location+"\\"+file,"r") as dml:
						for row in dml.readlines()[:-1]:
							cursor.execute(row)
						conn.commit()

				# creates all procedures
				for file in listdir(ddl_location+"\\PROCEDURES"):
					with open(ddl_location+"\\PROCEDURES\\"+file,"r") as ddl:
						cursor.execute(ddl.read())

				conn.close()
				
				print("database created!")

				conn = connect(
					host="127.0.0.1",
					user="root",
					passwd=input("insert psw to populate db: "),
					database=sql_db_instance
				)
				cursor=conn.cursor()
				print("running ..")
				cursor.callproc("update_transactions")
				conn.commit()
				print("db is up to date!")
		else:
			quit()
else:
	cursor = conn.cursor()
	for filename in listdir(dml_staging_location):
		with open(dml_staging_location+"\\"+filename,"r") as dml:
			for row in dml.readlines()[:-1]:
				cursor.execute(row)
		conn.commit()
		rename(src=dml.name,dst=path.join(dml_ingested_location,filename))

	cursor.callproc("update_transactions")
	conn.commit()
	print("db is up to date!")