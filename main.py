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
		passwd=input("db login psw: ")
		)
	cursor = conn.cursor()
	cursor.execute(f"USE {sql_db_instance};")
except SQLError as e:
	print("MySQL error: ", e)
	if e.errno == 1049:
		if input(f"Create {sql_db_instance} db from scratch? (y/n): ") in "yY":
			try:
				cursor = conn.cursor()
				cursor.execute(f"CREATE DATABASE {sql_db_instance};") # creates db
				cursor.execute(f"USE {sql_db_instance};")

				# creates all tables
				for file in listdir(ddl_location+"\\TABLES"):
					if file == ".gitkeep":
						continue

					with open(ddl_location+"\\TABLES\\"+file,"r") as ddl:
						cursor.execute(ddl.read())

				# populates all tables for which dml exists
				for file in listdir(dml_ingested_location):
					if file == ".gitkeep":
						continue

					with open(dml_ingested_location+"\\"+file,"r") as dml:
						for row in dml.readlines()[:-1]:
							cursor.execute(row)
						conn.commit()

				# populates all tables for which dml exists
				for file in listdir(dml_staging_location):
					if file == ".gitkeep":
						continue

					with open(dml_staging_location+"\\"+file,"r") as dml:
						for row in dml.readlines()[:-1]:
							cursor.execute(row)
						conn.commit()

				# creates all views
				for file in listdir(ddl_location+"\\VIEWS"):
					if file == ".gitkeep":
						continue

					with open(ddl_location+"\\VIEWS\\"+file,"r") as ddl:
						cursor.execute(ddl.read())

				# creates all procedures
				ddl = ""
				for file in listdir(ddl_location+"\\PROCEDURES"):
					if file == ".gitkeep":
						continue
				
					with open(ddl_location+"\\PROCEDURES\\"+file,"r") as f:
						ddl += "\n" + f.read()
				cursor.execute(ddl)
				
				conn.close()
				
				print("database created!")
				
			except SQLError as e:
				print("MySQL: ", e)
				quit()
			else:
				conn = connect(
					host="127.0.0.1",
					user="root",
					passwd=input("insert psw to populate db: "),
					database=sql_db_instance
				)
				cursor=conn.cursor()
				print("populating ..")
				cursor.callproc("update_transactions")
				cursor.callproc("update_calendar")
				cursor.callproc("update_earnings")
				print("..")
				cursor.callproc("update_revenues")
				conn.commit()
				print("db is up to date!")
		else:
			quit()
else:
	for filename in listdir(dml_staging_location):
		if filename == ".gitkeep":
			continue

		with open(dml_staging_location+"\\"+filename,"r") as dml:
			for row in dml.readlines()[:-1]:
				cursor.execute(row)
		conn.commit()
		rename(src=dml.name,dst=path.join(dml_ingested_location,filename))

	print("updating db ..")
	cursor.callproc("update_transactions")
	cursor.callproc("update_calendar")
	cursor.callproc("update_earnings")
	print("..")
	cursor.callproc("update_revenues")
	conn.commit()
	print("db is up to date!")