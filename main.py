from mysql.connector import connect, Error as SQLError
from os import getenv,path, mkdir, listdir, rename
from time import sleep, process_time
import webbrowser
import worker

sql_db_instance = "bankapp"
sql_db_env = "dev"

app_uri = path.join(getenv('USERPROFILE'),'Desktop','code-workbench',sql_db_instance)

ddl_location= path.join(app_uri,sql_db_env,'datalake','DDL')
dml_staging_location= path.join(app_uri,sql_db_env,'datalake','DML','STAGING')
dml_ingested_location= path.join(app_uri,sql_db_env,'datalake','DML','INGESTED')

mps_provider_url = "https://digital.mps.it/pri/login/home_mobile.jsp?_ga=2.215198551.1509105742.1620041788-785024715.1610638296"

def create_db_if_not_exists(conn, cursor):
	print(f"creating '{sql_db_instance}' db from scratch ..")

	cursor.execute(f"CREATE DATABASE {sql_db_instance};") # creates db
	cursor.execute(f"USE {sql_db_instance};")

	# creates all tables
	i = 0
	for file in listdir(ddl_location+"\\TABLES"):
		if file == ".gitkeep":
			continue

		with open(ddl_location+"\\TABLES\\"+file,"r") as ddl:
			cursor.execute(ddl.read())
			i+=1
	print(f"{i} tables created!")

	# populates all tables for which dml exists
	print("populating tables ..")
	ingest_data(conn=conn, cursor=cursor, dml_location=dml_ingested_location, deploy_worker=False)
	ingest_data(conn=conn, cursor=cursor, dml_location=dml_staging_location, deploy_worker=False)

	# creates all views
	i = 0
	for file in listdir(ddl_location+"\\VIEWS"):
		if file == ".gitkeep":
			continue

		with open(ddl_location+"\\VIEWS\\"+file,"r") as ddl:
			cursor.execute(ddl.read())
			i+=1
	print(f"{i} views created!")

	# creates all procedures
	i=0
	ddl = ""
	for file in listdir(ddl_location+"\\PROCEDURES"):
		if file == ".gitkeep":
			continue
	
		with open(ddl_location+"\\PROCEDURES\\"+file,"r") as f:
			ddl += "\n" + f.read()
			i+=1
	cursor.execute(ddl)
	print(f"{i} procedures created!")

	print(f"database '{sql_db_instance}' successfully created!")

def ingest_data(conn, cursor, dml_location=dml_staging_location, deploy_worker=True):

	if deploy_worker:
		try:
			worker.parse_mps_file()
		except FileNotFoundError:
			print()
			print("No new transaction report found in the Download folder.")
			if input("Do you want to extract a new transactions report from MPS website? (y/n): ") in "yY":
				webbrowser.open_new(mps_provider_url)
				print("waiting for new file to be detected in Download folder ..")
				sleep(80) # gives user time to login on MPS home banking and extract the report, while offloading process resources
				i = 1
				while True:
					try:
						worker.parse_mps_file()
					except FileNotFoundError:
						if i == 1:
							sleep(40)
							t = process_time()
							continue

						if process_time()-t > 59*i:
							if input("Python is waiting for a new file to land on the Dowload folder, do you want to stop it? (y/n) ") in "yY":
								break
							else:
								i+=1 #wait for an additional minute before asking again
					except Exception as e:
						print(f"Unknown problem while parsing the report: {e}")
						print("db update failed.")
						quit()
					else:
						break

	for filename in listdir(dml_location):
		if filename == ".gitkeep":
			continue

		with open(dml_location+"\\"+filename,"r") as dml:
			for row in dml.readlines()[:-1]:
				cursor.execute(row)
			conn.commit()

		if dml_location!=dml_ingested_location:
			rename(src=dml.name,dst=path.join(dml_ingested_location,filename))

def elevate_data(conn, cursor):
	try:
		print("updating transactions ..")
		cursor.callproc("update_transactions")
		
		print("updating calendar ..")
		cursor.callproc("update_calendar")

		print("updating earnings ..")
		cursor.callproc("update_earnings")

		print("updating revenues ..")
		cursor.callproc("update_revenues")
	except Exception as e:
		print(f"MySQL error:{e}")
		quit()
	else:
		conn.commit()
		print(f"db '{sql_db_instance}' is up to date!")

def update_db():
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
			print(f"No instance of {sql_db_instance} db found.")
			if input(f"Create {sql_db_instance} db from scratch? (y/n): ") in "yY":
				try:
					create_db_if_not_exists(conn=conn, cursor=cursor)
					conn.close()

				except SQLError as e:
					print("MySQL: ", e)
					quit()
				else:
					conn = connect(
						host="127.0.0.1",
						user="root",
						passwd=input("insert psw to elevate data: "),
						database=sql_db_instance
					)
					elevate_data(conn=conn, cursor=conn.cursor())
			else:
				print("closing connection..")
				quit()
	else:
		ingest_data(conn=conn, cursor=cursor)
		elevate_data(conn=conn, cursor=cursor)

if __name__ == "__main__":
	
	for i in range(0, dml_ingested_location.split(app_uri)[1].count("\\")):
		mkdir(path.join(app_uri,"\\".join(dml_ingested_location.split(app_uri+"\\")[1].split("\\")[0:i+1]))) if not(path.isdir(path.join(app_uri,"\\".join(dml_ingested_location.split(app_uri+"\\")[1].split("\\")[0:i+1])))) else next

	update_db()