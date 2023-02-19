import openpyxl as pyxl
from time import time
from os import path, mkdir, rename
from random import uniform
import io

sql_db_instance = "bankapp"
sql_db_env = "prd"
sql_tablename = "TRANSACTIONS_DUMP"
header = "FLOW_ID, EXEC, PROVIDER, TR_DATE, TR_WEEK, TR_MONTH, TR_DAY, TR_WEEKDAY, TR_HOUR, TR_TYPE, TR_LOCATION, TR_BENEFICIARY, TR_METHOD, TR_INFO, TR_VALUE, TR_UOM"
provider = "MPS"

app_uri = "C:\\Users\\Jacopo\\Desktop\\code-workbench\\"+sql_db_instance

sql_dump_uri = app_uri+"\\"+sql_db_env+"\\datalake\\DML\\STAGING"
raw_dump_uri = app_uri+"\\"+sql_db_env+"\\dump\\"+provider
csv_dump_uri = app_uri+"\\"+sql_db_env+"\\dump\\"+provider+"\\csv"

revenues_owner = "SELF"

for i in range(0, sql_dump_uri.split(app_uri)[1].count("\\")):
	mkdir(path.join(app_uri,"\\".join(sql_dump_uri.split(app_uri+"\\")[1].split("\\")[0:i+1]))) if not(path.isdir(path.join(app_uri,"\\".join(sql_dump_uri.split(app_uri+"\\")[1].split("\\")[0:i+1])))) else next

for i in range(0, raw_dump_uri.split(app_uri)[1].count("\\")):
	mkdir(path.join(app_uri,"\\".join(raw_dump_uri.split(app_uri+"\\")[1].split("\\")[0:i+1]))) if not(path.isdir(path.join(app_uri,"\\".join(raw_dump_uri.split(app_uri+"\\")[1].split("\\")[0:i+1])))) else next
mkdir(csv_dump_uri) if not(path.isdir(csv_dump_uri)) else next


landed_file_uri = "C:\\Users\\Jacopo\\Downloads\\I miei movimenti conto.xlsx"

with open(landed_file_uri, "rb") as f:
	wb = pyxl.load_workbook(io.BytesIO(f.read()), data_only=True)

data = wb[wb.sheetnames[0]]

exec = int(time())

processed_filename = str(exec)+"_"+landed_file_uri.split("\\")[-1].replace(" ","_")

sql_insert_name = f"INSERT_{sql_tablename}_{exec}.sql"
sql_insert_dml = "" # empty insert sql statement into target tablename

csv_name = f"TRANSACTIONS_{exec}.csv"
csv = header + "\n" # initialise empty csv with specified header

for r in data.iter_rows(min_col=3, max_col=7, min_row=20, values_only=True):
	
	r = ["TRANSACTIONS", exec, provider] + list(r) + ["EUR"] # adds metadata to  payload making it unique even if extraction is repeated w/ same parameters
	
	if "Totale" in r[5]:
		wb.close()
		break

	r[5] = r[5].replace("\n"," ") # removes \n inserted by the provider when transaction description gets verbose
	r.pop(6) # remove empty cell for each row, provider-specific file structure

	r.insert(4,int(r[3].strftime("%W"))) # weeknum
	r.insert(5,int(r[3].strftime("%m"))) # monthnum
	r.insert(6,int(r[3].strftime("%d"))) # daynum
	r.insert(7,int(r[3].strftime("%w"))) # weekday
	r[3]=r[3].strftime("%Y-%m-%d")

	if "Prelievo " in r[9]:
		r.insert(8,int(r[9].split("Prelievo Self Service Atm ")[1].split(" ")[2].split(":")[0])) # hour
	elif "Bon." in r[9] or  "Storno" in r[9] or "bollo" in r[8] or "Addebito" in r[8]:
		r.insert(8,int(uniform(9, 20))) # hour randomly assigned due to lack of data in provider payload
	else:
		try:
			r.insert(8,int(r[9].split(" Ora ")[1][0:2])) # hour
		except IndexError:
			r.insert(8,None) # hour

	r.insert(9, "expense" if r[-2]<=0 else "revenue") # type

	if "Prelievo " in r[11]:
		r.insert(10," ".join(r[11].split("Prelievo Self Service ")[1].split(" ")[0:2])) # location
	elif "Bon." in r[11]:
		r.insert(10,"Home Banking") # location
	else:
		try:
			r.insert(10,r[11].split(" Loc.")[1].split(" Esercente" if " Esercente" in r[11] else " Imp.")[0].title()) # location
		except IndexError:
			r.insert(10,None) # location

	if r[-2]>0:
		r.insert(11,revenues_owner) # beneficiary
	elif "Prel." in r[11]:
		r.insert(11,"Unknown") # beneficiary
	elif "Bon." in r[11] or "Addebito" in r[11]:
		r.insert(11,r[12].split(" A Favore ")[1].split(" Iban" if " Iban" in r[12] else "Codice")[0].strip(" ")) # beneficiary
	else:
		try:
			r.insert(11,r[12].split("Esercente")[1].split(" Imp.")[0].strip().lstrip(":").lstrip().title()) # beneficiary
		except IndexError:
			r.insert(11,None) # beneficiary

	csv += str(r)[1:-1].replace(" '",' "').replace("',",'",') + "\n"
	sql_insert_dml += f"INSERT INTO {sql_db_instance}.{sql_tablename} ({header}) VALUES ({str(r)[1:-1]});" + "\n"

with open(csv_dump_uri+"\\"+csv_name, "w") as f:
	f.write(csv.replace("\n'",'\n"').replace("'\n",'"\n'))

with open(sql_dump_uri+"\\"+sql_insert_name, "w") as f:
	f.write(sql_insert_dml.replace("None","NULL"))

rename(src=landed_file_uri,dst=path.join(raw_dump_uri,processed_filename))