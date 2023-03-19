from os import path, getcwd, getenv

SQL_DB_NAME = "bankapp"
APP_URI = getcwd()
DDL_URI = path.join(APP_URI,'datalake','DDL')
DDL_TABLE_URI = path.join(DDL_URI,'TABLES')
DDL_VIEW_URI = path.join(DDL_URI,'VIEWS')
DDL_PROC_URI = path.join(DDL_URI,'PROCEDURES')

DML_STAGING_LOCATION= path.join(APP_URI,'datalake','DML','STAGING')
DML_INGESTED_LOCATION= path.join(APP_URI,'datalake','DML','INGESTED')
FILE_LANDING_URI = path.join(getenv('USERPROFILE'),'Downloads')

PROVIDERS = {
    "MPS":
    {
    	"name":"MPS",
        "url": "https://digital.mps.it/pri/login/home_mobile.jsp",
        "filename": "I miei movimenti conto.xlsx",
        "raw_dump": path.join(APP_URI,'dump',"MPS"),
		"csv_dump": path.join(APP_URI,'dump',"MPS",'csv')
	}
}