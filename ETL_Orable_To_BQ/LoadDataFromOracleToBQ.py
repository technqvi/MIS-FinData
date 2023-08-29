#!/usr/bin/env python
# coding: utf-8

# In[127]:


import pandas as pd
import numpy as np
from datetime import datetime 
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest
import os
import sys 
import shutil

import pyodbc # ms sql server
import cx_Oracle # oracle

import sqlalchemy
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

import sqlite3

from dotenv import dotenv_values


# # Input Parameter

# In[161]:


# set True whatever , you want to reload all items
isLoadingAllItems=False  # True (fullload) 
option_write_to_bq=1 # 1=append (periodict job) 2=truncate (fullload) 
is_py=True


source_name=''

 # sql server
# source_name='vw_ipf'
# oracle
source_name='yit_ar_aging_with_cost'
#source_name='yip_wip_project'
# source_name='yip_bg_account'
#source_name="yip_invoice_monthly" # df
#source_name="yip_ar_receipt"  # df/csv
#source_name='yip_ap_payment' # csv
#source_name="yip_pj_status" # csv
# source_name='yip_po_listing' # df
#source_name='yip_gl_account' # df


# # Time To Import

# In[162]:


dt_imported=datetime.now()

dtStr_imported=dt_imported.strftime("%Y-%m-%d %H:%M:%S")
# dtStr_imported='2023-06-13 17:11:54'

dt_imported=datetime.strptime(dtStr_imported,"%Y-%m-%d %H:%M:%S")

print(dtStr_imported)
print(dt_imported)


env_path=r'D:\ETL_Orable_To_BQ\.env'


# # Constant variable

# In[163]:


init_date_query='2020-01-01 00:00:00'
# init_date_query='2023-01-01 00:00:00'

data_base_file=r'D:\ETL_Orable_To_BQ\etl_web_admin\etl_config_transaction.db'
json_credential_file=r'C:\Windows\mismgntdata-bigquery--bq-loader-34713c332847.json'


csv_temp_folder='csv_temp'
csv_error_folder='csv_error'


# # Enter parameter on script

# In[164]:


if is_py:
    press_Y=''
    ok=False

    if len(sys.argv) > 1:
        source_name=sys.argv[1]

        if sys.argv[2]=='0':
         isLoadingAllItems=False
        elif sys.argv[2]=='1'  :
         isLoadingAllItems=True
        else:
            raise Exception("For Loading Data Mode ,allow you to enter either 1=Full Load | 0=Incremental Load")  
            
        option_write_to_bq= int(sys.argv[3])
        if  option_write_to_bq not in [1,2]:
         raise Exception("For Write to BQ Mode ,Allow you to enter either 1=Append or 2=Truncate")

        ok=True 

    else:
        print("Enter the following input: ")
        source_name = input("View Table Name : ")
        source_name=source_name.lower()
        
        load_option= int(input("Loading Data Mode option (1=Full Load | 0=Incremental Load): "))
        if load_option==0:
         isLoadingAllItems=False
        elif load_option==1  :
         isLoadingAllItems=True
        else:
            raise Exception("For Loading Data Mode ,allow you to enter either 1=Full Load | 0=Incremental Load")  
            

        option_write_to_bq= int(input("Write to BQ  option (1=Append | 2=Truncate): "))
        if  option_write_to_bq not in [1,2]:
         raise Exception("For Write to BQ Mode ,Allow you to enter either 1=Append or 2=Truncate")  

        print(f"Confirm to Load view = {source_name} and Load All Data= {isLoadingAllItems} and Option writing to BigQuery={option_write_to_bq}")
        press_Y=input(f"Press Y=True But any key=False : ") 
        if press_Y=='Y':
         ok=True

    if ok==False:
        print("No any action to do")
        quit()


# # Summary What sytem is about to perform

# In[165]:


print(f"Load view = {source_name} and Load All Data= {isLoadingAllItems} and Option writing toBQ={option_write_to_bq}")


# # Check temp and error folder

# In[166]:


if os.path.exists(csv_temp_folder)==False:
  os.mkdir(csv_temp_folder)
if os.path.exists(csv_error_folder)==False:
  os.mkdir(csv_error_folder)  


# # Init Const Variable

# In[167]:


listError=[]

sqlite3.register_adapter(np.int64, lambda val: int(val))
sqlite3.register_adapter(np.int32, lambda val: int(val))

temp_path=f'{csv_temp_folder}/{source_name}.csv'

start_date_query=''
updateCol='last_update_date'
etlDateCols=[updateCol,'creation_date']


# # Email Nofification &  Manage Log Error Message

# In[168]:


import  smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def sendMailForError(errorSubject,errorHtml):
    message = MIMEMultipart("alternative")
    message["Subject"] =errorSubject 
    message["From"] = sender
    message["To"] = ','.join(receivers)

    html =errorHtml 

    part_html = MIMEText(html, "html")
    message.attach(part_html)

    try:

        with smtplib.SMTP(host,port) as mail_server:
            #mail_server.login(login, password)
            mail_server.sendmail(sender, receivers, message.as_string())
            print("Successfully sent email")

    except (gaierror, ConnectionRefusedError):
       msg='Failed to connect to the server. Bad connection settings?'
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,msg]) 
       logErrorMessage(listError)
    except smtplib.SMTPServerDisconnected:
       msg='Failed to connect to the server. Wrong user/password?'
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,msg]) 
       logErrorMessage(listError)
    except smtplib.SMTPException as e:
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)]) 
       logErrorMessage(listError)

    return True
    


# In[169]:


def move_error_file(): # if any error ,then move csv to investigte later
    error_csv_path=''
    try:
        if os.path.exists(temp_path):
         error_csv_file=f"{source_name}_error_{ dt_imported.strftime('%d%m%y_%H%M%S')}.csv"   
         new_temp_path=f'{csv_temp_folder}/{error_csv_file}'
         os.rename(temp_path,new_temp_path)  

         error_csv_path=f'{csv_error_folder}/{error_csv_file}'
         shutil.move(new_temp_path,error_csv_path )
         


    except Exception as e:
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
        logErrorMessage(listError)
    return   error_csv_path      
 


# In[170]:


def logErrorMessage(errorList,raise_ex=True):
    
    def add_error_to_file(error_des):
        f = open(r'log_error.txt', 'a')
        error_str = f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}|{repr(error_des)}\n'

        f.write(error_str)
        f.close()
        print(error_str)
        raise Exception(error_str)
    
    def add_logError(recordList):
        try:
            sqliteConnection = sqlite3.connect(os.path.abspath(data_base_file))
            cursor = sqliteConnection.cursor()
            sqlite_insert_query = """
            INSERT INTO log_error
            (error_datetime,etl_datetime, data_source_id,message)  VALUES (?,?,?,?);
             """
            cursor.executemany(sqlite_insert_query, recordList)
            print("Done Log Error")
            sqliteConnection.commit()
            cursor.close()
            
        except Exception as e:
            msg=f"{data_base_file} error : {str(e)}"
            listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)]) 
            add_error_to_file(msg)
        finally:
            if sqliteConnection:
                sqliteConnection.close()
            
    if len(errorList)>0:
        
        error_message=f"ETL Error on {source_name} at {dtStr_imported}"
        # move csv error file to examine later
        error_csv_path  =move_error_file()
        if error_csv_path!='':
            error_message= f"{error_csv_path} - {error_message}"
        error_message=f"MIS-BI : {error_message}"   
        
        print(error_message)
        
        dfError=pd.DataFrame(data=errorList,columns=["error_datetime","etl_datetime","data_source_id","message"])
        print(f"Total log error={len(dfError)}")
        print(dfError)
        
        # add to error table into sqlite
        recordError=dfError.to_records(index=False) 
        add_logError(recordError)
        
        # sened main
        emailResult=sendMailForError(error_message, dfError.to_html(index=False))
        
        if raise_ex==True:
         raise  Exception(error_message)
    


# # Read Credential Config from .env file

# In[171]:


try: 
    config = dotenv_values(dotenv_path=env_path)
    projectId=config['PROJECT_ID']
    region=config['REGION']
    dataset_id=config['DATASET_ID']
    table_id = f"{projectId}.{dataset_id}.{source_name}"
    

    host = config['MAIL_IP']
    port=  int(config['MAIL_PORT'])
    sender=config['MAIL_SENDER']
    receivers=[config['MAIL_RECEIVER']]
    
    
    print(f"{projectId} - {region} - {dataset_id} - {table_id}")
    # print(f"{host}-{port}-{sender}-{receivers}")
    print("Read all credential config values sucessfully.")


except Exception as e:
    msg="Not found .env file or invalid key in .env file"
    listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str( msg)])
    logErrorMessage(listError)


# # Setting BigQuery and Check the follwing
# * check data set
# * check data table , it is not allowed to do full loading  if table exists

# In[172]:


# credentials = service_account.Credentials.from_service_account_file(json_credential_file)
# client = bigquery.Client(credentials=credentials, project=projectId)
client = bigquery.Client(project=projectId)
print()

write_to_bq_mode="WRITE_APPEND" 
if option_write_to_bq==2:
 write_to_bq_mode="WRITE_TRUNCATE"

print(f"Load to BQ by : {write_to_bq_mode}")

# dataset
try:
    dataset = client.get_dataset(f"{projectId}.{dataset_id}")
    print("Dataset {} already exists".format(dataset_id))
except Exception as ex:
    msg=f"Dataset {dataset_id} is not found"
    listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,msg]) 
    logErrorMessage(listError)
    
    


# In[173]:


# table for full loading
def get_table():
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False
     
if isLoadingAllItems==True:
    if get_table()==True:
        msg=f"Found {table_id}, isReLoadAll={isLoadingAllItems}, please delete table  prior to performing full load."
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,msg]) 
        logErrorMessage(listError)
        
#       print("Delete table and re-create new one.")
#       client.delete_table(table_id, not_found_ok=True)  
#       create_table()


# # Get data view as data source

# In[174]:


# get data from data_source
def get_ds(data_source_name):
   try: 
        conn = sqlite3.connect(os.path.abspath(data_base_file))
        sql_ds=f"""select * from data_source where id='{data_source_name}'  """
        print(sql_ds)
        df_item=pd.read_sql_query(sql_ds, conn)
        if df_item.empty==False:
           return df_item.iloc[0,:]
        else:
            raise Exception(f"Not found {data_source_name} in data_source table")

   except Exception as e:
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)]) 
       logErrorMessage(listError)
    
ds_item=get_ds(source_name)
init_date_query=f"{ds_item['first_load_date']} 00:00:00"
print(ds_item)
print(f"Init date to query : {init_date_query}")


# # Get data store by data view

# In[175]:


# get data from data_store
def get_data_store(datastore_id):
   try: 
        conn = sqlite3.connect(os.path.abspath(data_base_file))
        sql_ds=f"""select * from data_store where id={datastore_id}  """
        print(sql_ds)
        df_item=pd.read_sql_query(sql_ds, conn)
        
        if df_item.empty==False:
           return df_item.iloc[0,:]
        else:
           return None
   except Exception as e:
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)]) 
       logErrorMessage(listError)

datastore=get_data_store(ds_item["datastore_id"])
db_product=datastore['database_product']
print(f"{datastore['data_store_name']} - {datastore['database_product']}")

try:
     
    _ip=datastore['database_ip']
    _hostname=datastore['database_host']
    _port=datastore['database_port']
    _servicename=datastore['databases_service_name']
    _username=datastore['databases_user']
    _password=datastore['databases_password']
    #print(f"{_ip}#{_hostname}#{_port}#{_servicename}#{_username}#{_password}")

    
    print("Read database values sucessfully.")


except Exception as e:
    msg=f"Not found data store  {datastore['data_store_name']}"
    listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str( msg)])
    logErrorMessage(listError)


# # Load Data Configuration of each data source for BigQuery

# In[176]:


if ds_item is not None:
    print("Load data source config data")

    loading_from=ds_item['load_from_type']
    print(f"Load data into BigQuery from {loading_from}")
    
    colFirstLoad=ds_item['first_load_col']
    colFirstLoad=colFirstLoad.strip().lower()
    print(f"Date Column as condition to load at first = {colFirstLoad}")

    partitionCol=ds_item['partition_date_col']  # required DateTime Type
    partitionCol=partitionCol.strip().lower()
    if   ds_item['partition_date_type']=="DAY":
     partitionType=bigquery.TimePartitioningType.DAY
    elif ds_item['partition_date_type']=="MONTH":
     partitionType=bigquery.TimePartitioningType.MONTH    
    elif ds_item['partition_date_type']=="YEAR":
     partitionType=bigquery.TimePartitioningType.YEAR   
    else:
     partitionType=bigquery.TimePartitioningType.DAY
    
    print(f"Partition : {partitionCol} - {partitionType}")
    
        
    if ds_item['cluster_col_list']=='':
     clusterCols=[]   
     print(f"{clusterCols} (No cluster cols)")   
     
    else:
     clusterCols=  ds_item['cluster_col_list'].split(',') 
     clusterCols = list(map(str.strip,clusterCols))
     clusterCols = list(map(str.lower,clusterCols))   
     print(f"Cluster List: {clusterCols}")

    if ds_item['date_col_list']=='':
     dateCols=[]   
     print(f"{dateCols} (No Date cols)")   
     
    else:
     dateCols=  ds_item['date_col_list'].split(',') 
     dateCols = list(map(str.strip,dateCols))
     dateCols = list(map(str.lower,dateCols))      
     print(f"Date Cols: {dateCols}")

else:
   listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,f"Not found view {source_name} in data_source table"])
   logErrorMessage(listError)
    
dateColsToConvert=[partitionCol]+dateCols+etlDateCols
dateColsToConvert = list(dict.fromkeys(dateColsToConvert))
print("All Reuired Date Cols")
print(dateColsToConvert)


# # List Last ETL Transacton by Datasource Name
# ### Get last etl of the specific view to perform incremental update

# In[177]:


def get_last_etl_by_ds(data_source):
   try: 
    conn = sqlite3.connect(os.path.abspath(data_base_file))
    sql_last_etl=f"""select etl_datetime,data_source_id from etl_transaction where data_source_id='{data_source}' 
    order by etl_datetime desc limit 1
    """
    print(sql_last_etl)
    df_item=pd.read_sql_query(sql_last_etl, conn)
    print(df_item)
    return df_item
    
   except Exception as e:
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)]) 
       logErrorMessage(listError)

print(f"Load-Option=={isLoadingAllItems} and (Write-BQ Option={option_write_to_bq}")    
if (isLoadingAllItems==False) and (option_write_to_bq==1):
    dfLastETL=get_last_etl_by_ds(source_name)
    if dfLastETL.empty==False:
      start_date_query=dfLastETL.iloc[0,0]
      print(f"Start Import on update_at of last ETL date :  {start_date_query}" ) 
    else:
       isLoadingAllItems=True 
       
       start_date_query=init_date_query
       print(f"No etl transaction , we will get started with importing all items from :  {start_date_query}" ) 
else:
   start_date_query=init_date_query
   print(f"Reload all data:  {start_date_query}" ) 

if isLoadingAllItems==True:
 is_load_all=1
else:
 is_load_all=0   


# # Load data from DataBase  as DataFrame 

# In[178]:


# https://learn.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-pandas?view=sql-server-ver16
def loadData_mssql(isReLoadAll):
   
    try:
       cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+_hostname +';DATABASE='+_servicename+';Port='+str(_port)+';UID='+_username+';PWD='+_password)
       cursor = cnxn.cursor()
       # laod all Or trucate both are similar but load all is used if schema change 
       if (isReLoadAll==True) or (option_write_to_bq==2):   
         sql=f"""select * from {source_name}   where  {colFirstLoad}>='{start_date_query}' """    
       else:   
           sql =f"""select * from {source_name}  where  {updateCol}>='{start_date_query}' """
       print(sql)

       dfAll = pd.read_sql(sql,  cnxn,parse_dates=dateColsToConvert)
       print(f"isReLoadAll=={isReLoadAll}=={is_load_all}")  

       return dfAll 
    except Exception as e:
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
       logErrorMessage(listError)


# In[179]:


def loadData_oracle(isReLoadAll):
   
    try:
       engine = sqlalchemy.create_engine(f"oracle+cx_oracle://{_username}:{_password}@{_ip}:{_port}/?service_name={_servicename}")
       # laod all Or trucate both are similar but load all is used if schema change 
       if (isReLoadAll==True) or (option_write_to_bq==2): 

         sql=f"""select * from {source_name}   
           where  {colFirstLoad}>=to_date('{start_date_query}','yyyy-mm-dd hh24:mi:ss') 
           """    
       else:   

           sql =f"""select * from {source_name}  
           where  {updateCol}>=to_date('{start_date_query}','yyyy-mm-dd hh24:mi:ss') """
            
       print(sql)
       
       dfAll = pd.read_sql(sql, engine,parse_dates=dateColsToConvert)
       print(f"isReLoadAll=={isReLoadAll}=={is_load_all}")  
    
       return dfAll 
    except Exception as e:
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
       logErrorMessage(listError)
    


# In[180]:


if db_product=='mssql':
    dfAll=loadData_mssql(isLoadingAllItems)
elif db_product=='oracle':
    dfAll=loadData_oracle(isLoadingAllItems)

print(dfAll.shape)    


# In[181]:


if dfAll.empty:
    print("No row to import to BQ")
    exit()
else:
    print(f"{dfAll.shape[0]} rows are about  to import to BQ")


# In[ ]:


# Test Data
# dfAll['last_update_date'].unique()
# dfAll=dfAll.drop(columns=['line_description8','line_description9'  ,'dept_code'])
# _dateColTest='last_update_date'

# dfAll=dfAll.query(f"{_dateColTest}<@_dateValueTest")

# _dateValueTest='2023-06-13 17:11:54'
# dt_imported=datetime.strptime(_dateValueTest,'%Y-%m-%d %H:%M:%S')
# dtStr_imported=dt_imported.strftime("%Y-%m-%d %H:%M:%S")


# if dfAll.empty:
#     print("No row to import to BQ")
#     exit()
# else:
#     print(f"{dfAll.shape[0]} rows are about  to import to BQ")

# # Transform Dataframe prior to Ingesting it to BQ

# In[150]:


dfAll.columns=[ col.lower() for col in  dfAll.columns   ]  

dfAll['ImportedAt']=dt_imported 

listColDF=dfAll.columns.tolist()
print("List columns of DF")
print(listColDF)

print(dfAll.info())
dfAll.head()


# In[ ]:





# # BigQuery Schema Management and Mapping

# In[151]:


listColAdminConfig=[colFirstLoad,partitionCol]
if len(clusterCols)>0:
 listColAdminConfig.extend(clusterCols)
if len(dateCols)>0:
 listColAdminConfig.extend(dateCols)
listColAdminConfig=list(dict.fromkeys(listColAdminConfig))

# checkSomeNotExistingDF = [ col   for col in listColAdminConfig if col not in [ x.lower() for x in  listColDF] ]
checkSomeNotExistingDF = [ col   for col in listColAdminConfig if col not in listColDF ]
if len(checkSomeNotExistingDF)>0:
    msg=f"Some columns in data source are not in dataframe from {db_product} View = {checkSomeNotExistingDF }"
    listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,msg])
    logErrorMessage(listError)
else:
    print(f"{listColAdminConfig} is in dataframe from {db_product} View")


# In[152]:


def createBQSchemaByDF():
# schema = [
# bigquery.SchemaField("CUSTOMER_TRX_ID", "INTEGER", mode="NULLABLE"),
# bigquery.SchemaField("GL_DATE", "DATE", mode="NULLABLE"),
# bigquery.SchemaField("DEPT_NAME", "STRING", mode="NULLABLE"),      
# bigquery.SchemaField("INVOICE_AMOUNT", "FLOAT", mode="NULLABLE"),    
# bigquery.SchemaField("LAST_UPDATE_DATE", "TIMESTAMP", mode="NULLABLE"),
# ]
#https://cloud.google.com/bigquery/docs/schemas
#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.dtypes.html
    schema = []
    srCols=dfAll.dtypes
    try:
        for name, type_name in srCols.items():
            # print(name,type_name)
            if str(type_name) in ['int32','int64']:
              schema.append(bigquery.SchemaField(name, "INTEGER", mode="NULLABLE"))
            elif str(type_name) =='float64':
              schema.append(bigquery.SchemaField(name, "FLOAT", mode="NULLABLE"))
            elif str(type_name) =='datetime64[ns]':
              if name in   dateCols:
                 schema.append(bigquery.SchemaField(name,  "DATE", mode="NULLABLE"))
              else:
                 schema.append(bigquery.SchemaField(name,  "DATETIME", mode="NULLABLE"))
            elif str(type_name) == 'bool':
                 schema.append(bigquery.SchemaField(name,  "BOOL", mode="NULLABLE"))
            else: # if not found type , it will be converted to STRING
               schema.append(bigquery.SchemaField(name,  "STRING", mode="NULLABLE")) 

    except Exception as e:
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
       logErrorMessage(listError)

    schemaDictNameType=dict([(f'{fieldInfo.name}',f'{fieldInfo.field_type}' )  for fieldInfo in schema])
    
    return  schema,schemaDictNameType


# In[153]:


#https://pbpython.com/pandas_dtypes.html
      #https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html  
      #https://datatofish.com/strings-to-datetime-pandas/ 
def convert_dfSchema_same_bqSChema(bqName,bqType): 
  try:  
    if bqType in ["INTEGER","FLOAT"]:
        try:
            if  bqType=="INTEGER":
                dfAll[bqName]=dfAll[bqName].astype('int')
            else:  
                dfAll[bqName]=dfAll[bqName].astype('float')
        except Exception as e:
            print(f"Extra : {bqName}-{bqType} has been converted by pd.to_numeric")
            dfAll[bqName]=pd.to_numeric(dfAll[bqName], errors='coerce')
    elif bqType in ["DATE","DATETIME"]:
      dfAll[bqName]=pd.to_datetime(dfAll[bqName], errors='coerce',exact=False)   
    elif bqType=="BOOL":
      dfAll[bqName]=dfAll[bqName].astype('bool')
    else:
      dfAll[bqName]=dfAll[bqName].apply(lambda x: str(x))  
    
    print(f"{bqName} has been converted  to {bqType} ")
    
  except Exception as ex:
    raise ex


# In[154]:


def create_table(schema):
    try:  
        table = bigquery.Table(table_id,schema=schema)
        if  partitionCol!="":
         table.time_partitioning = bigquery.TimePartitioning(
         type_=partitionType,field=partitionCol)

        if len(clusterCols)>0:
         table.clustering_fields = clusterCols

        table = client.create_table(table) 
        print(
            "Created new table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    except Exception as e:
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)]) 
        logErrorMessage(listError)   


# In[ ]:





# In[155]:


# def check_same_schema(listFieldBQ,partitionNameBQ,partitionTypeBQ,clusterBQ,dateTypeBQ):
def check_same_schema():
    print("===============================================================================================")
    print("Check every columns name and partition cluster and date type column on table against dataframe")
    def find_difference(dfX,bqX):
        intersec_DF_BQ = [set(dfX).symmetric_difference(set(bqX))]
        list_DF_BQ=[]
        if len(intersec_DF_BQ)>0:
         for item in intersec_DF_BQ:
            list_DF_BQ=list_DF_BQ+list(item)
        return list_DF_BQ 
        
    listColumnX=find_difference(listColDF,listFieldBQ)
    if len(listColumnX)>0:
        e=f"Columns: {listColumnX} are NOT THE SAME on BigQuery and ViewTable {source_name} of {db_product}"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    else:
        print("1#All Fields on BQ and DF are ok.")  
        print("==================================")
        
    try:
        __,SchemaDictNameType=createBQSchemaByDF()

        for key_name, val_filed_type in ExistingSchemaDictNameType.items():
            val2=SchemaDictNameType[key_name] 
            if  val_filed_type!=val2:
              msg=f"{key_name}-{val_filed_type} on existing schema is not them same as {key_name}-{val2} on loading schema."
              print(msg)
              convert_dfSchema_same_bqSChema( key_name,val_filed_type)
        print("2#All Field Type on BQ and DF are the same.")
        print("==================================")

    except Exception as e:
        msg=f'{key_name} name on the loading schema does not exists  in the existing schema on Bigquery, so the system is unable to check field type matching.'
        print(msg)
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,msg])
       
    # PartitionName
    if partitionNameBQ!=partitionCol:
        e=f"Partition Column :{partitionNameBQ} in BQ is NOT THE SAME as {partitionCol} defined on ETL Config on Web Admin"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    else:
        print("3#Partition Name Fields on BQ and DF is ok.")    
        print("==================================" )    
        

    # # PartitionDateType
    if partitionTypeBQ!=partitionType:
        e=f"Partition Date Type :{partitionTypeBQ} in BQ is NOT THE SAME as {partitionType} defined on ETL Config on Web Admin"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    
    # Cluster List
    listClusterX=find_difference(clusterCols,clusterBQ)
    if len( listClusterX)>0:
        e=f"Cluster columns : {listClusterX} are NOT THE SAME on BigQuery and ETL Config on Web Admin"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    else:
        print("4#All Cluster on BQ and DF are ok.")
        print("==================================")      
    
    # Date Type List
    
        # Cluster List
    listDateColX=find_difference(dateCols,dateTypeBQ)
    if len( listDateColX)>0:
        e=f"Date columns : {listDateColX} are NOT THE SAME on BigQuery and ETL Config on Web Admin"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    else:
        print("5#All Date Column on BQ and DF are ok.")
        print("==================================")

    if len(listError)>0:
        logErrorMessage(listError)
        

 
  


# In[156]:


isExistingTable=get_table()
if isExistingTable:    
    table=client.get_table(table_id)
    print("Table {} already exists.".format(table_id))
    
    listFieldBQ=[field.name for field in table.schema]
    bqSchema=table.schema
    ExistingSchemaDictNameType=dict([(f'{fieldInfo.name}',f'{fieldInfo.field_type}' )  for fieldInfo in bqSchema])
    
    # required field
    partitionNameBQ=table.time_partitioning.field
    partitionTypeBQ=table.partitioning_type

    clusterBQ=table.clustering_fields
    if clusterBQ is None : clusterBQ =[]
        
    dateTypeBQ=[field.name for field in table.schema if field.field_type=='DATE']
    
    
    print(f"All {len(ExistingSchemaDictNameType)} Fields as belows")
    print(ExistingSchemaDictNameType)
    
    print(f"Partiton Field&Type: {partitionNameBQ} - {ExistingSchemaDictNameType}")
    print(f"Cluster Field List: {clusterBQ}")
    print(f"Date Field List: {dateTypeBQ}")
    
    check_same_schema()
else:
    bqSchema,_=createBQSchemaByDF()
    create_table(bqSchema)
        


# # Load data from CSV file to BQ

# In[157]:


try: 
    no_rows=len(dfAll)
    no_cols=len(dfAll.columns)
   
    if dfAll.empty==False:
     if loading_from=='csv':
        
        dfAll.to_csv (temp_path,index=False)
    print(f"{no_rows} rows and {no_cols} columns are about to be imported to BQ by {loading_from}")
    print(dfAll.dtypes)
    print("***********************************************************************************")    

except Exception as e:

  listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])  
  logErrorMessage(listError)


# In[158]:


# cannot auto detect because some column , there are Y,N,R  For R BQ is known as Bool
#https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv

def collectBQError(x_job):
 if x_job.errors is not None:
    for error in x_job.errors:  
      msg=f"{error['reason']} - {error['message']}"
      listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,msg])
    if   len(listError)>0:
     logErrorMessage(listError,False)

try:
    print(f"Load bulk data from {loading_from}")
    if loading_from=='csv' :
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1,
            schema=bqSchema,autodetect=False,
            max_bad_records=(no_rows-1),
            # autodetect=True,
            write_disposition=write_to_bq_mode,
            )
        with open(temp_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    else :
        job_config = bigquery.LoadJobConfig(write_disposition=write_to_bq_mode,schema=bqSchema)
        job = client.load_table_from_dataframe(dfAll, table_id, job_config=job_config,)  


    result_job=job.result()  # Waits for the job to complete.
    
    # error but continue
    collectBQError(job)   
    
    print(f"Import data from {source_name} on {db_product} into BQ successfully.")
   


except Exception as e:

  msg=f"BigQuery Error While Ingesting data with {str(e)}"  
  listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,msg])  
  collectBQError(job)
    


# # Create Transation and delete csv file

# In[159]:


#Addtional Try Error    
def insertETLTrans(recordList):
    try:
        sqliteConnection = sqlite3.connect(os.path.abspath(data_base_file))
        cursor = sqliteConnection.cursor()
        sqlite_insert_query = """
        INSERT INTO etl_transaction
        (etl_datetime, data_source_id,is_load_all,completely)  VALUES (?,?,?,?);
         """

        cursor.executemany(sqlite_insert_query, recordList)
        print("Done ETL Trasaction")
        sqliteConnection.commit()
        cursor.close()

    except Exception as e:
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
        logErrorMessage(listError)
        print("Failed to insert etl_transaction table", str(e))
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            


if len(listError)>0:
 is_loaded_completely=0
else:
 is_loaded_completely=1


dfETFTran=pd.DataFrame.from_records([{'etl_datetime':dtStr_imported,'data_source_id':source_name,'is_load_all':is_load_all,'completely':is_loaded_completely}])
recordsToInsert=list(dfETFTran.to_records(index=False))
insertETLTrans(recordsToInsert)


# In[160]:


#Addtional Try Error
try:
    if os.path.exists(temp_path):
      os.remove(temp_path)
      print(f"Deleted {temp_path}")
    
except Exception as e:
    listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    logErrorMessage(listError)


# In[ ]:





# In[ ]:





# In[ ]:




