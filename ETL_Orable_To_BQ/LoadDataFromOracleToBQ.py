#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
from datetime import datetime 
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest
import os
import sys 

import cx_Oracle
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

import sqlite3


# # Parameter variable

# In[3]:


source_name=''
#source_name="yip_ar_receipt"
#source_name="yip_invoice_monthly"
#source_name="yip_pj_status"
# set True whatever , you want to reload all items
isLoadingAllItems=False
is_py=True


# In[ ]:





# In[4]:


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
            raise Exception("isLoadingAllItems 1=True | 0=False")

        ok=True 

    else:
        print("Enter the following input: ")
        source_name = input("View Table Name : ")
        source_name=source_name.lower()
        load_option= int(input("Loading All Data option (1=True | 0=False): "))
        if load_option==0:
         isLoadingAllItems=False
        elif load_option==1  :
         isLoadingAllItems=True
        else:
            raise Exception("isLoadingAllItems 1=True | 0=False")  

        print(f"Confirm to Load view = {source_name} and Load All Data= {isLoadingAllItems}")
        press_Y=input(f"Press Y=True But any key=False : ") 
        if press_Y=='Y':
         ok=True

    if ok==False:
        print("No any action")
        quit()


# # Init Variable

# In[16]:


listError=[]

init_date_query='2020-01-01 00:00:00'

projectId='mismgntdata-bigquery'
region='asia-southeast1'
dataset_id='MIS_BI_DW'
table_id = f"{projectId}.{dataset_id}.{source_name}"

_ip='172.30.57.10' #'172.30.57.10'
_hostname='YIPGERP'
_port=1521
_servicename='PROD'
_username='yipgbi'
_password='yipgbi'


host = 'mail.yipintsoi.com'
port=  25
sender="mis-bi-service@yipintsoigroup.com"
receivers=['pongthorn.sa@yipintsoi.com']
#receivers=['pongthorn.sa@yipintsoi.com','mis-bi-service@yipintsoigroup.com']

data_base_file=r'D:\ETL_Orable_To_BQ\etl_web_admin\etl_config_transaction.db'
sqlite3.register_adapter(np.int64, lambda val: int(val))
sqlite3.register_adapter(np.int32, lambda val: int(val))

json_credential_file=r'C:\Windows\mismgntdata-bigquery--bq-loader-34713c332847.json'

temp_path=f'temp/{source_name}.csv'


start_date_query=''
updateCol='last_update_date'


# In[17]:


dt_imported=datetime.now()
dtStr_imported=dt_imported.strftime("%Y-%m-%d %H:%M:%S")
#dtStr_imported='2023-05-23 23:00:00'

dt_imported=datetime.strptime(dtStr_imported,"%Y-%m-%d %H:%M:%S")

print(dtStr_imported)
print(dt_imported)


# # Manage Log Error Message

# In[18]:


import  smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def sendMailForError(errorHtml):
    message = MIMEMultipart("alternative")
    message["Subject"] = f"MIS-BI : ETL-Error on {source_name} at {dtStr_imported}"
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
    


# In[19]:


def logErrorMessage(errorList):
    def logError(recordList):
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
            listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)]) 
            logErrorMessage(listError)
        finally:
            if sqliteConnection:
                sqliteConnection.close()
            
    if len(errorList)>0:
        error_message=f"{source_name} ETL at {dt_imported} raise some errors."
        print(error_message)
        
        dfError=pd.DataFrame(data=errorList,columns=["error_datetime","etl_datetime","data_source_id","message"])
        print(dfError)
        
        logError(dfError.to_records(index=False))
        
        emailResult=sendMailForError(dfError.to_html(index=False))
        
        error_message=f"{source_name} ETL at {dt_imported} raise some errors."
        
        
        # send email to admin
        raise  Exception(error_message)
    


# # Get & Set Oracle ViewName and other configuration data

# In[20]:


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
           return None
   except Exception as e:
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)]) 
       logErrorMessage(listError)
    
ds_item=get_ds(source_name)


# In[21]:


if ds_item is not None:
    print("Load data source config data")

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
     print(clusterCols)

    if ds_item['date_col_list']=='':
     dateCols=[]   
     print(f"{dateCols} (No Date cols)")   
     
    else:
     dateCols=  ds_item['date_col_list'].split(',') 
     dateCols = list(map(str.strip,dateCols))
     dateCols = list(map(str.lower,dateCols))      
     print(dateCols)


# # List Last ETL Transacton by Datasource Name
# ### Get last etl of the specific view to perform incremental update

# In[22]:


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

if isLoadingAllItems==False:
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
        


# # Load data from Oracel  as DataFrame 

# In[23]:


def loadData(isReLoadAll):
    try:
       engine = sqlalchemy.create_engine(f"oracle+cx_oracle://{_username}:{_password}@{_ip}:{_port}/?service_name={_servicename}")
       # if isReLoadAll==True:
       #   sql=f"""select * from {source_name}   
       #     where  {colFirstLoad}>=to_date('{start_date_query}','yyyy-mm-dd') 
       #     """    
       # else:    
       sql =f"""select * from {source_name}  
       where  {updateCol}>=to_date('{start_date_query}','yyyy-mm-dd hh24:mi:ss') """
       print(sql)
       
       dfAll = pd.read_sql(sql, engine)
       dfAll['ImportedAt']=dt_imported 
    
       return dfAll 
    except Exception as e:
       listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
       logErrorMessage(listError)
    

dfAll=loadData(isLoadingAllItems)


# In[24]:


# dfAll=dfAll.drop(columns=['receipt_number','method_name','application_type']) # receipt
#dfAll=dfAll.drop(columns=['customer_trx_id','org_id']) # invoice

listColDF=dfAll.columns.tolist()
print(listColDF)

print(dfAll.info())
dfAll.head()


# In[25]:


if dfAll.empty:
    print("No row to import to BQ")
    exit()


# # BigQuery

# In[26]:


credentials = service_account.Credentials.from_service_account_file(json_credential_file)
client = bigquery.Client(credentials=credentials, project=projectId)


# ## Creaste bigquery schema from dataframe

# In[27]:


# schema = [
# bigquery.SchemaField("CUSTOMER_TRX_ID", "INTEGER", mode="NULLABLE"),
# bigquery.SchemaField("GL_DATE", "DATE", mode="NULLABLE"),
# bigquery.SchemaField("DEPT_NAME", "STRING", mode="NULLABLE"),      
# bigquery.SchemaField("INVOICE_AMOUNT", "FLOAT", mode="NULLABLE"),    
# bigquery.SchemaField("LAST_UPDATE_DATE", "TIMESTAMP", mode="NULLABLE"),
# ]

schema = []
srCols=dfAll.dtypes
for name, type_name in srCols.items():
    # print(name,type_name)
    if str(type_name) in ['int32','int64']:
      schema.append(bigquery.SchemaField(name, "INTEGER"))
    elif str(type_name) =='float64':
      schema.append(bigquery.SchemaField(name, "FLOAT"))
    elif str(type_name) =='datetime64[ns]':
      if name in   dateCols:
         schema.append(bigquery.SchemaField(name,  "DATE"))
      else:
         schema.append(bigquery.SchemaField(name,  "DATETIME"))
    else:
       schema.append(bigquery.SchemaField(name,  "STRING")) 
      
print(schema)  


# ## Check whether dataframe and bigquery schema are the same
# 
# ## Check Existing DataSet and Table

# In[30]:


# dataset
try:
    dataset = client.get_dataset(f"{projectId}.{dataset_id}")
    print("Dataset {} already exists".format(dataset_id))
except Exception as ex:
    msg=f"Dataset {dataset_id} is not found"
    listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,msg]) 
    logErrorMessage(listError)


# In[32]:


def create_table():
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


# In[33]:


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
        e=f"Columns: {listColumnX} are NOT THE SAME on BigQuery and ViewTable {source_name} of Oracle"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    else:
        print("All Fields on BQ and DF are ok.")
        
        
    # PartitionName
    if partitionNameBQ!=partitionCol:
        e=f"Partition Column :{partitionNameBQ} in BQ is NOT THE SAME as {partitionCol} defined on ETL Config on Web Admin"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    else:
        print("Partition Name Fields on BQ and DF is ok.")    
        

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
        print("All Cluster on BQ and DF are ok.")
    
    # Date Type List
    
        # Cluster List
    listDateColX=find_difference(dateCols,dateTypeBQ)
    if len( listDateColX)>0:
        e=f"Date columns : {listDateColX} are NOT THE SAME on BigQuery and ETL Config on Web Admin"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    else:
        print("All Date Column on BQ and DF are ok.")


    if len(listError)>0:
        logErrorMessage(listError)
        # delete table
        # set isLoading=True to load all data
        
#       isLoadingAllItems=True
#       start_date_query=init_date_query

#       print("ReLoad Data due to something in schema changed")  
#       dfAll=loadData(isLoadingAllItems)
#       print(dfAll.info())  

#       print("Delete table and re-create new one.")
#       client.delete_table(table_id, not_found_ok=True)  
#       create_table()
 
  


# In[34]:


# table    
try:
    table=client.get_table(table_id)
    print("Table {} already exists.".format(table_id))
    # if no table it will call create_table
    
    listFieldBQ=[field.name for field in table.schema]
    
    # required field
    partitionNameBQ=table.time_partitioning.field
    partitionTypeBQ=table.partitioning_type

    clusterBQ=table.clustering_fields
    if clusterBQ is None : clusterBQ =[]
        
    dateTypeBQ=[field.name for field in table.schema if field.field_type=='DATE']
    
    
    print(f"All Fields : {listFieldBQ}")
    print(f"Partiton Field&Type: {partitionNameBQ} - {partitionTypeBQ}")
    print(f"Cluster Field List: {clusterBQ}")
    print(f"Date Field List: {dateTypeBQ}")
    
    #check_same_schema(listFieldBQ,partitionNameBQ,partitionTypeBQ,clusterBQ,dateTypeBQ)
    check_same_schema()
        
except Exception as ex:
    create_table()



# # To load data into BQ , technically you need  to save it as CSV file first 

# In[35]:


if dfAll.empty==False:
    no_rows=len(dfAll)
    print(f"{no_rows} rows are about to be imported to BQ")
    dfAll.to_csv (temp_path,index=False)


# # Load data from CSV file to BQ

# In[36]:


# if isLoadingAllItems==False:
# print("Load with appending")
try:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1,
        autodetect=False,write_disposition="WRITE_APPEND"
        )

    with open(temp_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            no_rows, len(table.schema), table_id
        )
    )  
except Exception as e:
    listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    logErrorMessage(listError)


# # Create Transation and delete csv file

# In[37]:


#Addtional Try Error    
def insertETLTrans(recordList):
    try:
        sqliteConnection = sqlite3.connect(os.path.abspath(data_base_file))
        cursor = sqliteConnection.cursor()
        sqlite_insert_query = """
        INSERT INTO etl_transaction
        (etl_datetime, data_source_id,no_rows,is_load_all)  VALUES (?,?,?,?);
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
            


if isLoadingAllItems==True:
    is_load_all=1
else:
    is_load_all=0

dfETFTran=pd.DataFrame.from_records([{'etl_datetime':dtStr_imported,'data_source_id':source_name,'no_rows':no_rows,'is_load_all':is_load_all}])
recordsToInsert=list(dfETFTran.to_records(index=False))
insertETLTrans(recordsToInsert)


# In[38]:


#Addtional Try Error
try:
    if os.path.exists(temp_path):
      os.remove(temp_path)
      print(f"Deleted {temp_path}")
    
except Exception as e:
    listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    logErrorMessage(listError)


# In[ ]:




