#!/usr/bin/env python
# coding: utf-8

# In[336]:


import pandas as pd
import numpy as np
from datetime import datetime 
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest
import os

import cx_Oracle
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

import sqlite3

#https://github.com/technqvi/SMart-AI/blob/main/LoadIncident_PostgresToBQ.ipynb
#https://github.com/technqvi/AlertPriceInRange/blob/master/price_range_notification.ipynb


# # Init constant variable

# In[337]:


#source_name="yip_ar_receipt"
source_name="yip_invoice_monthly"
init_date_query='2020-01-01'
# set True whatever , you want to reload all items
isLoadingAllItems=False

listError=[]


# In[339]:


projectId='mismgntdata-bigquery'
region='asia-southeast1'
dataset_id='MIS_BI_DW'
table_id = f"{projectId}.{dataset_id}.{source_name}"

_ip='172.30.57.10'
_hostname='YIPGERP'
_port=1521
_servicename='PROD'
_username='yipgbi'
_password='yipgbi'


data_base_file=r'D:\ETL_Orable_To_BQ\etl_web_admin\etl_config_transaction.db'
sqlite3.register_adapter(np.int64, lambda val: int(val))
sqlite3.register_adapter(np.int32, lambda val: int(val))

json_credential_file=r'C:\Windows\mismgntdata-bigquery--bq-loader-34713c332847.json'

temp_path=f'temp/{source_name}.csv'


start_date_query=''
updateCol='last_update_date'


# In[340]:


dt_imported=datetime.now()
dtStr_imported=dt_imported.strftime("%Y-%m-%d %H:%M:%S")
#dtStr_imported='2023-05-23 23:00:00'

dt_imported=datetime.strptime(dtStr_imported,"%Y-%m-%d %H:%M:%S")

print(dtStr_imported)
print(dt_imported)


# # Manage Log Error Message

# In[341]:


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
        
        error_message=f"{source_name} ETL at {dt_imported} raise some errors."
        
        # send email to admin
        raise  Exception(error_message)
    


# # Get & Set Oracle ViewName and other configuration data

# In[342]:


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


# In[343]:


if ds_item is not None:
    print("Load data source config data")

    colFirstLoad=ds_item['first_load_col']
    print(f"Column to load at first = {colFirstLoad}")

    partitionCol=ds_item['partition_date_col']  # required DateTime Type
    if   ds_item['partition_date_type']=="DAY":
     partitionType=bigquery.TimePartitioningType.DAY
    elif ds_item['partition_date_type']=="MONTH":
     partitionType=bigquery.TimePartitioningType.MONTH    
    elif ds_item['partition_date_type']=="YEAR":
     partitionType=bigquery.TimePartitioningType.YEAR   
    else:
     partitionType=bigquery.TimePartitioningType.DAY
    
    print(f"{partitionCol} - {partitionType}")
    
        
    if ds_item['cluster_col_list']=='':
     print("No cluster cols")   
     clusterCols=[]
    else:
     clusterCols=  ds_item['cluster_col_list'].split(',') 
     clusterCols = list(map(str.strip,clusterCols))
     print(clusterCols)

    if ds_item['date_col_list']=='':
     print("No Date cols")   
     dateCols=[]
    else:
     dateCols=  ds_item['date_col_list'].split(',') 
     dateCols = list(map(str.strip,dateCols))
     print(dateCols)


# # List Last ETL Transacton by Datasource Name
# ### Get last etl of the specific view to perform incremental update

# In[344]:


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


dfLastETL=get_last_etl_by_ds(source_name)
if dfLastETL.empty==False:
  start_date_query=dfLastETL.iloc[0,0]
  print(f"Start Import on update_at of last ETL date :  {start_date_query}" ) 
else:
   isLoadingAllItems=True 
   start_date_query=init_date_query
   print(f"No etl transaction , we will get started with importing all items from :  {start_date_query}" ) 
        


# # Load data from Oracel  as DataFrame 

# In[345]:


def loadData(isReLoadAll):
    try:
       engine = sqlalchemy.create_engine(f"oracle+cx_oracle://{_username}:{_password}@{_ip}:{_port}/?service_name={_servicename}")
       if isReLoadAll==True:
         sql=f"""select * from {source_name}   
           where  {colFirstLoad}>=to_date('{start_date_query}','yyyy-mm-dd') 
           """    
       else:    
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


# In[346]:


# dfAll=dfAll.drop(columns=['receipt_number','method_name','application_type']) # receipt
#dfAll=dfAll.drop(columns=['customer_trx_id','org_id']) # invoice

listColDF=dfAll.columns.tolist()
print(listColDF)

print(dfAll.info())
dfAll.head()


# # BigQuery

# In[347]:


credentials = service_account.Credentials.from_service_account_file(json_credential_file)
client = bigquery.Client(credentials=credentials, project=projectId)


# ## Creaste bigquery schema from dataframe

# In[348]:


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

# In[349]:


# dataset
try:
    dataset = client.get_dataset(f"{projectId}.{dataset_id}")
    print("Dataset {} already exists".format(dataset_id))

except Exception as ex:
    raise("Dataset {} is not found".format(dataset_id))


# In[350]:


def create_table():
    table = bigquery.Table(table_id,schema=schema)
    if  partitionCol!="":
     table.time_partitioning = bigquery.TimePartitioning(
     type_=partitionType,field=partitionCol)
    
    if len(clusterCols)>0:
     table.clustering_fields = clusterCols

    table = client.create_table(table) 
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


# In[351]:


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
        e=f"Columns: {listColumnX} are the same on BigQuery and View {source_name} "
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    else:
        print("All Fields on BQ and DF are ok.")
        
        
    # PartitionName
    if partitionNameBQ!=partitionCol:
        e=f"Partition Column :{partitionNameBQ} in BQ is not the same as {partitionCol} defined on ETL Config on Web Admin"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    else:
        print("Partition Name Fields on BQ and DF is ok.")    
        

    # # PartitionDateType
    if partitionTypeBQ!=partitionType:
        e=f"Partition Date Type :{partitionTypeBQ} in BQ is not the same as {partitionType} defined on ETL Config on Web Admin"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    
    # Cluster List
    listClusterX=find_difference(clusterCols,clusterBQ)
    if len( listClusterX)>0:
        e=f"Cluster columns : {listClusterX} are the same on BigQuery defined on ETL Config on Web Admin"
        listError.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),dtStr_imported,source_name,str(e)])
    else:
        print("All Cluster on BQ and DF are ok.")
    
    # Date Type List
    
        # Cluster List
    listDateColX=find_difference(dateCols,dateTypeBQ)
    if len( listDateColX)>0:
        e=f"Date columns : {listDateColX} are the same on BigQuery defined on ETL Config on Web Admin"
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
 
  


# In[352]:


# table    
try:
    table=client.get_table(table_id)
    print("Table {} already exists.".format(table_id))
    # if no table it will call create_table
    
    listFieldBQ=[field.name for field in table.schema]
    
    partitionNameBQ=table.time_partitioning.field
    partitionTypeBQ=table.partitioning_type
    clusterBQ=table.clustering_fields
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

# In[353]:


if dfAll.empty==False:
    no_rows=len(dfAll)
    print(f"{no_rows} rows are about to be imported to BQ")
    dfAll.to_csv (temp_path,index=False)
else:
    print("No row to import to BQ")
    exit()


# # Load data from CSV file to BQ

# In[354]:


# if isLoadingAllItems==False:
# print("Load with appending")
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1,
    autodetect=False,write_disposition="WRITE_APPEND"
    )
# else:
#     print("Load with all data")
#     job_config = bigquery.LoadJobConfig(
#         source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1,
#         autodetect=False,write_disposition="WRITE_TRUNCATE"
#     )



with open(temp_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)
job.result()  # Waits for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        no_rows, len(table.schema), table_id
    )
)



# # Create Transation and delete csv file

# In[355]:


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

    except sqlite3.Error as error:
        print("Failed to insert etl_transaction table", error)
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


# In[356]:


if os.path.exists(temp_path):
  os.remove(temp_path)
  print(f"Deleted {temp_path}")


# In[138]:


# if any error , send mail to adminstrator


# In[ ]:




