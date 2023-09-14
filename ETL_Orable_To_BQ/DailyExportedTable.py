#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import datetime 


# In[3]:


tableList=['yip_ap_payment','yip_ar_receipt','yip_bg_account','yip_gl_account'
            ,'yip_invoice_monthly','yip_pj_status','yip_po_listing'
            ,'yip_wip_project','yit_ar_aging_with_cost']
today_str=datetime.now().strftime('%d%b%y') 
writer=pd.ExcelWriter(f"D:\ETL_Orable_To_BQ\{today_str}-MIS-Daily-ExportedTable.xlsx",engine='xlsxwriter')


# In[4]:


projectId='mismgntdata-bigquery'
dataset_id='MIS_BI_DW'

client = bigquery.Client(project=projectId)
def load_data_bq(sql:str):
 client_bq = bigquery.Client()
 query_result=client_bq.query(sql)
 df_all=query_result.to_dataframe()
 return df_all



# In[5]:


for table in tableList:
    table_id = f"{projectId}.{dataset_id}.{table}"
    sql=f"""
    select ImportedAt,count(*) as {table}_no_imported
    from  `{table_id}`  group by ImportedAt  order by ImportedAt desc
    """
    df=load_data_bq(sql)
    df.to_excel(writer, sheet_name=table,index=False)


writer.close()


# In[ ]:




