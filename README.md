# About
This project is about ingesting data from Oracle ERP financial data through oracle view to BigQuery. it will take this data to build 
dashboard analytics using PowerBI.

# Tool and Framework
* [Python 3.9 on Anaconda Enviroment](https://www.anaconda.com/download)
* [Djanog 4.2](https://docs.djangoproject.com/en/4.2/releases/4.2/) 
* [DB Browser for SQLite](https://sqlitebrowser.org/) 

# ETL Process to load data from ANY Ralational Databases to BigQuery
### [LoadDataFromOracleToBQ_Dev.ipynb](https://github.com/technqvi/MIS-FinData/blob/main/LoadDataFromOracleToBQ_Dev.ipynb)
<img width="703" alt="MIS-FinData" src="https://github.com/technqvi/MIS-FinData/assets/38780060/cb92bf51-b75e-428d-afa0-5ec9012c5335">
<img width="597" alt="data_flow_ingestion" src="https://github.com/technqvi/MIS-FinData/assets/38780060/c07b69da-e554-4173-aff8-e5b9d5638c1e">

* Get the last time to perform that ETL from table view to BQ to do incremental updates from the table view of any database.
* Retrieve data as view table from any database by specifying condition to get the most-up-to-date records from time last update from prev step.
* Load  configuration data from SQLite by view name like  partition column, cluster columns  date column  and ETL mode(CSV,Dataframe) so that these can be used to perform data schema validation against BigQuery.
* Check whether the table exists(if no table) and check table schema is on view and BQ is the same (if so).
* Ingest data into BigQuery.
* Record ETL Transaction to SQLite.
* If thre is any error, the script will record error through SQLite and send any error notification to admin.

  # [Web Administration For Data Configuration and Transaction](https://github.com/technqvi/MIS-FinData/tree/main/ETL_Orable_To_BQ/etl_web_admin)
  ![image](https://github.com/technqvi/MIS-FinData/assets/38780060/50e9bb99-0e19-4b19-bd4f-6daee7eb0c1e)
  * Data Store in WebAdmn is database configuration such as databas name, host/ip ,port ,username  and password.
  * Data Source in WebAdmin is view table created in Database(Data Store) consists of  the following fields.
    * Partition and Cluster column on BigQuery.
    * Date column converted from Datetime.
    * Load type to BigQuery such as CSV,Dataframe.

 
# Program Structure For Production
The figure below shows the program structuret that is running on production that  consists of the following items.
![program-structure](https://github.com/technqvi/MIS-FinData/assets/38780060/b0af9a12-dfcc-4ba8-b574-afb9a73fe7c6)

* LoadDataFromOracleToBQ.py : it is core file to perform ETL from database to Bigquery
* .env : store credentials data in database
* {no}_batch_{view name}.bat : thess file are batch jobs that is set on Window Scheduler to run LoadDataFromOracleToBQ.py by view name and other additonal parameter.
* ManualBatch_LoadDataFromOracleToBQ.bat : this batch job is allowed user to run LoadDataFromOracleToBQ.py by passing view anme manually.
* purge_etl_trans_n_days.bat | purge_etl_trans_n_days.sql : it is userd to purge the ETL transaction.
* sql_init_table.sql :  it is provided to generate table that stored configuration data  sucha  data store and data source(tbale view), ETL transaction and Error.
* etl_web_admin : This is Web admin developed based on Django framework, web site administrator to manage data source data and view ETL-Transactoin & Error.
* ETL_Orable_To_BQ/etl_web_admin/etl_config_transaction.db : SQLite Data base to store configuration data
* DailyExportedTable.py | DailyExportedTable.bat : run this to check the number of records exported from the database to Bigquery for data consistency checks.
* UserManual.docx : For administrator.

