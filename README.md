# About
This project is about ingesting data from Oracle ERP financial data through oracle view to BigQuery. it will take this data to build 
dashboard analytics using PowerBI.

# Tool and Framework
* [Python 3.9 on Anaconda Enviroment](https://www.anaconda.com/download)
* [Djanog 4.2](https://docs.djangoproject.com/en/4.2/releases/4.2/) 
* [DB Browser for SQLite](https://sqlitebrowser.org/) 

# ETL Process to load data from ANY Ralational Databases to BigQuery
<img width="703" alt="MIS-FinData" src="https://github.com/technqvi/MIS-FinData/assets/38780060/cb92bf51-b75e-428d-afa0-5ec9012c5335">

* Get the last time to perform that ETL from table view to BQ to do incremental updates from the table view of any database.
* Retrieve data from any database as view by specifying condition to get the most-up-to-date records from time last update from prev step.
* Load  configuration data from SQLite by view name like  partition column, cluster columns  date column  and ETL mode(CSV,Dataframe) so that these can be used to perform data schema validation against BigQuery.
* Check whether the table exists(if no table) and check table schema is on view and BQ is the same (if so).
* Ingest data into BigQuery.
* Record ETL Transaction to SQLite.
* If thre is any error, the script will record error through SQLite and send any error notification to admin.

  # [Web Administration For Data Configuration and Transaction](https://github.com/technqvi/MIS-FinData/tree/main/ETL_Orable_To_BQ/etl_web_admin)
  ![image](https://github.com/technqvi/MIS-FinData/assets/38780060/50e9bb99-0e19-4b19-bd4f-6daee7eb0c1e)
 
# Folder/File & Document
* [LoadDataFromOracleToBQ_Dev.ipynb](https://github.com/technqvi/MIS-FinData/blob/main/LoadDataFromOracleToBQ_Dev.ipynb) | [LoadDataFromOracleToBQ.py](https://github.com/technqvi/MIS-FinData/blob/main/ETL_Orable_To_BQ/LoadDataFromOracleToBQ.py) : script for loading data from any rational databases to BigQuery, we can schecule it on Window Task Scheduler to execute.
* [DailyExportedTable.ipynb]()  :  Export daily item of each table.
* [ETL_Orable_To_BQ](https://github.com/technqvi/MIS-FinData/tree/main/ETL_Orable_To_BQ) :  folder to contail all files to run program.
* [ETL_Orable_To_BQ/etl_web_admin](https://github.com/technqvi/MIS-FinData/tree/main/ETL_Orable_To_BQ/etl_web_admin) : web site administrator to manage data source data and view ETL-Transactoin & Error.
* ETL_Orable_To_BQ/etl_web_admin/etl_config_transaction.db : SQLite Data base to store configuration data , transaction and error. To purge old data 45 days ago, we can run purge_etl_trans_n_days.bat
* [UserManual.docx](https://github.com/technqvi/MIS-FinData/blob/main/UserManual.docx)