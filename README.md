# About
This project is about ingesting data from Oracle ERP financial data through oracle view to BigQuery. it will take this data to build 
dashboard analytics using PowerBI.

# Tool and Framework
* [Python 3.9 on Anaconda Enviroment](https://www.anaconda.com/download)
* [Djanog 4.2](https://docs.djangoproject.com/en/4.2/releases/4.2/) 
* [DB Browser for SQLite](https://sqlitebrowser.org/) 

# ETL Process to load data from Oracle database to BigQuery
![image](https://github.com/technqvi/MIS-FinData/assets/38780060/cc3eb971-4bbc-437d-95a9-bb786e6620f7)
* Load data view configuration data from SQLite by view name like  partition column , cluster columns  date column  and ETL mode(CSV,Dataframe).
* Get last time to perform ETL to BQ to do incremental updates from Oracle view.
* Retrieve data from Oracle database by specifying condition to synch the most-up-to-date records.
* Check whether the table exists(if no table) and check table schema on view and BQ is the same (if so).
* Ingest data into BigQuery.
* Record ETL Transaction to SQLite.
* if any error, the script will record error through SQLite and send any error notification to admin.

  # Web Administration & Configuration (Python Django)
  ![image](https://github.com/technqvi/MIS-FinData/assets/38780060/50e9bb99-0e19-4b19-bd4f-6daee7eb0c1e)
  There are 4 main functions (Menu on left side)
  * Data Store(Oracle Database) : it stores configuration data to connect Oracle database
  * Data Source: it stores oracle view name and bigquery configuration data such as Partition column,Cluster columns and oracle connection data from Data Store.
  * ETL Transaction : it stores transaction of loading data from Oracle to Bigquery.
  * Log Error: it stores any error from ETL Transaction.

# Program Structure
* [ETL_Orable_To_BQ](https://github.com/technqvi/MIS-FinData/tree/main/ETL_Orable_To_BQ) : script to ingest data from oracle to BigQuery, we can schecule it on Window Task Scheduler.
* [etl_web_admin](https://github.com/technqvi/MIS-FinData/tree/main/ETL_Orable_To_BQ/etl_web_admin) : web site administrator to manage data source data and view ETL-Transactoin & Error.
* etl_config_transaction.db : SQLite Data base to store configuration data , transaction and error.