# About
This project is about ingesting data from Oracle ERP financial data through oracle view to BigQuery. it will take this data to build 
dashboard analytics using PowerBI.

# Tool and Framework
* [Python 3.9 on Anaconda Enviroment](https://www.anaconda.com/download)
* [Djanog 4.2](https://docs.djangoproject.com/en/4.2/releases/4.2/) 
* [DB Browser for SQLite](https://sqlitebrowser.org/) 

# ETL Process
* -


# Program Structure
* [ETL_Orable_To_BQ](https://github.com/technqvi/MIS-FinData/tree/main/ETL_Orable_To_BQ) : script to ingest data from oracle to BigQuery, we can schecule it on Window Task Scheduler.
* [etl_web_admin](https://github.com/technqvi/MIS-FinData/tree/main/ETL_Orable_To_BQ/etl_web_admin) : web site administrator to manage data source data and view ETL-Transactoin & Error.
* etl_config_transaction.db : SQLite Data base to store configuration data , transaction and error.