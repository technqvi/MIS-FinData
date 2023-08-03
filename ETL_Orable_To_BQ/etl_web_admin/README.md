# About 
Create simple djagon web site to handle administrative task + SQLite

  # Web Administration & Configuration (Python Django)
  ![image](https://github.com/technqvi/MIS-FinData/assets/38780060/50e9bb99-0e19-4b19-bd4f-6daee7eb0c1e)
  There are 4 main functions (Menu on left side)
  * Data Store(Oracle Database) : it stores configuration data to connect Oracle database
  * Data Source: it stores oracle view name and bigquery configuration data such as Partition column,Cluster columns and oracle connection data from Data Store.
  * ETL Transaction : it stores transaction of loading data from Oracle to Bigquery.
  * Log Error: it stores any error from ETL Transaction.
  
## Tutorial
* [https://code.visualstudio.com/docs/python/tutorial-django](https://code.visualstudio.com/docs/python/tutorial-django)
* [https://docs.djangoproject.com/en/4.2/intro/tutorial01/](https://docs.djangoproject.com/en/4.2/intro/tutorial01/)

## Tool and Framwork
* Djaongo 4.2
* create .venv and  run python -m pip install django