from django.db import models


# Create your models here.

class DataStore(models.Model):

    database_ip=models.CharField(max_length=255,verbose_name='DATABASE_IP')
    database_host=models.CharField(max_length=255,verbose_name='DATABASE_HOST') 
    database_port=models.IntegerField(verbose_name='DATABASE_PORT')

    databases_service_name=models.CharField(max_length=255,verbose_name='DATABASES_SERVICE_NAME')
    databases_user=models.CharField(max_length=255,verbose_name='DATABASES_USER')
    databases_password=models.CharField(max_length=255,verbose_name='DATABASES_PASSWORD')


    data_store_name=models.CharField(max_length=255,verbose_name='DATA_STORE_NAME',unique=True)
    
    class Meta:
        managed = False
        db_table = 'data_store'

    def __str__(self):
        return self.data_store_name
    

PARTITIONING_CHOICES = (
    ("DAY", "DAY"),
    ("MONTH", "MONTH"),
    ("YEAR", "YEAR"),
)

LOAD_TO_BQ_FROM_CHOICES = (
    ("dataframe", "dataframe"),
    ("csv", "csv"),
)

class DataSource(models.Model):
    #type_name = models.CharField(unique=True, max_length=255,editable=False)
    id = models.CharField(primary_key=True, max_length=100,verbose_name="View Name")
    datastore = models.ForeignKey(DataStore,  on_delete=models.CASCADE)
    first_load_col=models.CharField(max_length=255,verbose_name='First Load Column',help_text='Date Column to determine condition to load since ?')
    partition_date_col=models.CharField(max_length=100,verbose_name='Partition Column',help_text='only 1 colName')
    partition_date_type=  models.CharField(
        max_length = 50,
        choices = PARTITIONING_CHOICES,
        default = '1',verbose_name='Partition Type'
    )
    load_from_type=  models.CharField(
        max_length = 50,
        choices = LOAD_TO_BQ_FROM_CHOICES,
        default = '1',verbose_name='Load To BQ From Type'
    )
    
    cluster_col_list=models.TextField(max_length=255,null=True,blank=True,verbose_name= 'Cluster List(4 Cols)',help_text='colName1,colName2,colName3,colName4')
    date_col_list=models.TextField(max_length=255,null=True,blank=True,verbose_name= 'DateColumn List',help_text='colName1,colName2...')
    
    
    class Meta:
        managed = False
        db_table = 'data_source'

    def __str__(self):
        return self.id
    
    
class ETLTransaction(models.Model):
    #type_name = models.CharField(unique=True, max_length=255,editable=False)
    id = models.IntegerField(primary_key=True)
    etl_datetime = models.CharField(max_length=100)
    data_source_id=models.CharField(max_length=100)
    # data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    completely=models.IntegerField()
    is_load_all=models.IntegerField()
    class Meta:
        managed = False
        db_table = 'etl_transaction'
        ordering = ['-id']

    def __str__(self):
        return f"{self.data_source_id} # {self.etl_datetime}"
    
class LogError(models.Model):
    #type_name = models.CharField(unique=True, max_length=255,editable=False)
    id = models.IntegerField(primary_key=True)
    error_datetime = models.CharField(max_length=100)
    etl_datetime = models.CharField(max_length=100)
    data_source_id=models.CharField(max_length=100)
    # data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    message =models.TextField()

    class Meta:
        managed = False
        db_table = 'log_error'
        ordering = ['-id']
    def __str__(self):
        return f"{self.etl_datetime} # {self.error_datetime} # {self.data_source_id}"