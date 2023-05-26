from django.db import models

# Create your models here.

PARTITIONING_CHOICES = (
    ("DAY", "DAY"),
    ("MONTH", "MONTH"),
    ("YEAR", "YEAR"),
)
class DataSource(models.Model):
    #type_name = models.CharField(unique=True, max_length=255,editable=False)
    id = models.CharField(primary_key=True, max_length=100,verbose_name="ID")
    first_load_col=models.CharField(max_length=255,verbose_name='First Load Column',help_text='Date Column to determine condition to load since ?')
    partition_date_col=models.CharField(max_length=100,verbose_name='Partition Column',help_text='only 1 colName')
    partition_date_type=  models.CharField(
        max_length = 50,
        choices = PARTITIONING_CHOICES,
        default = '1',verbose_name='Partition Type'
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
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    no_rows=models.IntegerField()
    is_load_all=models.IntegerField()
    class Meta:
        managed = False
        db_table = 'etl_transaction'
        ordering = ['-etl_datetime']

    def __str__(self):
        return f"{self.data_source} # {self.etl_datetime}"
    
class LogError(models.Model):
    #type_name = models.CharField(unique=True, max_length=255,editable=False)
    id = models.IntegerField(primary_key=True)
    error_datetime = models.CharField(max_length=100)
    etl_datetime = models.CharField(max_length=100)
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    message =models.TextField()

    class Meta:
        managed = False
        db_table = 'log_error'
        ordering = ['-error_datetime']
    def __str__(self):
        return f"{self.etl_datetime} # {self.error_datetime} # {self.data_source}"