from django.contrib import admin
from .models import  *
# Register your models here.

@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    actions =None
    list_display = ['id','load_from_type','datastore','partition_date_col']
    readonly_fields = ('id',)
    search_fields = ['id']

    def has_delete_permission(self, request, obj=None):
        return False
    
    def get_readonly_fields(self, request, obj=None):
        if obj:
            return ['id']
        else:
            return []
    
@admin.register(DataStore)
class DataStoreAdmin(admin.ModelAdmin):
    actions =None
    list_display = ['data_store_name','database_ip','database_host','database_port']
    readonly_fields = ('data_store_name',)
    search_fields = ['data_store_name']

    def has_delete_permission(self, request, obj=None):
        return False
    
    def get_readonly_fields(self, request, obj=None):
        if obj:
            return ['data_store_name']
        else:
            return []
    

    
@admin.register(ETLTransaction)
class ETLTransactionAdmin(admin.ModelAdmin):
    actions = None
    list_display = ['id','etl_datetime','data_source_id','completely','is_load_all']
    search_fields = ['data_source_id','etl_datetime']
    def has_delete_permission(self, request, obj=None):
        return False
    def has_add_permission(self, request, obj=None):
        return False
    def has_change_permission(self, request, obj=None):
        return  False

@admin.register(LogError)
class LogErrorAdmin(admin.ModelAdmin):
    actions = None
    list_display = ['id','error_datetime','etl_datetime','data_source_id']
    search_fields = ['error_datetime','etl_datetime','data_source_id']
    def has_delete_permission(self, request, obj=None):
        return False
    def has_add_permission(self, request, obj=None):
        return False
    def has_change_permission(self, request, obj=None):
        return  False

