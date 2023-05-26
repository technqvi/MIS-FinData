from django.contrib import admin
from .models import  *
# Register your models here.

@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    actions =None
    readonly_fields = ('id',)
    search_fields = ['id']

    def has_delete_permission(self, request, obj=None):
        return False
    
    def get_readonly_fields(self, request, obj=None):
        if obj:
            return ['id']
        else:
            return []
    

    
@admin.register(ETLTransaction)
class ETLTransactionAdmin(admin.ModelAdmin):
    actions = None
    list_display = ['id','etl_datetime','data_source','no_rows','is_load_all']
    search_fields = ['data_source','etl_datetime']
    def has_delete_permission(self, request, obj=None):
        return False
    def has_add_permission(self, request, obj=None):
        return False
    def has_change_permission(self, request, obj=None):
        return  False

@admin.register(LogError)
class LogErrorAdmin(admin.ModelAdmin):
    actions = None
    list_display = ['id','error_datetime','etl_datetime','data_source']
    search_fields = ['error_datetime','etl_datetime','data_source']
    def has_delete_permission(self, request, obj=None):
        return False
    def has_add_permission(self, request, obj=None):
        return False
    def has_change_permission(self, request, obj=None):
        return  False

