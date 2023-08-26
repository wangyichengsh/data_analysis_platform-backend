from django.contrib import admin

# Register your models here.
from .models import ParentTask, Version, Task, ExecFunction, Input, InputFileSheet, InputFileColumn, OutputSheet, OutputColumn, SqlCode

@admin.register(ParentTask)
class ParentTaskAdmin(admin.ModelAdmin):
    pass


@admin.register(Version)
class VersionAdmin(admin.ModelAdmin):
    pass

@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    pass
    
@admin.register(ExecFunction)
class ExecFunctionAdmin(admin.ModelAdmin):
    pass
    
@admin.register(Input)
class InputAdmin(admin.ModelAdmin):
    pass
    
@admin.register(InputFileSheet)
class InputFileSheetAdmin(admin.ModelAdmin):
    pass
    
@admin.register(InputFileColumn)
class InputFileColumnAdmin(admin.ModelAdmin):
    pass
    
@admin.register(OutputSheet)
class OutputSheetAdmin(admin.ModelAdmin):
    pass
    
@admin.register(OutputColumn)
class OutputColumnAdmin(admin.ModelAdmin):
    pass
    
@admin.register(SqlCode)
class SqlCodeAdmin(admin.ModelAdmin):
    pass
    
