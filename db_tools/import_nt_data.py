import os,sys

pwd = os.path.dirname(os.path.realpath(__file__))
sys.path.append(pwd+"/../")
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'AnlsTool.settings')

from openpyxl import load_workbook
from datetime import datetime
import django
django.setup()

from NormalTask.models import ParentTask, Version, Task, ExecFunction, Input, InputFileSheet, InputFileColumn, OutputSheet, OutputColumn, SqlCode

#获取管理员用户，创建子任务时提出者与开发者默认使用管理员
from django.contrib.auth import get_user_model
User = get_user_model()
sse = User.objects.filter(is_superuser=1)[0]

#初始化ExecFunction
exec_info = [{'exec_id':'1','chinese_name':'elk查询函数(公共服务)','func_name':'ELKQuery1','db_type':2},
             {'exec_id':'2','chinese_name':'elk更新函数(公共服务)','func_name':'ELKQuery2','db_type':2},
             {'exec_id':'3','chinese_name':'elk备端查询函数(postgre)','func_name':'PSGQuery','db_type':2},
             {'exec_id':'4','chinese_name':'elk备端更新函数(postgre)','func_name':'PSGUpd','db_type':2},
             {'exec_id':'5','chinese_name':'mysql查询函数','func_name':'MysqlQuery','db_type':6},
             {'exec_id':'6','chinese_name':'mysql更新函数','func_name':'MysqlUpd','db_type':6},
             {'exec_id':'7','chinese_name':'python解析器','func_name':'exec','db_type':0},
             {'exec_id':'8','chinese_name':'Oracle(仿真)','func_name':'ORCQuery','db_type':4},
             {'exec_id':'9','chinese_name':'Oracle(P01)','func_name':'ORCQueryP01','db_type':3},
             {'exec_id':'10','chinese_name':'elk主端查询函数','func_name':'PSGQuery_main','db_type':1}]
             
if len(ExecFunction.objects.all())==0:             
    for e in exec_info:
        ef = ExecFunction()
        ef.exec_id = e['exec_id']
        ef.name = e['chinese_name']
        ef.func_name = e['func_name']
        ef.db_type = e['db_type']
        ef.save()
    
#初始化Task,Input,InputFileSheet,InputFileColumn,OutputSheet,OutputColumn,SqlCode
tasklist = os.listdir('data')
for t_v in tasklist:
    tvlist = t_v.split('_')
    if len(tvlist)==2:
        t,v = tvlist
    else:
        t = tvlist[0]
        v = 1
    v = v.rstrip('.xlsx')
    version = Version()
    version.version_num = 1
    version.if_parent = False
    version.version_detail = '迁移自辅助分析工具'
    task = Task()
    task.title = t
    parenttask = ParentTask()
    parenttask.title = t
    parenttask.save()
    task.level = 2
    task.status = 4
    task.if_valid = 1
    task.creator = sse
    task.developer = sse
    task.create_time = datetime.now()
    task.save()
    version.son_task = task
    version.parent_task = parenttask
    version.save()
    wb = load_workbook('./data/'+t_v)
    ws = wb['输入配置']
    for i,row in enumerate(ws.values):
        if i == 0:
            continue
        input = Input()
        input.task = task
        input.name = row[0]
        input.replace_key = row[1]
        input.type = row[2]
        input.input_id = int(i)
        input.default_value = row[3]
        input.detail = row[4] if row[4] else ''
        input.save()
    ws = wb['输入文件配置']
    iss = {}
    for i,row in enumerate(ws.values):
        if i == 0:
            continue
        input_sheet = InputFileSheet()
        input_s = Input.objects.filter(name=str(row[0])).filter(task=task)[0]
        input_sheet.input = input_s
        input_sheet.sheet_id = row[1]
        input_sheet.name = row[2]
        ef = ExecFunction.objects.filter(exec_id=row[3])[0]
        input_sheet.exec_id = ef
        input_sheet.save()
        iss[int(row[1])] = input_sheet
    ws = wb['输入文件列名配置']
    for i,row in enumerate(ws.values):
        if i == 0:
            continue
        input_col = InputFileColumn()
        input_col.sheet = iss[int(row[0])]
        input_col.name = row[1]
        input_col.type = row[2]
        input_col.save()
    ws = wb['输出配置']
    oss = {}
    for i,row in enumerate(ws.values):
        if i == 0:
            continue
        osheet = OutputSheet()
        osheet.task = task
        osheet.sheet_output_id = row[0]
        osheet.name = row[1]
        osheet.save()
        oss[int(row[0])] = osheet
    ws = wb['输出字段信息']
    for i,row in enumerate(ws.values):
        if i == 0:
            continue
        col = OutputColumn()
        o_sheet = OutputSheet.objects.filter(sheet_output_id=int(row[0])).filter(task=task)[0]
        col.name = row[1]
        col.replace_key = row[2]
        col.detail = row[3] if row[3] else ''
        col.sheet = oss[int(row[0])]
        col.save()
    ws = wb['SQL代码信息']
    for i,row in enumerate(ws.values):
        if i == 0:
            continue
        code = SqlCode()
        code.task = task
        ef = ExecFunction.objects.filter(exec_id=row[0])[0]
        code.exec_id = ef
        code.file_type = row[1]
        code.display = row[2]
        code.code = row[3]
        code.sql_id = i
        code.save()
    
