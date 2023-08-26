import json

from .models import File, ParentTask, Version, Task, ExecFunction, Input, InputFileSheet, InputFileColumn, OutputSheet, OutputColumn, SqlCode, JobHistory, ChangeHistory, QueueChangeRecord
from rest_framework import serializers
from Feedback.models import FeedRecord

class FileSerializer(serializers.ModelSerializer):
    class Meta:
        model = File
        fields = "__all__"

class ParentTaskSimpleSerializer(serializers.ModelSerializer):
    class Meta:         
        model = ParentTask
        fields = "__all__"

class ParentTaskQueueSerializer(serializers.ModelSerializer):
    if_modity = serializers.SerializerMethodField()
    reason = serializers.SerializerMethodField()

    def get_if_modity(self, obj):
        l = QueueChangeRecord.objects.filter(model_name='ParentTask').filter(model_id=obj.id)
        if len(l)>0:
            return True
        else:
            return False

    def get_reason(self, obj):
        l = QueueChangeRecord.objects.filter(model_name='ParentTask').filter(model_id=obj.id).order_by('-change_time')
        if len(l)>0:
            return l[0].change_reason 
        else:
            return ''

    class Meta:         
        model = ParentTask
        fields = "__all__"

class ParentTaskSerializer(serializers.ModelSerializer):
    latest_task = serializers.SerializerMethodField()
    diff_task = serializers.SerializerMethodField()
    if_feed = serializers.SerializerMethodField()

    def get_if_feed(self, obj):
        l = FeedRecord.objects.filter(demind_seq=obj.seq).filter(if_feed=False)
        if len(l)>0:
            return True
        return False

    def get_latest_task(self, obj):
        version_list = Version.objects.filter(parent_task=obj)
        t = [version for version in version_list if version.if_exec ==True] 
        if(len(t)==0):
            return []
        else:
            v_d = {}
            max_task = {}
            for v in t:
                l = v_d.get(v.version_id, -1)
                if l < v.version_num:
                    v_d[v.version_id] = v.version_num
                    max_task[v.version_id] = {'id':v.son_task.id,'name':v.son_task.title,'demand_id':v.parent_task.id}
            res = []
            for k in max_task.keys():
                res.append({'name':max_task[k]['name'],'version_id':k,'task_id':max_task[k]['id'],'demand_id':max_task[k]['demand_id']})
            return res
    
    def get_diff_task(self, obj):
        version_list = Version.objects.filter(parent_task=obj)
        t = [version for version in version_list ] 
        if(len(t)==0):
            return []
        else:
            v_d = {}
            max_task = {}
            for v in t:
                l = v_d.get(v.version_id, -1)
                if l < v.version_num:
                    v_d[v.version_id] = v.version_num
                    max_task[v.version_id] = {'id':v.son_task.id,'name':v.son_task.title,'demand_id':v.parent_task.id}
            res = []
            for k in max_task.keys():
                res.append({'name':max_task[k]['name'],'version_id':k,'task_id':max_task[k]['id'],'demand_id':max_task[k]['demand_id']})
            return res
        
    class Meta:
        model = ParentTask
        fields = "__all__"
        
class VersionSerializer(serializers.ModelSerializer):
    # latest_version = serializers.SerializerMethodField()
    # def get_latest_version(self, obj):
        # version_list = Version.objects.filter(parent_task=obj.parent_task)
        # latest_version = max([version.version_num for version in version_list])
        # return latest_version
    parent_name = serializers.SerializerMethodField()
    son_name = serializers.SerializerMethodField()
    son_type = serializers.SerializerMethodField()
    
    def get_parent_name(self, obj):
        parent_name = obj.parent_task.title
        return parent_name
    
    def get_son_name(self, obj):
        son_name = obj.son_task.title
        return son_name
    
    def get_son_type(self, obj):
        son_type = obj.son_task.type
        return son_type
    
    class Meta:
        model = Version
        fields = "__all__"

class TaskSerializer(serializers.ModelSerializer):
    developer_name = serializers.SerializerMethodField()

    def get_developer_name(self, obj):
        return obj.developer.full_name

    class Meta:
        model = Task
        fields = "__all__"

class TaskQueueSerializer(serializers.ModelSerializer):
    
    if_modity = serializers.SerializerMethodField()
    reason = serializers.SerializerMethodField()
    demand_id = serializers.SerializerMethodField()
    version_id = serializers.SerializerMethodField()


    def get_if_modity(self, obj):
        l = QueueChangeRecord.objects.filter(model_name='Task').filter(model_id=obj.id)
        if len(l)>0:
            return True
        else:
            return False

    def get_reason(self, obj):
        l = QueueChangeRecord.objects.filter(model_name='Task').filter(model_id=obj.id).order_by('-change_time')
        if len(l)>0:
            return l[0].change_reason 
        else:
            return ''

    def get_demand_id(self, obj):
        version_l = Version.objects.filter(son_task=obj)
        demand_id = [ version.parent_task.id for version in version_l]
        return demand_id
    
    def get_version_id(self, obj):
        version_l = Version.objects.filter(son_task=obj)        
        version_id = [version.version_id for version in version_l]
        return version_id

    class Meta:
        model = Task
        fields = "__all__"

class TaskDevelopSerializer(serializers.ModelSerializer):
    demand_id = serializers.SerializerMethodField()
    version_id = serializers.SerializerMethodField()
    
    def get_demand_id(self, obj):
        version_l = Version.objects.filter(son_task=obj)
        demand_id = [ version.parent_task.id for version in version_l]
        return demand_id
    
    def get_version_id(self, obj):
        version_l = Version.objects.filter(son_task=obj)        
        version_id = [{'version_id':version.version_id,'demand_id':version.parent_task.id}  for version in version_l]
        return version_id
    
    class Meta:
        model = Task
        fields = "__all__"
        
        
class ExecFuncSerializer(serializers.ModelSerializer):
    class Meta:
        model = ExecFunction
        fields = "__all__"

class InputSerializer(serializers.ModelSerializer):
    class Meta:
        model = Input
        fields = "__all__"
        

class InputFileSheetSerializer(serializers.ModelSerializer):
    columns = serializers.SerializerMethodField()
    
    def get_columns(self, obj):
        columns = InputFileColumn.objects.filter(sheet=obj)
        columns_serializer = InputFileColumnSerializer(columns, many=True)
        return columns_serializer.data
    
    class Meta:
        model = InputFileSheet
        fields = "__all__" 

class InputFileSimpleSerializer(serializers.ModelSerializer):
    class Meta:
        model = InputFileSheet
        fields = "__all__"           
        
class InputFileColumnSerializer(serializers.ModelSerializer):
    class Meta:
        model = InputFileColumn
        fields = "__all__"
        
        
class OutputColumnSerializer(serializers.ModelSerializer):
    class Meta:
        model = OutputColumn
        fields = "__all__"

class OutputSheetSerializer(serializers.ModelSerializer):
    columns = serializers.SerializerMethodField()
    
    def get_columns(self, obj):
        columns = OutputColumn.objects.filter(sheet=obj).order_by('id')
        columns_serializer = OutputColumnSerializer(columns, many=True)
        return columns_serializer.data
    
    class Meta:
        model = OutputSheet
        fields = "__all__"
        
class OutputSimpleSerializer(serializers.ModelSerializer):    
    class Meta:
        model = OutputSheet
        fields = "__all__"
        
class SqlCodeSerializer(serializers.ModelSerializer):
    class Meta:
        model = SqlCode
        fields = "__all__"
        
class JobHistorySerializer(serializers.ModelSerializer):
    use_time = serializers.SerializerMethodField()
    user_name = serializers.SerializerMethodField()
    task_name = serializers.SerializerMethodField()
    input_param = serializers.SerializerMethodField()
    input_file_json = serializers.SerializerMethodField()
    res_file_json = serializers.SerializerMethodField()
    create_time_mod = serializers.SerializerMethodField()
    exec_status_zh = serializers.SerializerMethodField()
    
    def get_use_time(self, obj):
        return int((obj.renew_time - obj.create_time).seconds)
    
    def get_user_name(self, obj):
        return str(obj.user.full_name)
    
    def get_task_name(self, obj):
        return str(obj.task.title)
    
    def get_input_param(self, obj):
        input_json = json.loads(obj.input_json)
        input_json_key = sorted(input_json)[:4]
        return str(','.join([input_json[key] for key in input_json_key]))
        
    def get_input_file_json(self, obj):
        input_file_json =  json.loads(obj.input_file)
        return input_file_json
    
    def get_res_file_json(self, obj):
        res_file_json = json.loads(obj.res_file)
        return res_file_json
    
    def get_create_time_mod(self, obj):
        create_time_mod = obj.create_time.strftime('%Y/%m/%d %H:%M:%S')
        return create_time_mod
    
    def get_exec_status_zh(self, obj):
        if int(obj.exec_status) == 0:
            exec_status_zh = '查询中'
        elif int(obj.exec_status) == 1:
            exec_status_zh = '执行成功'
        elif int(obj.exec_status) == 2:
            exec_status_zh = 'sql执行失败'
        elif int(obj.exec_status) == 3:
            exec_status_zh = 'python执行失败'
        elif int(obj.exec_status) == 4:
            exec_status_zh = '临时表插入失败'
        return exec_status_zh
        
    class Meta:
        model = JobHistory
        fields = '__all__'
        # fields = (
        # 'user','user_name','task','task_name','create_time_mod','use_time','input_param','input_file_json','sql_done','sql_all','exec_status_zh','res_file_json'
        # )

class ChangeHistorySerializer(serializers.ModelSerializer):
    add_remove_file = serializers.SerializerMethodField()
    def get_add_remove_file(self, obj):
        if(obj.model_name=='ParentTask' and (obj.field_name=='requirment_files' or obj.field_name=='reason_file')):
            sig = ','
            new_set = set(obj.new.split(sig))
            old_set = set(obj.old.split(sig))
            res_list = []
            add_file_list = new_set - old_set
            if len(add_file_list)>0:
                for add_file in add_file_list:
                    if len(str(add_file))>0:
                        res_list.append('+'+str(add_file))
            remove_file_list  = old_set - new_set
            if len(remove_file_list)>0:
                for remove_file in remove_file_list:
                    if len(str(remove_file))>0:
                        res_list.append('-'+str(remove_file)) 
            return res_list
        else:
            return []

    class Meta:
        model = ChangeHistory
        fields = "__all__"
