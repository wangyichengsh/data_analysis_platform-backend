import json, os, time
from multiprocessing import Process, RLock
import mimetypes
from datetime import datetime
import logging

import django
from .models import File2Task, File, ParentTask, Version, Task, ExecFunction, Input, InputFileSheet, InputFileColumn, OutputSheet, OutputColumn, SqlCode, JobHistory, ChangeHistory, QueueChangeRecord
from .serializers import ParentTaskSerializer, VersionSerializer, TaskSerializer, TaskDevelopSerializer, ExecFuncSerializer, InputSerializer, InputFileSheetSerializer, InputFileSimpleSerializer, InputFileColumnSerializer, OutputSheetSerializer, OutputSimpleSerializer,OutputColumnSerializer, SqlCodeSerializer, JobHistorySerializer, \
FileSerializer, ChangeHistorySerializer, ParentTaskSimpleSerializer, ParentTaskQueueSerializer, TaskQueueSerializer
from rest_framework import viewsets, mixins
from rest_framework.views import APIView
from rest_framework import generics
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework import filters
from django.http import HttpResponse
import django_filters.rest_framework
from rest_framework.authentication import TokenAuthentication
import django_filters
from django.contrib.auth import get_user_model
from django.db import close_old_connections,connection
from django.db.models import DateField, DateTimeField, sql,Max,Q

User = get_user_model()

from .common import getInputByTask, getJobDone, createExecHistory, xl2db, createversion, renewversion, db2xl, db2json, xl2output, xl2twmpsql, copyDemandTask, send_mail,  getAllExecSql
from .filter import FileFilter
from openpyxl import Workbook, load_workbook
from .dev_queue import queue2list, initqueue, do_modify, del_modify

# 上传文件目前未将用户信息传进来
from rest_framework.permissions import AllowAny 

tech_group_mail = ['jywan@ssein.com.cn','huahuang2@ssein.com.cn','cqhuang@ssein.com.cn' \
                  ,'jzhang2@ssein.com.cn','yzheng@ssein.com.cn','yuhezhang@ssein.com.cn','yswang@ssein.com.cn'] 
# tech_group_mail = ['jzhang2@ssein.com.cn','yzheng@ssein.com.cn','yuhezhang@ssein.com.cn','yswang@ssein.com.cn'] 

logger = logging.getLogger('main.NormalTask_views')

class NormPagination(PageNumberPagination):
    '''
    接口默认分页设置
    '''
    page_size = 10
    page_size_query_param = 'page_size'
    page_query_param = 'page'

class FileViewSet(mixins.ListModelMixin,viewsets.GenericViewSet):
    queryset = File.objects.all()
    serializer_class = FileSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filter_class = FileFilter
    # filterset_fields = ['id']
    lookup_field = 'id'
    
class ParentTaskSimpleViewSet(mixins.ListModelMixin, mixins.RetrieveModelMixin,mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):    
    queryset = ParentTask.objects.all()
    serializer_class = ParentTaskSimpleSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['id']
    lookup_field = 'id'

class UnfinishedViewSet(mixins.ListModelMixin, mixins.RetrieveModelMixin,mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = ParentTask.objects.filter(~Q(status='finished') & ~Q(status='accepting') & ~Q(status='cancel') & ~Q(status='check') & ~Q(status='failed') ).order_by('-id')
    pagination_class = NormPagination
    serializer_class = ParentTaskSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend, filters.SearchFilter]
    filterset_fields = ['id','status','is_valid','create_by','priority']
    search_fields = ('title','seq', )

class ParentTaskViewSet(mixins.ListModelMixin, mixins.RetrieveModelMixin,mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = ParentTask.objects.all().order_by('-id')
    pagination_class = NormPagination
    serializer_class = ParentTaskSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['id','status','is_valid','create_by','priority']
    ordering_fields = ('seq','status','create_by','create_time','type','priority')
    search_fields = ('title','seq', )

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)

        if getattr(instance, '_prefetched_objects_cache', None):
            # If 'prefetch_related' has been applied to a queryset, we need to
            # forcibly invalidate the prefetch cache on the instance.
            instance._prefetched_objects_cache = {}
        
        # 定制化内容
        initqueue('ParentTask')
        if('priority' in request.data.keys()):
            if str(request.data['priority']) == '1' or  str(request.data['priority']) == '0':
               mail_str = '''特急需求"%(title)s"( http://193.168.42.243:60000/demandDetail?demandId=%(id)s ) 发生了变更。

----------------------------------------
该邮件为系统自动生成，请勿回复。'''
               meta = {'title':request.data['title'],'desc':instance.desc,'reason':instance.reason,'deadline':instance.deadline,'id':instance.id}
               body = mail_str % meta
               subject = '特急需求变更通知'
               send_mail(to=tech_group_mail,cc=[],subject=subject,body=body)

        return Response(serializer.data)   

 
    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())

        param = request.query_params
        if 'all' in param.keys():
            all_flag = param['all']
        else:
            all_flag = 'true'
        if all_flag == 'false':
            queryset = queryset.filter(~Q(status='check') & ~Q(status='failed'))
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

class VersionViewSet(mixins.ListModelMixin, mixins.RetrieveModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = Version.objects.all().order_by('-version_num')
    serializer_class = VersionSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['id','parent_task','son_task','version_id','demand_seq']

class TaskViewSet(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = Task.objects.all().order_by('-create_time')
    pagination_class = NormPagination
    serializer_class = TaskSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend, filters.SearchFilter]
    filterset_fields = ['id','if_valid','status','if_model']
    search_fields = ('title', )
    
class ExecFuncViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    queryset = ExecFunction.objects.all()
    serializer_class = ExecFuncSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['exec_id']
    
class InputViewSet(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = Input.objects.all().order_by('input_id')
    serializer_class = InputSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['id','task']
    lookup_field = 'id'

class InputFileSheetViewSet(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = InputFileSheet.objects.all()
    serializer_class = InputFileSheetSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['input']
    lookup_field = 'id'

class InputFileSimpleViewSet(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = InputFileSheet.objects.all()
    serializer_class = InputFileSimpleSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['input']
    lookup_field = 'id'

class InputFileColumnViewSet(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = InputFileColumn.objects.all()
    serializer_class = InputFileColumnSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['sheet']
    lookup_field = 'id'
    
class OutputSheetViewSet(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = OutputSheet.objects.all().order_by('sheet_output_id')
    serializer_class = OutputSheetSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['id','task']
    lookup_field = 'id'
    
class OutputSimpleViewSet(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = OutputSheet.objects.all().order_by('sheet_output_id')
    serializer_class = OutputSimpleSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['id','task']
    lookup_field = 'id'
    
class OutputColumnViewSet(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = OutputColumn.objects.all().order_by('id')
    serializer_class = OutputColumnSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['id','sheet']
    lookup_field = 'id'
    
class SqlCodeViewSet(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = SqlCode.objects.all().order_by('sql_id')
    serializer_class = SqlCodeSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['task']
    lookup_field = 'id'
    
    def options(self, request, parentTask_id, format=None):
        response = Response()
        response['Access-Control-Allow-Method'] = "POST, OPTIONS, GET, PUT, DELETE"
        response['Access-Control-Allow-Origin'] = "*"
        response['Access-Control-Allow-Headers'] = "Content-Type,Cookie,Authorization"
        return response
        
class JobHistoryViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    authentication_classes = (TokenAuthentication,)
    queryset = JobHistory.objects.all().order_by('-create_time')
    pagination_class = NormPagination
    serializer_class = JobHistorySerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['user', 'task']
    
    def list(self, request, *args, **kwargs):
        user = request.user
        param = request.query_params
        queryset = self.get_queryset()
        if('start_time' in param.keys()):
            if(len(str(param['start_time']))>0):
                queryset = queryset.filter(create_time__gte=param['start_time'])
        if('end_time' in param.keys()):
            if(len(str(param['end_time']))>0):
                queryset = queryset.filter(create_time__lte=param['end_time'])
        if('id' in param.keys()):
            if(len(str(param['id']))>0):
                queryset = queryset.filter(id=param['id'])
        if(user.is_superuser):
            if('user' in param.keys()):
                if(len(str(param['user']))>0):
                    queryset = queryset.filter(user=param['user'])
        else:
            queryset = queryset.filter(user=user)
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)
        
class CreateVersionView(APIView):
    """
    创建版本
    """
    def post(self, request, parentTask_id, format=None):
        if request.user.is_authenticated:
            create_user = request.user
            param  = request.data
            res = createversion(parentTask_id, param, create_user)
            if len(res) == 0:
                return Response({'status':'success','msg':''}, content_type="application/json")
            else:
                return Response({'status':'Error','msg':str(res)}, content_type="application/json")
        else:
            return Response({'status':'Error','msg':'Can not find user'}, content_type="application/json")

class RenewVersionView(APIView):
    """
    更新版本信息
    """
    def post(self, request, parentTask_id, format=None):
        if request.user.is_authenticated:
            param  = request.data
            res = renewversion(parentTask_id, param)
            if len(res) == 0:
                return Response({'status':'success','msg':''}, content_type="application/json")
            else:
                return Response({'status':'Error','msg':str(res)}, content_type="application/json")
        else:
            return Response({'status':'Error','msg':'Can not find user'}, content_type="application/json")

class DeleteVersionView(APIView):
    """
    删除版本
    """
    def post(self, request, format=None):
        if request.user.is_authenticated:
            create_user = request.user
            param  = request.data
            task_id = param.get('task_id')
            try:
                task = Task.objects.get(id=task_id)
                task.if_valid = False
                task.save()
                version = Version.objects.get(son_task=task)
                version.delete()
                return Response({'status':'success','msg':''}, content_type="application/json")
            except Exception as e:
                return Response({'status':'Error','msg':str(e)}, content_type="application/json")
        else:
            return Response({'status':'Error','msg':'Can not find user'}, content_type="application/json")

class ConfigView(APIView):
    """
    导出配置文件excel
    """    
    def get(self, request, task_id, format=None):
        res = db2xl(task_id)
        stream = res['stream']
        filename = res['filename']
        if len(res['msg']) == 0:
            mimetype = mimetypes.guess_type(filename)[0]
            response = HttpResponse(stream)            
            # response['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml'
            response['Content-Type'] = mimetype
            response['Content-Disposition'] = "attachment; filename={}".format(filename.encode().decode('latin-1'))
            # response.write(stream)
            return response
        else:
            return Response({'status':'Error','msg':str(res['msg'])}, content_type="application/json")
            
class ConfigPickleView(APIView):
    """
    导出配置文件pickle
    """    
    def get(self, request, task_id, format=None):
        res = db2json(task_id,Pickle=True)
        stream = res['stream']
        filename = res['filename']
        if len(res['msg']) == 0:
            mimetype = mimetypes.guess_type(filename)[0]
            response = HttpResponse(stream)            
            # response['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml'
            response['Content-Type'] = mimetype
            response['Content-Disposition'] = "attachment; filename={}".format(filename.encode().decode('latin-1'))
            # response.write(stream)
            return response
        else:
            return Response({'status':'Error','msg':str(res['msg'])}, content_type="application/json")

class ConfigUpdateView(APIView):
    """
    更新配置文件
    """
    def options(self, request, task_id, format=None):
        response = Response()
        response['Access-Control-Allow-Method'] = "POST, OPTIONS"
        response['Access-Control-Allow-Origin'] = "*"
        response['Access-Control-Allow-Headers'] = "Content-Type,Cookie,Authorization"
        return response
        
    def post(self, request, task_id,format=None):
        if request.user.is_authenticated:
            user = request.user
            if int(user.is_superuser)==1:
                file_param = request.data
                file_stream = file_param['config_file']
                file_type = str(file_stream.name).split('.')[-1]
                if(file_type=='xlsx' or file_type=='xls'):
                    res = xl2db(task_id, file_stream, mode='update')
                else:
                    res = xl2db(task_id, file_stream.read(), mode='update',stream_type='pickle')
                if len(res) == 0:
                    return Response({'status':'success','msg':''}, content_type="application/json")
                else:
                    return Response({'status':'Error','msg':str(res)}, content_type="application/json")
            else:
                return Response({'status':'Error','msg':'Can not find user'}, content_type="application/json")
        else:
            return Response({'status':'Error','msg':'Can not find user'}, content_type="application/json")
            
class OutputRenewView(APIView):
    """
    更新输出参数
    """
    def options(self, request, task_id, format=None):
        response = Response()
        response['Access-Control-Allow-Method'] = "POST, OPTIONS"
        response['Access-Control-Allow-Origin'] = "*"
        response['Access-Control-Allow-Headers'] = "Content-Type,Cookie,Authorization"
        return response
        
    def post(self, request, task_id,format=None):
        if request.user.is_authenticated:
            file_param = request.data
            file_stream = file_param['config_file']
            res = xl2output(task_id, file_stream)
            if len(res) == 0:
                return Response({'status':'success','msg':''}, content_type="application/json")
            else:
                return Response({'status':'Error','msg':str(res)}, content_type="application/json")
        else:
            return Response({'status':'Error','msg':'Can not find user'}, content_type="application/json")
        
class ConfigRenewView(APIView):
    """
    删除更新配置文件
    """
    def options(self, request, task_id, format=None):
        response = Response()
        response['Access-Control-Allow-Method'] = "POST, OPTIONS"
        response['Access-Control-Allow-Origin'] = "*"
        response['Access-Control-Allow-Headers'] = "Content-Type,Cookie,Authorization"
        return response
    
    def post(self, request, task_id,format=None):
        if request.user.is_authenticated:
            user = request.user
            if user.is_staff:
                file_param = request.data
                file_stream = file_param['config_file']
                file_type = str(file_stream.name).split('.')[-1]
                if(file_type=='xlsx' or file_type=='xls'):
                    res = xl2db(task_id, file_stream, mode='renew')
                else:
                    res = xl2db(task_id, file_stream.read(), mode='renew',stream_type='pickle')
                if len(res) == 0:
                    return Response({'status':'success','msg':''}, content_type="application/json")
                else:
                    return Response({'status':'Error','msg':str(res)}, content_type="application/json")
            else:
                return Response({'status':'Error','msg':'Can not find user'}, content_type="application/json")
        else:
            return Response({'status':'Error','msg':'Can not find user'}, content_type="application/json")
        
class ExecTaskView(APIView):
    """
    执行代码返回结果
    """
    def options(self, request, task_id, format=None):
        response = Response()
        response['Access-Control-Allow-Method'] = "POST, OPTIONS"
        response['Access-Control-Allow-Origin'] = "*"
        response['Access-Control-Allow-Headers'] = "Content-Type,Cookie,Authorization"
        return response
        
    def post(self, request, task_id, format=None):
        if request.user.is_authenticated:
            user = request.user
            user_id = user.id
            import uuid
            uid = str(uuid.uuid4())
            task = Task.objects.get(id=task_id)
            input_param = request.data
            job_id = createExecHistory(task,input_param,user_id,uid)
            lock = RLock()
            t = Process(target=getJobDone, args=(task,input_param,user_id,uid,job_id,lock,))
            t.start()
            close_old_connections()
            lock.acquire()
            jh = JobHistory.objects.get(id = job_id)
            print(jh)
            print('进程号:'+str(t.pid))
            jh.pid = int(t.pid)
            jh.save()
            lock.release()
            print('记录id:'+str(jh.id)+' 创建时间:'+str(jh.create_time)+' 任务id:'+str(jh.task_id)+' 用户id:'+str(jh.user_id)+' pid:'+str(t.pid))
            return Response({'status':'sucess','msg':''}, content_type="application/json")
        else:
            return Response({'status':'Error','msg':'Can not find user'}, content_type="application/json")

class TempTableSqlView(APIView):
    """
    返回临时表建表及插入语句
    """
    def post(self, request, input_id, format=None):
        import uuid
        uid = str(uuid.uuid4())
        xlsx_file = request.data['param_file']
        sql_text = xl2twmpsql(input_id,xlsx_file)
        response = HttpResponse(sql_text)
        response['Content-Type'] = 'text/plain'
        return response

class ExecSqlTestView(APIView):
    """
    返回所有sql语句
    """
    def post(self, request, task_id, format=None):
        import uuid
        uid = str(uuid.uuid4())
        task = Task.objects.get(id=task_id)
        input_param = request.data
        sql_text = getAllExecSql(task, input_param, uid)
        response = HttpResponse(sql_text)
        response['Content-Type'] = 'text/plain'
        return response

        
class FileUpLoadView(APIView):
    permission_classes = (AllowAny,)
    """
    上传文件接收接口
    """
    def post(self, request, format=None):
        file = File()
        upload_file = request.data['file']
        file.type = request.data['type']
        file.file_name = upload_file.name
        file.upload_time = datetime.now()
        hz = file.file_name.strip('.')[-1]
        filename = upload_file.name.split('.')
        filename[-2] += str(int(time.time())*1000)
        filename = '.'.join(filename)
        if not os.path.exists('static'):
            os.mkdir('static')
        if file.type == 'req_file':
            sec_path = 'demand'
        elif file.type == 'req_reason_file':
            sec_path = 'reason'
        elif file.type == 'bug_file':
            sec_path = 'bug'
        elif file.type == 'res_file':
            sec_path = 'res'
        if not os.path.exists('static/'+sec_path):
            os.mkdir('static/'+sec_path)
        file_path = 'static/'+sec_path+'/'+filename
        file.file_path = file_path
        with open(file_path,'wb') as f:
            f.write(upload_file.read())
        file.save()
        if file.type == 'res_file':
            task_id = request.data['task']
            task = Task.objects.get(id=task_id)
            f2t = File2Task()
            f2t.task = task
            f2t.file = file
            f2t.save()
        return Response({'id':int(file.id),'path':file_path,'raw_name':upload_file.name,'msg':''}, content_type="application/json")

class DemandStatusChange(APIView):
    """
    需求状态变更
    """
    def post(self, request, demand_id, format=None):
        try:
            param = request.data
            user = request.user
            status = param.get('status','wait')
            demand = ParentTask.objects.get(id=demand_id)
            demand.status = status
            if status == 'cancel':
                demand.canceled_time = datetime.now()
                demand.canceled_by = user
            elif status =='finished':
                demand.closed_time = datetime.now()
                demand.closed_by = user
            elif status == 'accepting':
                body = str(demand.create_by.full_name)+''':

    您的需求“'''+str(demand.title)+'''”(http://193.168.42.243:60000/demandDetail?demandId='''+str(demand_id)+''' )已经完成。请前往验收。 (我的需求 http://193.168.42.243:60000/taskList )验收通过后请关闭该需求。

----------------------------------------
该邮件为系统自动生成，请勿回复。'''
                subject = '“'+str(demand.title)+'”已完成'
                to_email = str(demand.create_by.username)+'@ssein.com.cn'
                try:
                    send_mail(to=[to_email],cc=['jzhang2@ssein.com.cn'],subject=subject,body=body)
                except:
                    pass
            elif status == 'conforming':
                body = str(demand.create_by.full_name)+''':

    技术人员对于您的需求“'''+str(demand.title)+'''”(http://193.168.42.243:60000/demandDetail?demandId='''+str(demand_id)+''' )有疑问。请前往解答。

----------------------------------------    
该邮件为系统自动生成，请勿回复。 '''
                subject = '"'+str(demand.title)+'"待确认'
                to_email = str(demand.create_by.username)+'@ssein.com.cn'
                try:
                    send_mail(to=[to_email],cc=['jzhang2@ssein.com.cn'],subject=subject,body=body)
                except:
                    pass
            demand.update_time = datetime.now()
            demand.save()
            initqueue('ParentTask')
            return Response({'status':'success','msg':'','id':demand.id}, content_type="application/json")
        except Exception as e:
            return Response({'status':'fail','msg':str(e),'id':-1}, content_type="application/json")
        
class DemandView(APIView):
    """
    需求相关接口
    """
    def post(self, request, format=None):
        try:
            param = request.data
            user = request.user
            demand = ParentTask()
            if 'seq' not in param.keys():
                seq = int(demand.get_last_seq())+1
            else:
                seq = param['seq']
            demand.seq = seq
            demand.title = param['title']
            demand.type = param['type']
            demand.priority = param['priority']
            demand.desc = param['desc']
            demand.reason = param['reason']
            if('deadline' in param.keys()):
                demand.deadline = datetime.strptime(param['deadline'],'%Y-%m-%d')
            if not "status" in param.keys():
                demand.status = 'wait'
            else:
                demand.status = param['status']
            if not 'create_by' in param.keys():
                demand.create_by = user
            else:
                creator =  User.objects.get(id=param['create_by'])
                demand.create_by = creator
            Now = datetime.now()
            demand.create_time = Now
            demand.update_time = Now
            demand.save()
            demand.requirment_files.set(File.objects.filter(id__in=param['requirment_files']))
            demand.reason_file.set(File.objects.filter(id__in=param['reason_file']))
            demand.save()
            initqueue('ParentTask')
            if str(demand.priority) == '1' or str(demand.priority) == '0':
               mail_str = '''%(full_name)s 创建了特急需求"%(title)s"( http://193.168.42.243:60000/demandDetail?demandId=%(id)s )

----------------------------------------
该邮件为系统自动生成，请勿回复。'''
               meta = {'full_name':user.full_name,'title':demand.title,'desc':demand.desc,'reason':demand.reason,'deadline':demand.deadline,'id':demand.id}
               body = mail_str % meta
               subject = '特急需求'
               send_mail(to=tech_group_mail,cc=[],subject=subject,body=body)
            return Response({'status':'success','msg':'','id':demand.id}, content_type="application/json")
        except Exception as e:
            return Response({'status':'fail','msg':str(e),'id':-1}, content_type="application/json")
            
class SonTaskView(APIView):
    """
    管理子任务
    """
    def get(self,request,format=None):
        param = request.query_params
        demand_id = param['demand_id']
        demand = ParentTask.objects.get(id=demand_id)
        version_list = Version.objects.filter(parent_task=demand)
        t = [version for version in version_list ]
        res = []
        if(len(t)==0):
            return Response(res, content_type="application/json")
        else:
            v_d ={}
            max_task ={}
            for v in t:
                l = v_d.get(v.version_id, -1)
                if l < v.version_num:
                    v_d[v.version_id] = v.version_num
                    max_task[v.version_id] = v
            for k in max_task.keys():
                res.append(max_task[k])
            serializer = VersionSerializer(res, many=True)
            return Response(serializer.data)
        
            
    
    def post(self, request, format=None):
        try:
            param = request.data
            user = request.user
            task = Task()
            task.title = param['title']
            task.level = param['level']
            task.type = param['type']
            task.status = param['status']
            task.desc = param['desc']
            task.developer = User.objects.get(id=param['developer'])
            task.if_model = param.get('if_model',False)
            task.if_valid = True
            task.creater = user
            task.create_time = datetime.now()
            task.update_time = datetime.now()
            task.assign_time = datetime.now()
            task.save()
            return Response({'status':'success','msg':'','id':task.id}, content_type="application/json")
        except Exception as e:
            return Response({'status':'fail','msg':str(e),'id':-1}, content_type="application/json") 
            
class ResFileView(APIView):
    """
    结果文件
    """
    def get(self,request,format=None):
        param = request.query_params
        task_id = param['task_id']
        task = Task.objects.get(id=task_id)
        res = []
        res_files = File2Task.objects.filter(task_id=task)
        for rel in res_files:
            file = File.objects.get(id=rel.file_id)
            res.append(file)
        serializer = FileSerializer(res, many=True)
        return Response(serializer.data)
        
    def delete(self,request,format=None):
        try:
            file_id = request.query_params['file_id']
            file = File.objects.get(id=file_id)
            res_files = File2Task.objects.filter(file_id=file)
            for file in res_files: 
                file.delete()
            return Response({'status':'success','msg':''}, content_type="application/json")
        except Exception as e:
            return Response({'status':'failed','msg':str(e)}, content_type="application/json")
    
class TaskInDevelopView(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = Task.objects.all().filter(if_valid=True).order_by('-update_time')
    pagination_class = NormPagination
    serializer_class = TaskDevelopSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend,  filters.SearchFilter]
    filterset_fields = ['id','status']
    search_fields = ('title', )
        
class ExtDemand(APIView):
    """
    将原需求对应的可执行任务copy到新需求下
    """
    def post(self, request, format=None):
        #try:
        param = request.data
        cp_from = param['copyFrom']
        cp_to = param['copyTo']
        copyDemandTask(cp_from, cp_to)
        initqueue('ParentTask')
        return Response({'status':'success','msg':''}, content_type="application/json")
        #except Exception as e:
            #return Response({'status':'failed','msg':str(e)}, content_type="application/json")
    
class ChangeHistoryViewSet(mixins.ListModelMixin, mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):
    queryset = ChangeHistory.objects.all().order_by('-change_time')
    serializer_class = ChangeHistorySerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['id','user','model_name','field_name','change_id']
    lookup_field = 'id'

class SeqFile(APIView):
    def get(self, request, seq_id, format=None):
        changes = ChangeHistory.objects.filter(change_id=seq_id).filter(model_name='ParentTask').filter(Q(field_name='requirment_files') | Q(field_name='reason_file'))
        l = []
        for i in changes:
            if(len(i.old)>0): 
                l.extend([int(k) for k in str(i.old).split(',')])
            if(len(i.new)>0):                
                l.extend([int(k) for k in str(i.new).split(',')])
        file = File.objects.filter(id__in=l)
        ser = FileSerializer(file,many=True)
        return Response(ser.data)
        
class DeveQueue(APIView):
    def get(self, request, format=None):
        param = request.query_params
        model_name = param['model_name']
        # initqueue(model_name,order_by='create_time')
        queue_list = queue2list(model_name , order_by='create_time')
        if model_name == 'ParentTask':
            ser = ParentTaskQueueSerializer(queue_list, many=True)
        elif model_name == 'Task':
            ser = TaskQueueSerializer(queue_list, many=True)
        return Response(ser.data)
        
    def post(self, request, format=None):
        param = request.data
        model_name = param['model_name']
        this_item = param['this_item']
        pre_item = param['pre_item']
        change_reason = param['change_reason']
        rec = QueueChangeRecord()
        rec.model_name = model_name
        rec.model_id = this_item
        rec.prev_id = pre_item
        rec.change_reason = change_reason
        rec.save()
        res = {'status':'success','msg':''}
        try:
            do_modify([{int(pre_item):int(this_item)}], model_name)
            initqueue(model_name)
        except Exception as e:
            res = {'status':'failed','msg':str(e)}
            return Response(res)
        else:
            return Response(res)
            
    def delete(self, request, format=None):
        param = request.query_params
        model_name = param['model_name']
        res = {'status':'success','msg':''}
        try:
            del_modify(model_name)
            QueueChangeRecord.objects.filter(model_name=model_name).delete()
            initqueue(model_name)
        except Exception as e:
            res = {'status':'failed','msg':str(e)}
            return Response(res)
        else:
            return Response(res)

############结果查看视图##############
def get_data_size(table_name):
    conn = connection
    sql_count = 'select count(1) from '+table_name + ';'
    cur = conn.cursor()
    try:
        cur.execute(sql_count)
    except:
        return 0
    d = cur.fetchall()
    cur.close()
    return d[0][0]

def get_page_data(table_name,page,size,order):
    conn = connection
    start = (int(page)-1) * int(size)
    if len(str(order)) >0:
        order_str = str(order)[1:]+' '
        if str(order).startswith('-'):
            order_str += 'desc'
        sql = '''SELECT * from
        %(table_name)s 
        ORDER BY %(order_str)s 
        offset %(start)d 
        limit %(size)d ; 
        ''' % {'table_name':table_name,'order_str':order_str,'start':start,'size':int(size)}
    else:
        sql = '''SELECT * from
        %(table_name)s 
        offset %(start)d 
        limit %(size)d ;
        ''' % {'table_name':table_name,'start':start,'size':int(size)}
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error('查询历史结果数据出错:'+str(e))
        logger.error(sql)
        return []
    data = cur.fetchall()
    rowname = [x[0] for x in cur.description]
    result = []
    for row in data:
        d = {}
        for i,col in enumerate(rowname):
            if isinstance(row[i],datetime):
                d[col] = row[i].strftime('%Y/%m/%d %H:%M:%S')
            else:
                d[col] = row[i]
        result.append(d)
    return result

class ShowResView(APIView):
    '''
    结果在线展示
    '''
    def post(self, request, format=None):
        if request.user.is_authenticated:
            param = request.data
            # param = request.query_params
            job_id = param['job_id']
            sheet_nu = param['sheet_nu']
            page = param['page']
            size = param['size']
            order = param['order']
            schema = django.conf.settings.RES_SCHEMA
            table_name = str(schema)+'.res_'+str(job_id)+'_'+str(sheet_nu)
            res = {}
            res['count'] = get_data_size(table_name)
            res['results'] = get_page_data(table_name,page,size,order)
            return Response(res, content_type="application/json")
        else:
            return Response({'status':'Error','msg':'Can not find user'}, content_type="application/json")

