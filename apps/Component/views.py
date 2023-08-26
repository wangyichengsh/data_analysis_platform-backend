from uuid import uuid4
from multiprocessing import Process, RLock
from rest_framework import filters
from rest_framework import viewsets,mixins
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.pagination import PageNumberPagination
import django_filters
from Component.models import DateCanlendar, AppExecuteHistory
from Component.serializers import DateCanlendarSerializer, AppExecuteHistorySerializer

from utils.viewset import ListModelApiMixin
from Component.exec import exec_script



class Pagination(PageNumberPagination):
    '''
    接口默认分页设置
    '''
    page_size = 15
    page_size_query_param = 'page_size'
    page_query_param = 'page'


# 日历表
class DateCanlendarViewSet(mixins.ListModelMixin,viewsets.GenericViewSet):
    queryset = DateCanlendar.objects.all()
    serializer_class = DateCanlendarSerializer
    filterset_fields = ['is_mkt_sh','is_week_end']


# 执行历史记录表
# class AppExecuteHistoryView(ListModelApiMixin,mixins.UpdateModelMixin,GenericAPIView):
class AppExecuteHistoryViewSet(ListModelApiMixin, mixins.UpdateModelMixin, viewsets.GenericViewSet):
# class AppExecuteHistoryViewSet(ListModelApiMixin, viewsets.ModelViewSet):
    queryset = AppExecuteHistory.objects.all()
    serializer_class = AppExecuteHistorySerializer

    filter_backends = [django_filters.rest_framework.DjangoFilterBackend, filters.SearchFilter]
    page = 15
    filterset_fields = ['remark']
    columns = [
        {'name': 'user_name', 'label': '执行人员'},
        {'name': 'execute_start_time', 'label': '开始时间'},
        {'name': 'execute_end_time', 'label': '结束时间'},
        {'name': 'remark', 'label': '备注'},
        {'name': 'exec_status', 'label': '操作'},
    ]

    def _get_queryset(self, request, *args, **kwargs):
        user = request.user
        app_id = request.query_params.get('app_id', None)
        order_by = request.query_params.get('order_by', None)
        if not app_id:
            app_id = request.data.get('app_id', None)
        self.queryset = self.queryset.filter(app_id=app_id)
        # 查看日志
        if user.has_perm('Component.view_log'):
            self.serializer_class.Meta.fields.append('log_file_name')
        # 关闭进程
        if user.has_perm('Component.kill_pid'):
            self.serializer_class.Meta.fields.append('pid')
        # 查看全部记录
        if user.has_perm('Component.view_all'):
            pass
        # 查看本组记录
        elif user.has_perm('Component.view_group'):
            self.queryset = self.queryset.filter(user__groups__in=user.get_group_id())
        # 查看本人记录
        else:
            self.queryset = self.queryset.filter(user=user)
        self.queryset = self.queryset.distinct()

    # 查看历史任务
    def list(self, request, *args, **kwargs):
        self._get_queryset(request, *args, **kwargs)
        return super().list(request,*args,**kwargs)

    # post请求执行程序，并在history表中开始做记录
    def post(self,request,*args,**kwargs):
        user = request.user
        app_type = 'ComplexApp'
        app_id = request.data.get('app_id')
        # 所有的参数包含在parameter中
        parameter = request.data.get('parameter',None)
        remark = request.data.get('remark',None)
        query_id = uuid4()
        obj = AppExecuteHistory()
        obj.query_id = query_id
        obj.app_id = app_id
        obj.app_name = '账户关联'
        obj.user = user
        obj.parameter = parameter
        obj.exec_status = 0
        obj.has_viewed = False
        obj.remark = remark
        obj.save()

        lock = RLock()
        t = Process(target=exec_script, args=(app_type, app_id, obj.id, query_id, parameter, lock,))
        t.start()
        obj.pid = t.pid
        obj.save()
        lock.acquire()
        # AppExecuteHistorySerializer(data=data).is_valid().save()
        lock.release()
        # result = self._get_history(user,app_id)
        return self.list(request, *args, **kwargs)
        # return Response(data=result, status = status.HTTP_200_OK)

    # 根据app_id获取应用
    def _get_app(self,app_id):
        pass

    def _record(self,user, app_id, parameter):
        pass
