from rest_framework import viewsets, mixins
from rest_framework.views import APIView
from rest_framework.authentication import TokenAuthentication
from rest_framework.response import Response
from django.http import HttpResponse
from rest_framework import filters
import django_filters.rest_framework
import django_filters
from django.contrib.auth import get_user_model
from rest_framework.pagination import PageNumberPagination
from rest_framework import filters

from .models import FeedRecord
from .serializers import FeedRecordSerializer

from NormalTask.models import File, ParentTask, Version, Task, ExecFunction, Input, InputFileSheet, InputFileColumn, OutputSheet, OutputColumn, SqlCode, JobHistory, ChangeHistory

User = get_user_model()

# Create your views here.

class NormPagination(PageNumberPagination):
    '''
    接口默认分页设置
    '''
    page_size = 10
    page_size_query_param = 'page_size'
    page_query_param = 'page'

class FeedRecordViewSet(mixins.ListModelMixin, mixins.RetrieveModelMixin,mixins.CreateModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, viewsets.GenericViewSet):    
    queryset = FeedRecord.objects.all().order_by('-id')
    serializer_class = FeedRecordSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['id','demind_seq','demind_id','model_name','if_feed']

class FeedPageViewSet(mixins.ListModelMixin, mixins.RetrieveModelMixin, viewsets.GenericViewSet):    
    queryset = FeedRecord.objects.all().order_by('if_feed').order_by('-id')
    serializer_class = FeedRecordSerializer
    pagination_class = NormPagination
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend, filters.SearchFilter]
    filterset_fields = ['id','demind_seq','demind_id','model_name','if_feed']
    search_fields = ('demind_seq','model_title','feedto','feedback', )

class HasNewFeedViewSet(APIView):
    """
    是否有新反馈 需求
    """
    def get(self, request, demand_id, format=None):
        feed_list = FeedRecord.objects.filter(demind_seq=demand_id).filter(if_feed=False)
        if len(feed_list)>0:
            return  Response({'newfeed':True}, content_type="application/json")
        return  Response({'newfeed':False} )

class HasNewFeedViewSetTask(APIView):
    """
    是否有新反馈 开发任务
    """
    def get(self, request, task_id, format=None):
        feed_list = FeedRecord.objects.filter(task_id=task_id)
        feed_list = feed_list.filter(model_name='OutputColumn').filter(if_feed=False)
        if len(feed_list)>0:
            return  Response({'newfeed':True}, content_type="application/json")
        return  Response({'newfeed':False} )

class ConfirmSonTask(APIView):
    """
    需求确认后更新开发任务状态也为确认
    """
    def post(self,request, format=None):
        param = request.data
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
                res.append(max_task[k].son_task.id)
                if (max_task[k].son_task.status == 'confirming'):
                    max_task[k].son_task.status = 'ranking'
                    max_task[k].son_task.save()
            return Response(res)
        
            



