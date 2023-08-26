import os
import uuid
import pandas as pd
import psycopg2

import django_filters
from django.shortcuts import  render

from django.http import HttpResponse, HttpResponseRedirect, StreamingHttpResponse,FileResponse
from django.contrib.auth import authenticate, login as auth_login, logout as auth_logout
from django.contrib.auth.mixins import LoginRequiredMixin, PermissionRequiredMixin
from django.views import View
from django.forms import modelformset_factory
from django.contrib.auth import get_user_model
from django.apps import apps
from django.db import connection as conn

from rest_framework import viewsets, mixins
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.generics import GenericAPIView,ListAPIView,UpdateAPIView
from rest_framework import exceptions, status

from AnlsTool.settings import MEDIA_ROOT
from Audit.models import AuditRuleDetail, AuditOtherGroupSec, AuditFreqSec, AuditFreqAcct, AuditSensAcct, AuditOtherIp, \
    AuditNontrade, AuditQueryAcctType, AuditQuerySec, AuditMonth, AuditRuleList
from Audit.forms import MonthChoiceForm


# from .models import AuditRuleList, AuditRuleDetail, AuditOtherGroupSec, AuditFreqSec,  AuditFreqAcct, AuditSensAcct, AuditOtherIp, AuditNontrade, AuditQueryAcctType, AuditQuerySec, AuditMonth
from Audit.serializers import AuditOtherGroupSecSerializer, AuditFreqSecSerializer, AuditFreqAcctSerializer, AuditSensAcctSerializer, AuditOtherIpSerializer, AuditNontradeSerializer, AuditQueryAcctTypeSerializer, AuditQuerySecSerializer, AuditMonthSerializer

# Create your views here.

User = get_user_model()

_locals = locals()

# 文件下载用方法
def file_iterator(file_name, chunk_size=512):
    with open(file_name, 'rb') as f:
        while True:
            c = f.read(chunk_size)
            if c:
                yield c
            else:
                break


# class AuditView(ListAPIView,UpdateAPIView, GenericAPIView):
class AuditView(GenericAPIView):

    queryset = AuditRuleList.objects.all()

    def get(self, request, *args, **kwargs):
        user = request.user
        # user = User.objects.get(pk = 17)
        param = request.query_params
        start_month = param.get('start_month',None)
        end_month = param.get('end_month',None)
        is_group = param.get('is_group', False)
        action = param.get('action', None)
        try:
            if not start_month:
                start_month = 0
            if not end_month:
                end_month = 0
            if action == 'permission':
                return self.permission(user)
            elif action == 'list':
                return self.list(user, start_month, end_month, is_group)
            elif action == 'export':
                return self.export(user, start_month, end_month, is_group)
            elif action == 'report':
                return self.report(start_month, end_month)
        except:
            return Response(data=[],status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # 判断权限
    def permission(self,user):
        permissions = []
        if user.has_perm('Audit.audit_export'):
            permissions.append('audit_export')
        return Response(data=permissions, status=status.HTTP_200_OK)

    # 查询
    def list(self, user, start_month, end_month, is_group):
        # user = request.user
        # user = User.objects.get(pk = 17)
        # param = request.query_params
        # start_month = param.get('start_month',None)
        # end_month = param.get('end_month',None)

        group_names = None
        # is_group = param.get('is_group',None)

        if not start_month or not end_month:
            return Response()
        if is_group:
            group_names = user.get_group_name()
        result = []

        for rule in AuditRuleList.objects.all():
            rule_name = rule.rule_name
            model_name = rule.model_name
            data_model = _locals.get(model_name,None)
            self.serializer_class = _locals.get(model_name + 'Serializer')
            if group_names:
                queryset = data_model.objects.filter(month__exact = 0)
                for group_name in group_names:
                    queryset = queryset.union(data_model.objects.filter(month__gte=start_month, month__lte=end_month, group_name__icontains=group_name))
                queryset = queryset.distinct()
            else:
                queryset = data_model.objects.filter(month__gte=start_month, month__lte=end_month, full_name = user.full_name)
            serializer = self.get_serializer(queryset, many=True)
            data = serializer.data
            model_columns = dict([(field.name,field.verbose_name) for field in data_model._meta.fields])
            if data:
                columns = []
                for col_name in data[0].keys():
                    if col_name not in ('id','month'):
                        col_label = model_columns[col_name]
                        columns.append( {'col_name':col_name,'col_label':col_label})
                result.append({'model_name':model_name, 'rule_name':rule_name,'columns':columns, 'data':data})
        return  Response(result, status=status.HTTP_200_OK)

    # 修改
    def patch(self, request, *args, **kwargs):
        for result in request.data:
            model_name = result.get('model_name', None)
            data = result.get('data', None)
            data_model = _locals.get(model_name, None)
            self.serializer_class = _locals.get(model_name + 'Serializer')
            for row in data:
                row_data = data_model.objects.get(pk=row.get('id'))
                row_data.remark = row.get('remark',None)
                row_data.save()
        return Response(request.data, status=status.HTTP_200_OK)

    # 导出
    def export(self, user, start_month, end_month, is_group):
        uid = str(uuid.uuid4())
        basedir = os.path.join(MEDIA_ROOT, 'files', 'download', '审计结果', uid)
        os.makedirs(basedir)
        param = {}
        param['start_date'] = AuditMonth.objects.get(month=start_month).month_start
        param['end_date'] = AuditMonth.objects.get(month=end_month).month_end
        excel_file = os.path.join(basedir, '审计明细.xlsx')
        excel_writer = pd.ExcelWriter(excel_file)

        detaildatafields = [x.name for x in AuditRuleDetail._meta.fields if
                            x.name not in ['id', 'month', 'audit_rule_type']]
        detaildatacolumns = [x.verbose_name for x in AuditRuleDetail._meta.fields if
                             x.name not in ['id', 'month', 'audit_rule_type']]

        for rule in AuditRuleList.objects.all():
            rule_name = rule.rule_name
            if not is_group:
                df_detail = pd.DataFrame(list(
                    AuditRuleDetail.objects.filter(full_name=user.full_name, month__gte=start_month,
                                                   month__lte=end_month, audit_rule_type=rule_name).values(
                        *detaildatafields)), columns=detaildatafields)
                if not df_detail.empty:
                    df_detail.columns = detaildatacolumns
                    df_detail.to_excel(excel_writer=excel_writer, sheet_name=rule_name + '(明细)', index=False)
            else:
                group_names = user.get_group_name()
                if len(group_names) > 0:
                    for i in range(len(group_names)):
                        group_name = group_names[i]
                        if i == 0:
                            qst = AuditRuleDetail.objects.filter(group_name__icontains=group_name,
                                                                 month__gte=start_month,
                                                                 month__lte=end_month)
                        else:
                            qst = qst.union(AuditRuleDetail.objects.filter(group_name__icontains=group_names,
                                                                           month__gte=start_month, month__lte=end_month,
                                                                           audit_rule_type=rule_name))
                    qst = qst.distinct()
                df_detail = pd.DataFrame(list(qst.values(*detaildatafields)), columns=detaildatafields)
                if not df_detail.empty:
                    df_detail.columns = detaildatacolumns
                    df_detail.to_excel(excel_writer=excel_writer, sheet_name=rule_name + '(明细)', index=False)
        try:
            excel_writer.save()
        except:
            df = pd.DataFrame([{'无数据': ''}])
            df.to_excel(excel_writer=excel_writer, sheet_name='Sheet1')
            excel_writer.save()
        # response = StreamingHttpResponse(file_iterator(excel_file))
        f = open(excel_file,'rb')
        response = FileResponse(f)
        response['Content-Type'] = 'application/octet-stream'
        response['Content-Disposition'] = 'attachment;filename=%s' % '审计明细.xlsx'.encode('utf8').decode('ISO-8859-1')
        return response


    # 审计报告
    def report(self, start_month, end_month):
        uid = str(uuid.uuid4())
        basedir = os.path.join(MEDIA_ROOT, 'files', 'download', '审计结果', uid)
        os.makedirs(basedir)
        param = {}
        param['start_date'] = AuditMonth.objects.get(month=start_month).month_start
        param['end_date'] = AuditMonth.objects.get(month=end_month).month_end
        excel_file = os.path.join(basedir, '审计结果.xlsx')
        excel_writer = pd.ExcelWriter(excel_file)

        # 汇总数据
        df1 = pd.read_sql(con=conn, sql='''
            with t1 as (
            select	sys_name
                            ,count(1) as cnt_visit
            from 	audit.audit_log_dtl
            where date(log_time) between %(start_date)s and %(end_date)s
            group by 1
            )
            ,t2 as (
            select	'合计' as sys_name
                            ,count(1) as cnt_visit
            from 	audit.audit_log_dtl
            where date(log_time) between %(start_date)s and %(end_date)s
            )
            select a.sys_name
                        ,a.cnt_visit
                        ,a.cnt_visit/NULLIF(b.cnt_visit,0.0) AS cnt_visit_ratio
            from 	t1 a ,t2 b
            where 1 = 1
            union all
            select sys_name,cnt_visit,1 from t2        
        ''', params=param)
        df1.columns = ['系统名称', '访问次数', '访问次数占比']
        df1.to_excel(excel_writer=excel_writer, sheet_name='系统整体访问情况表', index=False)
        df2 = pd.read_sql(con=conn, sql='''
with t1 as (
    select  distinct greatest(week_start,%(start_date)s) as week_start,least(week_end,%(end_date)s) as week_end
    from    audit.ctl_tx_date
    where   trade_date between %(start_date)s and %(end_date)s
)
,t2 as (
    select  a.week_start
            ,b.sys_name
            ,count(b.*) as cnt_visit
    from    t1 a
            ,audit.audit_log_dtl b
    where   date(b.log_time) between %(start_date)s and %(end_date)s
    and     date(b.log_time) between a.week_start and a.week_end
    group by 1,2
)
select  a.week_start
        ,COALESCE(b.cnt_visit,0) as cnt_visit_3gss
        ,COALESCE(c.cnt_visit,0) as cnt_visit_msd
        ,COALESCE(d.cnt_visit,0) as cnt_visit_ngsp
--        ,COALESCE(e.cnt_visit,0) as cnt_visit_ngsp_uat
        ,COALESCE(f.cnt_visit,0) as cnt_visit_ngsp_fz
        ,COALESCE(g.cnt_visit,0) as cnt_visit_ngsp            
from    t1 a
left join 
        (select * from t2 where sys_name='3GSS') b
ON      a.week_start=b.week_start
left join 
        (select * from t2 where sys_name='MSD') c
ON      a.week_start=c.week_start
left join 
        (select * from t2 where sys_name='历史分析系统(大数据版)') d
ON      a.week_start=d.week_start
-- left join 
--         (select * from t2 where sys_name='新监察系统(UAT)') e
-- ON      a.week_start=e.week_start
left join 
        (select * from t2 where sys_name='新监察系统(仿真)') f
ON      a.week_start=f.week_start
left join 
        (select * from t2 where sys_name='新监察系统') g
ON      a.week_start=g.week_start
order by a.week_start           
        ''', params=param)
        df2.columns = ['起始日期', '3GSS', 'MSD', '历史分析系统(大数据版)', '新监察系统(仿真)', '新监察系统']
        df2.to_excel(excel_writer=excel_writer, sheet_name='系统按周访问情况表', index=False)
        df3 = pd.read_sql(con=conn, sql='''
with t1 as (
    select  distinct greatest(week_start,%(start_date)s) as week_start,least(week_end,%(end_date)s) as week_end
    from    audit.ctl_tx_date
    where   trade_date between %(start_date)s and %(end_date)s
)
,t2 as (
    select  date(log_time) as log_date,user_id,count(1) as cnt_visit
    from    audit.audit_log_dtl
		where 	date(log_time) between %(start_date)s and %(end_date)s
    group by 1,2
)
,t3 as (
    select  a.week_start,d.name as group_name,sum(b.cnt_visit) as cnt_visit
    from    t1 a
            ,t2 b
            ,anls.auth_user_group c
            ,anls.auth_group d
    where   b.log_date between a.week_start and a.week_end
    and     b.user_id = c.user_id
    and     b.log_date >= c.eff_date 
    and     b.log_date < c.end_date
    and     c.group_id = d.id
    group by 1,2
)
select  a.week_start
        ,COALESCE(b.cnt_visit,0) as cnt_visit1
        ,COALESCE(c.cnt_visit,0) as cnt_visit2
        ,COALESCE(d.cnt_visit,0) as cnt_visit3
        ,COALESCE(e.cnt_visit,0) as cnt_visit4  
        ,COALESCE(f.cnt_visit,0) as cnt_visit5
        ,COALESCE(g.cnt_visit,0) as cnt_visit6
        ,COALESCE(h.cnt_visit,0) as cnt_visit7
        ,COALESCE(i.cnt_visit,0) as cnt_visit8
from    t1 a
left join 
        (select * from t3 where group_name='实时1') b
ON      a.week_start=b.week_start
left join 
        (select * from t3 where group_name='实时2') c
ON      a.week_start=c.week_start
left join 
        (select * from t3 where group_name='实时3') d
ON      a.week_start=d.week_start
left join 
        (select * from t3 where group_name='科创组') e
ON      a.week_start=e.week_start
left join 
        (select * from t3 where group_name='分析组') f
ON      a.week_start=f.week_start
left join 
        (select * from t3 where group_name='线索1') g
ON      a.week_start=g.week_start
left join 
        (select * from t3 where group_name='线索2') h
ON      a.week_start=h.week_start
left join 
        (select * from t3 where group_name='技术组') i
ON      a.week_start=i.week_start
order by a.week_start
        ''', params=param)
        df3.columns = ['起始日期', '实时1', '实时2', '实时3', '科创组', '分析组', '线索1', '线索2', '技术组']
        df3.to_excel(excel_writer=excel_writer, sheet_name='小组按周访问情况表', index=False)

        detaildatafields = [x.name for x in AuditRuleDetail._meta.fields if
                            x.name not in ['id', 'month', 'audit_rule_type']]
        detaildatacolumns = [x.verbose_name for x in AuditRuleDetail._meta.fields if
                             x.name not in ['id', 'month', 'audit_rule_type']]

        # 规则数据
        for rule in AuditRuleList.objects.all():
            model_name = rule.model_name
            rule_name = rule.rule_name
            summary_data = apps.get_model('Audit', model_name)
            fields = [x.name for x in summary_data._meta.fields if x.name not in ['id', 'month']]
            columns = [x.verbose_name for x in summary_data._meta.fields if x.name not in ['id', 'month']]
            df_summary = pd.DataFrame(
                list(summary_data.objects.filter(month__gte=start_month, month__lte=end_month).values(*fields)),
                columns=fields)
            df_detail = pd.DataFrame(list(AuditRuleDetail.objects.filter(month__gte=start_month, month__lte=end_month,
                                                                         audit_rule_type=rule_name).values(
                *detaildatafields)), columns=detaildatafields)
            if not df_summary.empty:
                df_summary.columns = columns
                df_summary.to_excel(excel_writer=excel_writer, sheet_name=rule_name + '(汇总)', index=False)
            if not df_detail.empty:
                df_detail.columns = detaildatacolumns
                df_detail.to_excel(excel_writer=excel_writer, sheet_name=rule_name + '(明细)', index=False)
        excel_writer.save()

        response = FileResponse(open(excel_file,'rb'))
        # response.block_size=102400
        # response = StreamingHttpResponse(file_iterator(excel_file))
        response['Content-Type'] = 'application/octet-stream'
        response['Content-Disposition'] = 'attachment;filename=%s' % '审计结果.xlsx'.encode('utf8').decode('ISO-8859-1')
        return response


class AuditMonthViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    queryset = AuditMonth.objects.all()
    serializer_class = AuditMonthSerializer
