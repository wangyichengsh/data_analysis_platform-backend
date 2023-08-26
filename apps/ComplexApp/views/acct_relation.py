# 当前程序已写死，待修改
import os
from django.conf import settings
from django.contrib.auth import get_user_model
from django.http import FileResponse
from rest_framework import viewsets
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from ComplexApp.models.acct_relation import InvalidDevice
from ComplexApp.serializers.acct_relation import InvalidDeviceSerializer

from py2neo import Graph

from utils.viewset import ListModelApiMixin
from utils.db import DjangoDbHook

User = get_user_model()

table_info = [
    {"table": "physical_relation_dtl",
     "columns": [{"name": "acct_id_1", "label": "账户代码1"}, {"name": "acct_name_1", "label": "账户名称1"},
                 {"name": "acct_id_2", "label": "账户代码2"}, {"name": "acct_name_2", "label": "账户名称2"},
                 {"name": "relation_level", "label": "物理关联级别"}, {"name": "device_relation", "label": "期间用过同一终端信息"},
                 {"name": "gr_relation_phone", "label": "自然人账户之间:开户信息联系电话一致"},
                 {"name": "gr_relation_addr", "label": "自然人账户之间:开户信息联系地址一致"},
                 {"name": "jg_relation_legal", "label": "机构账户之间:机构主要负责人身份一致"},
                 {"name": "jg_relation_phone", "label": "机构账户之间:开户信息联系电话一致"},
                 {"name": "jg_relation_addr", "label": "机构账户之间:开户信息联系地址一致"},
                 {"name": "gr_jg_relation_id_legal", "label": "自然人账户与机构账户之间:所属机构主要负责人身份一致"},
                 {"name": "gr_jg_relation_phone", "label": "自然人账户与机构账户之间:开户信息联系电话一致"},
                 {"name": "gr_jg_relation_addr", "label": "自然人账户与机构账户之间:开户信息联系地址一致"},
                 {"name": "zg_relation_invest_cnsltnt", "label": "资管产品之间:投资顾问一致"},
                 {"name": "zg_relation_cnsltnt_rep", "label": "资管产品之间:投资顾问代表一致"},
                 {"name": "zg_relation_actl_operator", "label": "资管产品之间:账户实际操作人一致"},
                 {"name": "zg_relation_holder", "label": "资管产品之间:（劣后级）投资委托人或投资受益人存在一致情形"},
                 {"name": "zg_relation_phone", "label": "资管产品之间:（劣后级）投资委托人或投资受益人联系电话一致"},
                 {"name": "zg_relation_addr", "label": "资管产品之间:（劣后级）投资委托人或投资受益人联系地址一致"},
                 {"name": "gr_zg_relation_cnsltnt", "label": "自然人账户与资管产品账户之间:自然人账户持有人与资管产品账户投资顾问代表一致"},
                 {"name": "gr_zg_relation_actl_operator", "label": "自然人账户与资管产品账户之间:自然人账户持有人与资管产品账户实际操作人一致"},
                 {"name": "gr_zg_relation_holder", "label": "自然人账户与资管产品账户之间:自然人账户持有人与资管产品账户投资委托人或投资受益人存在一致情形"},
                 {"name": "gr_zg_relation_phone", "label": "自然人账户与资管产品账户之间:开户信息联系电话一致"},
                 {"name": "gr_zg_relation_addr", "label": "自然人账户与资管产品账户之间:开户信息联系地址一致"},
                 {"name": "gr_zg_relation_holder_kh_info",
                  "label": "自然人账户与资管产品账户之间:自然人账户开户预留电话号码或联系地址与资管产品账户投资委托人或投资受益人电话号码或联系地址存在一致情形"},
                 {"name": "jg_zg_relation_invest", "label": "机构账户与资管产品账户之间:机构账户所属机构与资管产品账户投资顾问一致"},
                 {"name": "jg_zg_relation_holder", "label": "机构账户与资管产品账户之间:机构账户所属机构或其主要负责人与资管产品账户投资委托人或投资受益人存在一致情形"},
                 {"name": "jg_zg_relation_actl_operator", "label": "机构账户与资管产品账户之间:机构账户所属机构主要负责人与资管产品账户实际操作人一致"},
                 {"name": "jg_zg_relation_cnsltnt", "label": "机构账户与资管产品账户之间:机构账户所属机构主要负责人与资管产品账户投资顾问代表一致"},
                 {"name": "jg_zg_relation_phone", "label": "机构账户与资管产品账户之间:开户信息联系电话一致"},
                 {"name": "jg_zg_relation_addr", "label": "机构账户与资管产品账户之间:开户信息联系地址一致"},
                 {"name": "jg_zg_relation_holder_kh_info",
                  "label": "机构账户与资管产品账户之间:机构账户开户预留电话号码或联系地址与资管产品账户投资委托人或投资受益人电话号码或联系地址存在一致情形"},
                 {"name": "relation_ccrc", "label": "一码通相同"}]},
    {"table": "device_relation",
     "columns": [{"name": "acct_id_1", "label": "账户代码1"}, {"name": "acct_name_1", "label": "账户名称1"},
                 {"name": "acct_id_2", "label": "账户代码2"}, {"name": "acct_name_2", "label": "账户名称2"},
                 {"name": "device_type", "label": "终端类型"}, {"name": "device", "label": "终端信息"},
                 {"name": "is_same_sec_sameday", "label": "是否同一日内使用该终端交易目标证券"},
                 {"name": "is_same_sec_period", "label": "是否分析期间内使用该终端交易目标证券"},
                 {"name": "is_diff_sec_sameday", "label": "是否同一日内使用该终端交易不同证券"},
                 {"name": "is_diff_sec_period", "label": "是否分析期间内使用该终端交易不同证券"},
                 {"name": "diff_trade_date", "label": "最短在X日内使用该终端交易目标证券"},
                 {"name": "relation_lvl_2", "label": "关联级别（2级）"}, {"name": "relation_lvl_3", "label": "关联级别（3级）"},
                 {"name": "relation_lvl_4", "label": "关联级别（4级）"}]},
    {"table": "acct_group",
     "columns": [{"name": "acct_id", "label": "股东代码"}, {"name": "acct_name", "label": "股东名称"},
                 {"name": "ccrc_acct_id", "label": "一码通"}, {"name": "grp_lvl_2_ip", "label": "账户IP组号(2级)"},
                 {"name": "grp_lvl_2_mac", "label": "账户MAC组号(2/3级)"},
                 {"name": "grp_lvl_2_mobile", "label": "账户电话组号(2/3级)"},
                 {"name": "grp_lvl_2_hardware", "label": "账户硬盘序列号组号(2/3级)"}, {"name": "grp_lvl_2", "label": "关联级别（2级）"},
                 {"name": "grp_lvl_3_ip", "label": "账户IP组号(3级)"}, {"name": "grp_lvl_3", "label": "关联级别（3级）"},
                 {"name": "grp_lvl_4_ip", "label": "账户IP组号(4级)"}, {"name": "grp_lvl_4_mac", "label": "账户MAC组号(4级)"},
                 {"name": "grp_lvl_4_mobile", "label": "账户电话组号(4级)"},
                 {"name": "grp_lvl_4_hardware", "label": "账户硬盘序列号组号(4级)"}, {"name": "grp_lvl_4", "label": "关联级别（4级）"},
                 {"name": "grp_lvl_5", "label": "账户5级关联组号"}, {"name": "grp_lvl_6", "label": "账户6级关联组号"},
                 {"name": "grp_lvl_gt_2", "label": "物理关联并组（2级以上）"}, {"name": "grp_lvl_gt_3", "label": "物理关联并组（3级以上）"},
                 {"name": "grp_lvl_gt_4", "label": "物理关联并组（4级以上）"},
                 {"name": "is_trade_over", "label": "在分析期间对目标证券单边累计成交量是否超过10万股"},
                 {"name": "buy_vol", "label": "买入数量"},
                 {"name": "sell_vol", "label": "卖出数量"}]},
    {"table": "acct_device_all",
     "columns": [{"name": "trade_date", "label": "日期"}, {"name": "acct_id", "label": "账户代码"},
                 {"name": "actl_acct_id", "label": "实际账户代码"}, {"name": "acct_name", "label": "账户名称"},
                 {"name": "sec_code", "label": "证券代码"}, {"name": "sec_name", "label": "证券名称"},
                 {"name": "ip_addr", "label": "ip地址1"}, {"name": "ip_addr2", "label": "ip地址2"},
                 {"name": "mac_addr", "label": "mac地址1"}, {"name": "mac_addr2", "label": "mac地址2"},
                 {"name": "mobile", "label": "手机号"}, {"name": "hardware", "label": "硬盘序列号"},
                 {"name": "invalid_device", "label": "无效终端"}]},
    {"table": "invalid_device",
     "columns": [{"name": "device_type", "label": "终端类型"}, {"name": "device", "label": "终端"},
                 {"name": "reason", "label": "无效原因"}]},
    {"table": "relation_info", "label": "关联详细信息",
     "columns": [{"name": "acct_id_1", "label": "账户代码1"}, {"name": "acct_name_1", "label": "账户名称1"},
                 {"name": "acct_id_2", "label": "账户代码2"}, {"name": "acct_name_2", "label": "账户名称2"},
                 {"name": "acct_info_type", "label": "关联节点类型"}, {"name": "acct_info", "label": "关联节点"},
                 {"name": "relation_lvl", "label": "关联级别"}, {"name": "describe", "label": "关联描述"},
                 {"name": "acct_info_relation1", "label": "账户1与节点的关系"},
                 {"name": "acct_info_relation2", "label": "账户2与节点的关系"},
                 ]}
]


# 无效终端信息
class InvalidDeviceViewSet(ListModelApiMixin, viewsets.GenericViewSet):
    queryset = InvalidDevice.objects.all()
    serializer_class = InvalidDeviceSerializer

    def get_columns_info(self):
        self.columns = [{'col_name': field.name, 'col_label': field.verbose_name} for field in
                        InvalidDevice._meta.fields if field.name in self.serializer_class.Meta.fields]
        return self.columns


# 账户关联查询
class AcctRelationsView(APIView):
    def get(self, request, *args, **kwargs):
        id = request.query_params.get('id', None)
        tab = request.query_params.get('tab', None)
        filters = request.query_params.get('filters', {})
        page_size = request.query_params.get('page_size', None)
        page_number = request.query_params.get('page_number', 1)
        order_by = request.query_params.get('order_by', None)
        columns_info = []
        for table in table_info:
            if table['table'] == tab:
                columns_info = table['columns']
                break

        db = DjangoDbHook()
        result_table = 'anls_res.res_{app_id}_{id}_{tab}'.format(tab=tab, app_id='1', id=id)
        result = db.query_table(result_table, columns_info=columns_info, filters=filters, page_size=page_size,
                                page_number=page_number, order_by=order_by)
        return Response(data=result, status=status.HTTP_200_OK)


# 图形数据视图
class AcctGraphView(APIView):
    # 获取图形数据，返回账户节点、账户信息节点、账户之间关联、账户与信息关联、图例分组
    def get(self, request, *args, **kwargs):
        id = request.query_params.get('id', None)
        app_id = request.query_params.get('app_id', None)
        graph_group = request.query_params.get('graph_group', 'grp_lvl_gt_2')
        use_acct_default_group = request.query_params.get('use_acct_default_group',None)
        query_id = request.query_params.get('query_id', '')
        edges_acct_to_acct = self.get_acct_to_acct(graph_group, query_id)
        edges_acct_to_info = self.get_acct_to_info(graph_group, query_id)
        db = DjangoDbHook()
        conditions = {
            'grp_lvl_2_ip': "relation_lvl in (2,3,4) and acct_info_type = 'IP'",
            'grp_lvl_2_mac': "relation_lvl in (2,3,4) and acct_info_type = 'MAC'",
            'grp_lvl_2_mobile': "relation_lvl in (2,3,4) and acct_info_type = 'MOBILE'",
            'grp_lvl_2_hardware': "relation_lvl in (2,3,4) and acct_info_type = 'HARDWARE'",
            'grp_lvl_2': "relation_lvl in (2,3,4)",
            'grp_lvl_3_ip': "relation_lvl in (3,4) and acct_info_type = 'IP'",
            'grp_lvl_3': "relation_lvl in (3,4)",
            'grp_lvl_4_ip': "relation_lvl = 4 and acct_info_type = 'IP'",
            'grp_lvl_4_mac': "relation_lvl = 4 and acct_info_type = 'MAC'",
            'grp_lvl_4_mobile': "relation_lvl = 4 and acct_info_type = 'MOBILE'",
            'grp_lvl_4_hardware': "relation_lvl = 4 and acct_info_type = 'HARDWARE'",
            'grp_lvl_4': "relation_lvl = 4",
            'grp_lvl_5': "relation_lvl = 5",
            'grp_lvl_6': "relation_lvl = 6",
            'grp_lvl_gt_2': "relation_lvl >= 2",
            'grp_lvl_gt_3': "relation_lvl >= 3",
            'grp_lvl_gt_4': "relation_lvl >= 4",
        }
        acct_table = 'anls_res.res_{app_id}_{id}_acct_group'.format(app_id='1', id=id)
        relation_table = 'anls_res.res_{app_id}_{id}_relation_info'.format(app_id='1', id=id)
        relation_condition = conditions[graph_group]
        if use_acct_default_group == 'false' or not use_acct_default_group:
            acct_group_setting = graph_group
        else:
            acct_group_setting = 'grp_lvl_gt_2'

        sql = '''
        DROP TABLE IF EXISTS tmp_acct;
        CREATE TEMP TABLE tmp_acct AS (
            with t1 as (
                select  {acct_group_setting},count(1) as n
                from    {acct_table}
                group by {acct_group_setting}
                having  count(1) > 1
            )
            select  a.{acct_group_setting} as acct_group
                    ,a.acct_id
                    ,a.acct_name
                    ,greatest(a.buy_vol,a.sell_vol) as trade_vol
            from    {acct_table} a,t1 b
            where   a.{acct_group_setting} = b.{acct_group_setting}
        );
        DROP TABLE IF EXISTS tmp_relation_info;
        CREATE TEMP TABLE tmp_relation_info as (
            SELECT  *
            FROM    {relation_table}
            WHERE   {relation_condition}
        );
        -- 账户节点
        WITH t1 AS (
            SELECT DISTINCT acct_id_1,acct_id_2 FROM tmp_relation_info
        )
        ,t2 AS (
            SELECT  a.*,b.acct_id_2 AS acct_id_relate
            FROM    tmp_acct a,tmp_relation_info b
            WHERE   a.acct_id = b.acct_id_1
            UNION
            SELECT  a.*,b.acct_id_1 AS acct_id_relate
            FROM    tmp_acct a,tmp_relation_info b
            WHERE   a.acct_id = b.acct_id_2
        )
        SELECT distinct acct_group,acct_id,acct_name,trade_vol,random() AS x_random,random() AS y_random
                ,COUNT(acct_id_relate) AS cnt_relate_acct
        FROM t2 
        GROUP BY acct_group,acct_id,acct_name,trade_vol
        ORDER BY acct_group,acct_id;
        -- 终端信息节点        
        WITH t1 AS (
            SELECT  b.acct_info_type,b.acct_info,a.acct_group
            FROM    tmp_acct a,tmp_relation_info b
            WHERE   a.acct_id = b.acct_id_1
            UNION
            SELECT  b.acct_info_type,b.acct_info,a.acct_group
            FROM    tmp_acct a,tmp_relation_info b
            WHERE   a.acct_id = b.acct_id_2
        )
        SELECT  distinct *,random() AS x_random,random() AS y_random
        FROM    t1
        ORDER BY acct_info_type,acct_info;
        -- 账户分组
        SELECT DISTINCT acct_group,'组'||acct_group::varchar as name FROM tmp_acct order by acct_group
        '''.format(acct_table=acct_table, acct_group_setting=acct_group_setting, relation_table=relation_table,
                   relation_condition=relation_condition)
        res_sql = db.execute(sql, export_type='json')
        nodes_acct = res_sql[0]['data']
        nodes_acct_info = res_sql[1]['data']
        catogories = res_sql[2]['data']
        return Response(data={'catogories': catogories, 'nodes_acct': nodes_acct, 'nodes_acct_info': nodes_acct_info,
                              'edges_acct_to_acct': edges_acct_to_acct, 'edges_acct_to_info': edges_acct_to_info},
                        status=status.HTTP_200_OK)

    # 账户之间的关系
    def get_acct_to_acct(self, graph_group, query_id):
        graph = Graph(**settings.NEO4J_SETTING)
        cypher_conditions = {
            'grp_lvl_2_ip': "relationship.relation_lvl in [2,3,4] and relationship.acct_info_type = 'IP'",
            'grp_lvl_2_mac': "relationship.relation_lvl in [2,3,4] and relationship.acct_info_type = 'MAC'",
            'grp_lvl_2_mobile': "relationship.relation_lvl in [2,3,4] and relationship.acct_info_type = 'MOBILE'",
            'grp_lvl_2_hardware': "relationship.relation_lvl in [2,3,4] and relationship.acct_info_type = 'HARDWARE'",
            'grp_lvl_2': "relationship.relation_lvl in [2,3,4]",
            'grp_lvl_3_ip': "relationship.relation_lvl in [3,4] and relationship.acct_info_type = 'IP'",
            'grp_lvl_3': "relationship.relation_lvl in [3,4]",
            'grp_lvl_4_ip': "relationship.relation_lvl = 4 and relationship.acct_info_type = 'IP'",
            'grp_lvl_4_mac': "relationship.relation_lvl = 4 and relationship.acct_info_type = 'MAC'",
            'grp_lvl_4_mobile': "relationship.relation_lvl = 4 and relationship.acct_info_type = 'MOBILE'",
            'grp_lvl_4_hardware': "relationship.relation_lvl = 4 and relationship.acct_info_type = 'HARDWARE'",
            'grp_lvl_4': "relationship.relation_lvl = 4",
            'grp_lvl_5': "relationship.relation_lvl = 5",
            'grp_lvl_6': "relationship.relation_lvl = 6",
            'grp_lvl_gt_2': "relationship.relation_lvl >= 2",
            'grp_lvl_gt_3': "relationship.relation_lvl >= 3",
            'grp_lvl_gt_4': "relationship.relation_lvl >= 4",
        }
        cypher = 'match (start_acct:acct) - [relationship:acct_to_acct{query_id:{query_id}}] -> (end_acct:acct) where ' \
                 + cypher_conditions[graph_group] \
                 + '  return distinct start_acct,end_acct,collect(relationship) as relationship'
        relations = graph.run(cypher, query_id=query_id).to_data_frame().to_dict('records')
        return relations

    # 账户与账户信息之间的关系
    def get_acct_to_info(self, graph_group, query_id):
        graph = Graph(**settings.NEO4J_SETTING)
        cypher_conditions = {
            'grp_lvl_2_ip': "relationship.relation_lvl in [2,3,4] and acct_info.acct_info_type = 'IP'",
            'grp_lvl_2_mac': "relationship.relation_lvl in [2,3,4] and acct_info.acct_info_type = 'MAC'",
            'grp_lvl_2_mobile': "relationship.relation_lvl in [2,3,4] and acct_info.acct_info_type = 'MOBILE'",
            'grp_lvl_2_hardware': "relationship.relation_lvl in [2,3,4] and acct_info.acct_info_type = 'HARDWARE'",
            'grp_lvl_2': "relationship.relation_lvl in [2,3,4]",
            'grp_lvl_3_ip': "relationship.relation_lvl in [3,4] and acct_info.acct_info_type = 'IP'",
            'grp_lvl_3': "relationship.relation_lvl in [3,4]",
            'grp_lvl_4_ip': "relationship.relation_lvl = 4 and acct_info.acct_info_type = 'IP'",
            'grp_lvl_4_mac': "relationship.relation_lvl = 4 and acct_info.acct_info_type = 'MAC'",
            'grp_lvl_4_mobile': "relationship.relation_lvl = 4 and acct_info.acct_info_type = 'MOBILE'",
            'grp_lvl_4_hardware': "relationship.relation_lvl = 4 and acct_info.acct_info_type = 'HARDWARE'",
            'grp_lvl_4': "relationship.relation_lvl = 4",
            'grp_lvl_5': "relationship.relation_lvl = 5",
            'grp_lvl_6': "relationship.relation_lvl = 6",
            'grp_lvl_gt_2': "relationship.relation_lvl >= 2",
            'grp_lvl_gt_3': "relationship.relation_lvl >= 3",
            'grp_lvl_gt_4': "relationship.relation_lvl >= 4",
        }
        cypher = 'match (acct:acct) - [relationship:acct_to_info{query_id:{query_id}}] - (acct_info:acct_info) where ' \
                 + cypher_conditions[graph_group] \
                 + ' return distinct acct,acct_info,collect(relationship) as relationship'
        relations = graph.run(cypher, query_id=query_id).to_data_frame().to_dict('records')
        return relations

    # 路径查询
    def post(self,request,*args,**kwargs):
        id = request.data.get('id', None)
        app_id = request.data.get('app_id', None)
        query_id = request.data.get('query_id', '')
        graph_group = request.data.get('graph_group', 'grp_lvl_gt_2')
        start_acct_id = request.data.get('nodeA',None)
        end_acct_id = request.data.get('nodeB', None)
        graph = Graph(**settings.NEO4J_SETTING)
        cypher_conditions = {
            'grp_lvl_2_ip': "relationship.relation_lvl in [2,3,4] and relationship.acct_info_type = 'IP'",
            'grp_lvl_2_mac': "relationship.relation_lvl in [2,3,4] and relationship.acct_info_type = 'MAC'",
            'grp_lvl_2_mobile': "relationship.relation_lvl in [2,3,4] and relationship.acct_info_type = 'MOBILE'",
            'grp_lvl_2_hardware': "relationship.relation_lvl in [2,3,4] and relationship.acct_info_type = 'HARDWARE'",
            'grp_lvl_2': "relationship.relation_lvl in [2,3,4]",
            'grp_lvl_3_ip': "relationship.relation_lvl in [3,4] and relationship.acct_info_type = 'IP'",
            'grp_lvl_3': "relationship.relation_lvl in [3,4]",
            'grp_lvl_4_ip': "relationship.relation_lvl = 4 and relationship.acct_info_type = 'IP'",
            'grp_lvl_4_mac': "relationship.relation_lvl = 4 and relationship.acct_info_type = 'MAC'",
            'grp_lvl_4_mobile': "relationship.relation_lvl = 4 and relationship.acct_info_type = 'MOBILE'",
            'grp_lvl_4_hardware': "relationship.relation_lvl = 4 and relationship.acct_info_type = 'HARDWARE'",
            'grp_lvl_4': "relationship.relation_lvl = 4",
            'grp_lvl_5': "relationship.relation_lvl = 5",
            'grp_lvl_6': "relationship.relation_lvl = 6",
            'grp_lvl_gt_2': "relationship.relation_lvl >= 2",
            'grp_lvl_gt_3': "relationship.relation_lvl >= 3",
            'grp_lvl_gt_4': "relationship.relation_lvl >= 4",
        }
        cypher = 'match p=shortestPath((start_acct:acct{acct_id:{start_acct_id}}) - [relationship:acct_to_acct*] - (end_acct:acct{acct_id:{end_acct_id}})) '\
                 + 'where all(relationship in relationships(p) where relationship.query_id={query_id} and '\
                 + cypher_conditions[graph_group] \
                 + ')  return nodes(p) as nodes,relationships(p) as edges,length(p) as distance'
        res = graph.run(cypher, query_id=query_id,start_acct_id=start_acct_id,end_acct_id=end_acct_id).data()
        path={'nodes':[],'edges':[],'distance':0}
        data = []
        acct_id_list = []
        if res:
            path['distance'] = res[0]['distance']

            for node in res[0]['nodes']:
                path['nodes'].append(dict(node))
                acct_id_list.append('\''+node['acct_id']+'\'')
            for edge in res[0]['edges']:
                path['edges'].append(dict(edge))


            relation_table = 'anls_res.res_{app_id}_{id}_relation_info'.format(app_id='1', id=id)
            acct_id_in_clause = '('+','.join(acct_id_list)+')'
            conditions = {
                'grp_lvl_2_ip': "relation_lvl in (2,3,4) and acct_info_type = 'IP'",
                'grp_lvl_2_mac': "relation_lvl in (2,3,4) and acct_info_type = 'MAC'",
                'grp_lvl_2_mobile': "relation_lvl in (2,3,4) and acct_info_type = 'MOBILE'",
                'grp_lvl_2_hardware': "relation_lvl in (2,3,4) and acct_info_type = 'HARDWARE'",
                'grp_lvl_2': "relation_lvl in (2,3,4)",
                'grp_lvl_3_ip': "relation_lvl in (3,4) and acct_info_type = 'IP'",
                'grp_lvl_3': "relation_lvl in (3,4)",
                'grp_lvl_4_ip': "relation_lvl = 4 and acct_info_type = 'IP'",
                'grp_lvl_4_mac': "relation_lvl = 4 and acct_info_type = 'MAC'",
                'grp_lvl_4_mobile': "relation_lvl = 4 and acct_info_type = 'MOBILE'",
                'grp_lvl_4_hardware': "relation_lvl = 4 and acct_info_type = 'HARDWARE'",
                'grp_lvl_4': "relation_lvl = 4",
                'grp_lvl_5': "relation_lvl = 5",
                'grp_lvl_6': "relation_lvl = 6",
                'grp_lvl_gt_2': "relation_lvl >= 2",
                'grp_lvl_gt_3': "relation_lvl >= 3",
                'grp_lvl_gt_4': "relation_lvl >= 4",
            }
            sql = '''
            SELECT *
            FROM   {relation_table}
            WHERE  (acct_id_1 IN {acct_id_in_clause} and acct_id_2 IN {acct_id_in_clause})
            and    {relation_condition}
            '''.format(acct_id_in_clause=acct_id_in_clause, relation_table=relation_table,
                       relation_condition=conditions[graph_group])
            db = DjangoDbHook()
            data = db.execute(sql, export_type='json',columns_info=table_info[-1]['columns'])[0]
        return Response(data={'path':path,'data':data},status=status.HTTP_200_OK)


# 文件下载
class FileDownloadView(APIView):
    def get(self, request, *args, **kwargs):
        id = request.query_params.get('id', None)
        file_type = request.query_params.get('file_type', None)
        dir = os.path.join(settings.MEDIA_ROOT, 'app_exec_history', str(id))
        if file_type == 'excel':
            file = os.path.join(dir, '账户关联分组.xlsx')
        else:
            file = os.path.join(dir, '账户终端流水.csv')
        f = open(file, 'rb')

        response = FileResponse(f)
        response['Content-Type'] = 'application/octet-stream'
        response['Content-Disposition'] = 'attachment;filename=%s' % os.path.basename(file).encode('utf8').decode(
            'ISO-8859-1')
        return response


# 说明文档
class DocumentView(APIView):
    def get(self, request, *args, **kwargs):
        file = os.path.join(settings.MEDIA_ROOT, 'document', '新终端关联使用手册.docx')
        f = open(file, 'rb')
        response = FileResponse(f)
        response['Content-Type'] = 'application/octet-stream'
        response['Content-Disposition'] = 'attachment;filename=%s' % os.path.basename(file).encode('utf8').decode(
            'ISO-8859-1')
        return response
