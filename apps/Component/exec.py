import os
from django.db import close_old_connections
from utils.db import DbHook,DjangoDbHook
from utils.jinja_sql import JinjaScript
from django.conf import settings
import csv
import pandas as pd
import datetime
import numpy as np
from py2neo import Graph

import django
django.setup()

from Component.models import AppExecuteHistory
from utils import get_create_sql


def exec_script(app_type, app_id, id, query_id, parameter ,*args, **kwargs):
    try:
        query_id = str(query_id)
        jsql_file = os.path.join(settings.JSQL_ROOT, 'ComplexApp', 'acct_relation', 'execute.jsql')
        with open(jsql_file, 'r', encoding='utf8') as f:
            jsql = f.read()
        # print(parameter)
        parameter['start_date'] = datetime.datetime.strptime(parameter['start_date'], '%Y-%m-%d')
        parameter['end_date'] = datetime.datetime.strptime(parameter['end_date'], '%Y-%m-%d')
        parameter['acct_id'] = [('1', i) for i in parameter['acct_id'] if i]
        parameter['sec_code'] = [('1', i) for i in parameter['sec_code'] if i]
        parameter['ip'] = [('IP', i) for i in parameter['ip'] if i]
        parameter['mac'] = [('MAC', i) for i in parameter['mac'] if i]
        parameter['hardware'] = [('HARDWARE', i) for i in parameter['hardware'] if i]
        parameter['mobile'] = [('MOBILE', i) for i in parameter['mobile'] if i]
        # print(parameter)
        j = JinjaScript()
        s = j.render(jsql, parameter)
        sql = s[0]['statment']
        params = s[0]['params']

        db = DbHook('elk')
        db.autoclose = False
        db.connect()
        print('执行查询语句')
        print(id)
        cur = db.conn.cursor()

        with open('/tmp/test.sql','wb') as f:
            f.write(cur.mogrify(sql,params))

        db.execute(sql, params)
        print('大数据查询完成')
        # linux中使用fork机制，将父进程的信息复制到子进程，但子进程中实际无法使用该连接，需要现关闭掉
        close_old_connections()
        db_django = DjangoDbHook()
        db_django.execute('''
        create table anls_res.res_{app_id}_{id}_acct_group1	 (	acct_id	bpchar(10)
                ,	acct_name	varchar(120)
                ,	ccrc_acct_id	varchar(20)
                ,	grp_lvl_2_ip	int8
                ,	grp_lvl_2_mac	int8
                ,	grp_lvl_2_mobile	int8
                ,	grp_lvl_2_hardware	int8
                ,	grp_lvl_2	int8
                ,	grp_lvl_3_ip	int8
                ,	grp_lvl_3	int8	);
        create table anls_res.res_{app_id}_{id}_acct_group	 (	acct_id	bpchar(10)
                ,	acct_name	varchar(120)
                ,	ccrc_acct_id	varchar(20)
                ,	grp_lvl_2_ip	int8
                ,	grp_lvl_2_mac	int8
                ,	grp_lvl_2_mobile	int8
                ,	grp_lvl_2_hardware	int8
                ,	grp_lvl_2	int8
                ,	grp_lvl_3_ip	int8
                ,	grp_lvl_3	int8
                ,	grp_lvl_4_ip	int8
                ,	grp_lvl_4_mac	int8
                ,	grp_lvl_4_mobile	int8
                ,	grp_lvl_4_hardware	int8
                ,	grp_lvl_4	int8
                ,	grp_lvl_5	int8
                ,	grp_lvl_6	int8
                ,	grp_lvl_gt_2	int8
                ,	grp_lvl_gt_3	int8
                ,	grp_lvl_gt_4	int8
                ,	is_trade_over	int4
                ,	buy_vol	numeric
                ,sell_vol numeric	
                );
        create table anls_res.res_{app_id}_{id}_device_relation	 (	acct_id_1	bpchar(10)
                ,	acct_name_1	varchar(120)
                ,	acct_id_2	bpchar(10)
                ,	acct_name_2	varchar(120)
                ,	device_type	text
                ,	device	varchar(100)
                ,	is_same_sec_sameday	int4
                ,	is_same_sec_period	int4
                ,	is_diff_sec_sameday	int4
                ,	is_diff_sec_period	int4
                ,	diff_trade_date	int8
                ,	relation_lvl_2	int4
                ,	relation_lvl_3	int4
                ,	relation_lvl_4	int4	);
        create table anls_res.res_{app_id}_{id}_physical_relation_dtl	 (	acct_id_1	bpchar(10)
                ,	acct_name_1	varchar(120)
                ,	acct_id_2	bpchar(10)
                ,	acct_name_2	varchar(120)
                ,	relation_level	int4
                ,	device_relation	int4
                ,	gr_relation_phone	int4
                ,	gr_relation_addr	int4
                ,	jg_relation_legal	int4
                ,	jg_relation_phone	int4
                ,	jg_relation_addr	int4
                ,	gr_jg_relation_id_legal	int4
                ,	gr_jg_relation_phone	int4
                ,	gr_jg_relation_addr	int4
                ,	zg_relation_invest_cnsltnt	int4
                ,	zg_relation_cnsltnt_rep	int4
                ,	zg_relation_actl_operator	int4
                ,	zg_relation_holder	int4
                ,	zg_relation_phone	int4
                ,	zg_relation_addr	int4
                ,	gr_zg_relation_cnsltnt	int4
                ,	gr_zg_relation_actl_operator	int4
                ,	gr_zg_relation_holder	int4
                ,	gr_zg_relation_phone	int4
                ,	gr_zg_relation_addr	int4
                ,	gr_zg_relation_holder_kh_info	int4
                ,	jg_zg_relation_invest	int4
                ,	jg_zg_relation_holder	int4
                ,	jg_zg_relation_actl_operator	int4
                ,	jg_zg_relation_cnsltnt	int4
                ,	jg_zg_relation_phone	int4
                ,	jg_zg_relation_addr	int4
                ,	jg_zg_relation_holder_kh_info	int4
                ,	relation_ccrc	int4	);
        create table anls_res.res_{app_id}_{id}_invalid_device	 (	device_type	varchar
                ,	device	varchar
                ,	reason	varchar	);
        create table anls_res.res_{app_id}_{id}_acct_device_all	 (	trade_date	date
                ,	acct_id	varchar(10)
                ,	actl_acct_id	varchar
                ,	acct_name	varchar(120)
                ,	sec_code	varchar
                ,	sec_name	varchar(40)
                ,	ip_addr	varchar(100)
                ,	ip_addr2	varchar(100)
                ,	mac_addr	varchar(100)
                ,	mac_addr2	varchar(100)
                ,	mobile	varchar(100)
                ,	hardware	varchar(100)
                ,	invalid_device	text	);
        create table anls_res.res_{app_id}_{id}_relation_info (
            acct_id_1 CHAR(10)
            ,acct_name_1 VARCHAR
            ,acct_id_2 CHAR(10)
            ,acct_name_2 VARCHAR
            ,acct_info_type varchar
            ,acct_info varchar
            ,relation_lvl int4
            ,describe varchar
            ,acct_info_relation1 varchar
            ,acct_info_relation2 varchar 
        );
            '''.format(app_id=app_id, id=id))

        print('导出数据')
        tables = []
        table_info = [
            {"table": "acct_group", "label": "账户分组",
             "columns": [{"name": "acct_id", "label": "股东代码"}, {"name": "acct_name", "label": "股东名称"},
                         {"name": "ccrc_acct_id", "label": "一码通"}, {"name": "grp_lvl_2_ip", "label": "账户IP组号(2级)"},
                         {"name": "grp_lvl_2_mac", "label": "账户MAC组号(2/3级)"},
                         {"name": "grp_lvl_2_mobile", "label": "账户电话组号(2/3级)"},
                         {"name": "grp_lvl_2_hardware", "label": "账户硬盘序列号组号(2/3级)"},
                         {"name": "grp_lvl_2", "label": "关联级别（2级）"},
                         {"name": "grp_lvl_3_ip", "label": "账户IP组号(3级)"}, {"name": "grp_lvl_3", "label": "关联级别（3级）"},
                         {"name": "grp_lvl_4_ip", "label": "账户IP组号(4级)"},
                         {"name": "grp_lvl_4_mac", "label": "账户MAC组号(4级)"},
                         {"name": "grp_lvl_4_mobile", "label": "账户电话组号(4级)"},
                         {"name": "grp_lvl_4_hardware", "label": "账户硬盘序列号组号(4级)"},
                         {"name": "grp_lvl_4", "label": "关联级别（4级）"},
                         {"name": "grp_lvl_5", "label": "账户5级关联组号"}, {"name": "grp_lvl_6", "label": "账户6级关联组号"},
                         {"name": "grp_lvl_gt_2", "label": "物理关联并组（2级以上）"},
                         {"name": "grp_lvl_gt_3", "label": "物理关联并组（3级以上）"},
                         {"name": "grp_lvl_gt_4", "label": "物理关联并组（4级以上）"},
                         {"name": "is_trade_over", "label": "在分析期间对目标证券单边累计成交量是否超过10万股"},
                         {"name": "buy_vol", "label": "买入数量"},
                         {"name": "sell_vol", "label": "卖出数量"},
                         ]},
            {"table": "physical_relation_dtl","label":"物理关联明细",
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
                         {"name": "jg_zg_relation_holder",
                          "label": "机构账户与资管产品账户之间:机构账户所属机构或其主要负责人与资管产品账户投资委托人或投资受益人存在一致情形"},
                         {"name": "jg_zg_relation_actl_operator", "label": "机构账户与资管产品账户之间:机构账户所属机构主要负责人与资管产品账户实际操作人一致"},
                         {"name": "jg_zg_relation_cnsltnt", "label": "机构账户与资管产品账户之间:机构账户所属机构主要负责人与资管产品账户投资顾问代表一致"},
                         {"name": "jg_zg_relation_phone", "label": "机构账户与资管产品账户之间:开户信息联系电话一致"},
                         {"name": "jg_zg_relation_addr", "label": "机构账户与资管产品账户之间:开户信息联系地址一致"},
                         {"name": "jg_zg_relation_holder_kh_info",
                          "label": "机构账户与资管产品账户之间:机构账户开户预留电话号码或联系地址与资管产品账户投资委托人或投资受益人电话号码或联系地址存在一致情形"},
                         {"name": "relation_ccrc", "label": "一码通相同"}]},
            {"table": "device_relation","label":"终端关联明细",
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
            {"table": "acct_device_all","label":"账户终端流水",
             "columns": [{"name": "trade_date", "label": "日期"}, {"name": "acct_id", "label": "账户代码"},
                         {"name": "actl_acct_id", "label": "实际账户代码"}, {"name": "acct_name", "label": "账户名称"},
                         {"name": "sec_code", "label": "证券代码"}, {"name": "sec_name", "label": "证券名称"},
                         {"name": "ip_addr", "label": "ip地址1"}, {"name": "ip_addr2", "label": "ip地址2"},
                         {"name": "mac_addr", "label": "mac地址1"}, {"name": "mac_addr2", "label": "mac地址2"},
                         {"name": "mobile", "label": "手机号"}, {"name": "hardware", "label": "硬盘序列号"},
                         {"name": "invalid_device", "label": "无效终端"}]},
            {"table": "invalid_device","label":"无效终端",
             "columns": [{"name": "device_type", "label": "终端类型"}, {"name": "device", "label": "终端"},
                         {"name": "reason", "label": "无效原因"}]},
            {"table":"relation_info","label":"关联详细信息",
             "columns": [{"name": "acct_id_1", "label": "账户代码1"}, {"name": "acct_name_1", "label": "账户名称1"},
                         {"name": "acct_id_2", "label": "账户代码2"}, {"name": "acct_name_2", "label": "账户名称2"},
                         {"name": "acct_info_type", "label": "关联节点类型"}, {"name": "acct_info", "label": "关联节点"},
                         {"name": "relation_lvl", "label": "关联级别"}, {"name": "describe", "label": "关联描述"},
                         {"name": "acct_info_relation1", "label": "账户1与节点的关系"}, {"name": "acct_info_relation2", "label": "账户2与节点的关系"},
                         ]}
        ]

        csv_dir = os.path.join(settings.MEDIA_ROOT, 'app_exec_history', str(id))
        os.makedirs(csv_dir)
        excel_file = os.path.join(csv_dir, '账户关联分组.xlsx')
        excel_writer = pd.ExcelWriter(excel_file)
        # 数据文件
        for tb in table_info:
            print(tb['table'])
            tmp_table = 'tmp_result_' + tb['table']
            result_table = 'anls_res.res_{app_id}_{id}_{table}'.format(table=tb['table'], app_id=app_id, id=id)
            tables.append(result_table)
            mid_file = os.path.join(csv_dir, result_table + '.csv')
            db.dump_table(tmp_table, mid_file)
            db_django.load(result_table, mid_file)
            # 导出数据
            if tb['table'] != 'acct_device_all':
                df = pd.read_csv(mid_file)
                df.columns = [col['label'] for col in tb['columns']]
                df.to_excel(excel_writer, sheet_name=tb['label'], index=False)
            else:
                with open(mid_file, mode='r', encoding='utf8') as f:
                    f.readline()
                    columns_label = [i['label'] for i in tb['columns']]
                    open(os.path.join(csv_dir, '账户终端流水.csv'), 'a', encoding='gbk', errors='replace').write(
                        ','.join(columns_label) + '\n')
                    while True:
                        res = f.read(1024000)
                        if res:
                            open(os.path.join(csv_dir, '账户终端流水.csv'), 'a', encoding='gbk', errors='replace').write(
                                res.replace('\\N', ''))
                        else:
                            break
            os.remove(mid_file)
        try:
            excel_writer.save()
        except:
            df = pd.DataFrame([{'无数据': ''}])
            df.to_excel(excel_writer=excel_writer, sheet_name='Sheet1')
            excel_writer.save()
        # 图形文件
        graph_dir = os.path.join(settings.NEO4J_IMPORT_ROOT, query_id)
        os.makedirs(graph_dir)
        db.dump_table('tmp_result_relation_info', os.path.join(graph_dir, 'graph_relation_info.csv'))
        db.dump_table('tmp_result_node_acct', os.path.join(graph_dir, 'graph_node_acct.csv'))
        db.dump_table('tmp_result_node_acct_info', os.path.join(graph_dir, 'graph_node_acct_info.csv'))
        # 导入图数据库
        from py2neo import Graph, Node, Relationship, Subgraph
        graph = Graph(**settings.NEO4J_SETTING)
        cypher_load = '''
        USING PERIODIC COMMIT 10000
        LOAD CSV with headers FROM "file:///%(query_id)s/graph_node_acct.csv" AS line
        MERGE (n:acct {acct_id:line.acct_id})
        set n.acct_name=line.acct_name,n.ccrc_acct_id=line.ccrc_acct_id,n.acct_type=line.acct_type
        RETURN count(*);'''.replace('%(query_id)s', query_id)
        graph.run(cypher_load)
        cypher_load = '''
        USING PERIODIC COMMIT 10000
        LOAD CSV with headers FROM "file:///%(query_id)s/graph_relation_info.csv" AS line
        MERGE (:acct_info {acct_info_type:line.acct_info_type,acct_info:line.acct_info})
        RETURN count(*);'''.replace('%(query_id)s', query_id)
        graph.run(cypher_load)
        cypher_load = '''
        USING PERIODIC COMMIT 10000
        LOAD CSV with headers FROM "file:///%(query_id)s/graph_relation_info.csv" AS line
        match   (acct1:acct{acct_id:line.acct_id_1})
                ,(acct2:acct{acct_id:line.acct_id_2})
                ,(info:acct_info{acct_info_type:line.acct_info_type,acct_info:line.acct_info})
        MERGE   (acct1)-[r1:acct_to_acct{query_id:{query_id},acct_info_type:line.acct_info_type,acct_info:line.acct_info,relation_lvl:toInteger(line.relation_lvl),describe:line.describe}]-(acct2)
        MERGE   (acct1)-[r2:acct_to_info{query_id:{query_id},relation_lvl:toInteger(line.relation_lvl),relation:line.acct_info_relation1}]-(info)
        MERGE   (acct2)-[r3:acct_to_info{query_id:{query_id},relation_lvl:toInteger(line.relation_lvl),relation:line.acct_info_relation2}]-(info)
        RETURN  count(*);
        '''.replace('%(query_id)s', query_id)
        graph.run(cypher_load, query_id=query_id)

        db.close()
        obj = AppExecuteHistory.objects.get(pk=id)
        obj.result_table = tables
        obj.result_file_path = os.path.join(csv_dir, '账户关联分组.xlsx') + ';' + os.path.join(csv_dir, '账户终端流水.csv')
        obj.execute_end_time = datetime.datetime.now()
        obj.exec_status = 1
        obj.save()

    except:
        # close_old_connections()
        # db_django = DjangoDbHook()
        print('查询失败')
        close_old_connections()
        db_django = DjangoDbHook()
        obj = AppExecuteHistory.objects.get(pk=id)
        obj.execute_end_time = datetime.datetime.now()
        obj.exec_status = -1
        obj.save()
        raise

