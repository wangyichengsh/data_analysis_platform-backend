# encoding:utf8
import os
import shutil
from collections import OrderedDict
from contextlib import closing
import json
import configparser
import psycopg2
# import cx_Oracle
# import pymysql
import csv
# import sqlalchemy
import uuid
import numpy as np
import sqlparse
import subprocess

# csv文件注册。
csv.register_dialect(
    'csv',
    delimiter=',',
    quotechar='"',
    escapechar='"',
    doublequote=True,
    skipinitialspace=False,
    lineterminator='\n',
    quoting=csv.QUOTE_MINIMAL
)


# None数据转化，默认为'\N'
def _to_native_types(data, na_rep='\\N'):
    """ convert to our native types format, slicing if desired """
    values = np.array(data)
    values[values == None] = na_rep
    return values


# 读取csv表头
def _get_csv_header(file):
    with open(file, mode='r') as f:
        csv_reader = csv.reader(f.readlines(1), dialect='csv')
    header = list(csv_reader)[0]
    return header


# 文件字符集转换
def _trans_file_coding(file, source_coding, target_coding, errors='replace'):
    if source_coding == target_coding:
        return
    tmp_file = file + '.tmptranscoding'
    if os.path.exists(tmp_file):
        os.remove(tmp_file)
    os.rename(file, tmp_file)
    try:
        with open(tmp_file, encoding=source_coding, errors=errors) as f:
            while True:
                res = f.read(1024000).encode(source_coding).decode('gbk', errors='replace')  # 越大越快;过大会内存溢出报错
                if res:
                    open(file, 'a', encoding=target_coding, errors=errors).write(res)
                else:
                    break
        os.remove(tmp_file)
    except:
        raise


# 数据库连接配置文件
ConnCfgFile = os.path.join(os.path.dirname(__file__), 'conf/db_new.cfg')


# 读取配置文件
def _get_connect_conf(conn_id, encoding=None, autocommit=True, cfg_file=ConnCfgFile, **options):
    '''
    :param conn_id: 数据库配置名,参见connect.cfg文件,目前支持mysql、postgre、oracle
    :param encoding:客户端字符编码,默认从配置文件中读取,若未传入且未从配置文件中获取到,则使用utf8
    :param autocommit:自动提交设置,默认True
    :param cfg_file: 配置文件绝对路径，默认为./conf/db.conf
    :param options: 尚未添加该参数内容
    :return:
    '''
    cfg = configparser.ConfigParser()
    cfg.read(cfg_file)
    conn_param = {}
    if conn_id in cfg.sections():
        for k, v in cfg.items(conn_id):
            conn_param[k] = v
    try:
        if not encoding:
            encoding = conn_param.get('encoding', 'utf8')
        if 'encoding' in conn_param.keys():
            conn_param.pop('encoding')

        db_type = conn_param.get('db_type', '').upper()

        # conn_param = conn_param
        conn_param.pop('db_type')
        # postgre配置
        if db_type == 'POSTGRES' or db_type == 'POSTGRE':

            conn = psycopg2.connect(**conn_param)
            conn.set_client_encoding(encoding)
            conn.set_session(autocommit)
        # MySQL配置
        elif db_type == 'MYSQL':
            if 'port' in conn_param.keys():
                conn_param['port'] = int(conn_param['port'])
            conn = pymysql.connect(**conn_param)
            conn.charset = encoding
            conn.autocommit = autocommit
        # ORACLE配置
        elif db_type == 'ORACLE':
            str_connect = ''' %(user)s/%(password)s@%(host)s:%(port)s/%(server)s ''' % conn_param
            conn = cx_Oracle.connect(str_connect, encoding=encoding)
            conn.autocommit = autocommit
        # 其他配置暂时不作处理
        else:
            conn = None
    except:
        conn = None
    return conn, db_type


# 调用airflow钩子函数
def _get_connect_airflow(conn_id, encoding=None, autocommit=True, **options):
    '''
    从airflow配置中获取连接
    :param conn_id: 数据库配置名,参见connect.cfg文件,目前支持mysql、postgre、oracle
    :param encoding:客户端字符编码,客户端字符编码,仅对postgres生效(若无extra配置和传入则默认utf8),mysql、oracle需到airflow中分别设置extra为{'charset':'utf8'}、{'encoding':'gbk'}
    :param autocommit:自动提交设置,默认True
    :param options: 尚未添加该参数内容
    :return:
    '''

    from airflow.hooks.dbapi_hook import DbApiHook
    connection = DbApiHook.get_connection(conn_id)
    db_type = connection.dbtype.upper()
    extra = connection.extra_dejson
    if db_type == 'POSTGRES':
        if not encoding:
            encoding = extra.get('encoding', 'utf8')
        conn = connection.get_hook().get_conn()
        conn.set_client_encoding(encoding)
        conn.set_session(autocommit)
    # MySQL配置
    elif db_type == 'MYSQL':
        conn = connection.get_hook().get_conn()
        conn.autocommit = autocommit
    # ORACLE配置
    elif db_type == 'ORACLE':
        conn = connection.get_hook().get_conn()
        conn.autocommit = autocommit
    # 其他配置暂时不作处理
    else:
        conn = None
    return conn, db_type


# 根据conn_id获取数据库连接
def getConn(conf_name, encoding=None, autocommit=True):
    '''
    通过conf_name创建连接，优先可从配置文件读取，若无则从airflow配置中获取
    :param conf_name: 数据库配置名,参见connect.cfg文件,目前支持mysql、postgre、oracle
    :param encoding:客户端字符编码,客户端字符编码,仅对postgres生效(若无extra配置和传入则默认utf8),mysql、oracle需到airflow中分别设置extra为{'charset':'utf8'}、{'encoding':'gbk'}
    :param autocommit:自动提交设置,默认True
    :return: conn
    '''

    conn, db_type = _get_connect_conf(conn_id=conf_name, encoding=encoding, autocommit=autocommit)
    if not conn:
        try:
            conn, db_type = _get_connect_airflow(conn_id=conf_name, encoding=encoding, autocommit=autocommit)
        except:
            conn = None
    return conn, db_type


class BaseDbHook(object):
    pass


# 数据库连接类
class DbHook(object):
    def __init__(self, conn_id, encoding=None, mode='auto', conf_file=ConnCfgFile):
        """
        :param conn_id: 数据库id，可填elk(大数据主备端)、oracle(主N03，备N02)、mysql(主：金融云mysql，备：无)、
                postgres(主243、备244) 。
        :param encoding: 数据库客户端字符集，默认为None。若值为None时则在创建数据连接时根据配置文件自动分配，否则将指定该字符集。
        :param mode: 若填写elk、oracle，则该值可起作用。若type=compare，则在主备端分别执行，并对结果数据
                进行统计比对；若type=auto，则根据准备端负载情况自动分配节点执行。
        :param conf_file: 连接配置文件，已使用默认配置处理好，不需要再传入。但预留接口可手工指定。

        注：这两个功能尚未实现，目前无论mode填什么值，都只使用主节点计算。
        """
        self.conn_id = conn_id
        self.mode = mode
        self.autocommit = True
        self.autoclose = True
        self.conn = None
        self.dbtype = None
        self._conf_file = conf_file
        self.encoding = encoding

    # 创建连接
    def connect(self):
        if self.conn:
            return
        cfg = configparser.ConfigParser()
        cfg.read(self._conf_file, encoding='utf8')
        # 默认连接配置
        default_conn_param = {}
        # 备端连接配置，目前尚未使用
        backup_conn_param = {}

        if self.conn_id in cfg.sections():
            conn_param = dict(cfg.items(self.conn_id))
            if conn_param.get('db_cfg', None) == 'True':
                default_conn_param = dict(cfg.items(conn_param.get('default', None)))
                backup_conn_param = dict(cfg.items(conn_param.get('backup', None)))
            else:
                default_conn_param = conn_param
        if not default_conn_param and not backup_conn_param:
            return

        # 优先连接主端，失败则尝试连接备端
        try:
            self._connect(default_conn_param)
        except:
            self._connect(backup_conn_param)

    # 根据配置字典创建连接
    def _connect(self, conn_param):
        self.encoding = self.encoding or conn_param.get('encoding', 'utf8')
        self.dbtype = conn_param.get('db_type', '').lower()
        conn_param.pop('db_type')
        if 'encoding' in conn_param.keys():
            conn_param.pop('encoding')
        # postgres或elk
        if self.dbtype in ('postgres', 'postgre', 'elk'):
            self.conn = psycopg2.connect(**conn_param)
            self.conn.set_client_encoding(self.encoding)
            self.conn.set_session(self.autocommit)
        # MySQL
        elif self.dbtype == 'mysql':
            conn_param['port'] = int(conn_param.get('port', 3306))
            self.conn = pymysql.connect(**conn_param)
            self.conn.charset = self.encoding
            self.autocommit = self.autocommit
        elif self.dbtype == 'oracle':
            # str_connect = ''' %(user)s/%(password)s@%(host)s:%(port)s/%(server)s ''' % conn_param
            self.conn = cx_Oracle.connect(conn_param['user'], conn_param['password'],
                                          str(conn_param['host']) + ':' + str(conn_param['port']) + '/' + str(
                                              conn_param['server']), encoding=self.encoding)
            self.conn.autocommit = self.autocommit
        # 其他配置暂时不作处理
        else:
            self.conn = None

    # 关闭连接
    def close(self):
        # 只有非django连接时才可关闭，django的连接由django自己控制，这里仅使用
        if self.conn_id != 'django_default':
            try:
                self.conn.close()
            except:
                pass
            finally:
                self.conn = None

    # 设置连接属性
    def set(self, *args, **kwargs):
        pass

    # 导出数据
    def _export(self, cur, sql, params, export_type, columns_info):
        data = None
        columns_name = None
        if export_type == 'json':
            # postgre使用row_to_json提高效率
            if self.dbtype in ('elk', 'postgres'):
                tmp_table = 'tmp_' + str(uuid.uuid4()).replace('-', '')
                cur.execute('drop table if exists {tmp_table};create temp table {tmp_table} as {sql}'.format(
                    tmp_table=tmp_table, sql=sqlparse.split(sql)[0]), params)
                self.conn.commit()
                cur.execute('select * from {tmp_table}'.format(tmp_table=tmp_table))
                columns_name = [i.name for i in cur.description]
                cur.execute('select row_to_json({tmp_table}) from {tmp_table}'.format(tmp_table=tmp_table))
                data = []
                while True:
                    res = cur.fetchmany(10000)
                    if not res:
                        break
                    data.extend([i[0] for i in res])
        # fetchall结果
        elif export_type == 'raw':
            if self.dbtype in ('elk', 'postgres'):
                cur.execute(sql, params)
                columns_name = [i.name for i in cur.description]
                data = cur.fetchall()
        elif export_type == 'dataframe':
            if self.dbtype in ('elk', 'postgres'):
                columns_name = [i.name for i in cur.description]
            import pandas.io.sql as psql
            data = psql.read_sql(sql, con=self.conn, params=params)

        # column信息从columns_info中获取，若columns_info为空，则将label设为英文字段名
        columns = [{'name': col_name, 'label': col_name} for col_name in columns_name]
        dict_columns_info = {}
        if columns_info:
            for col in columns_info:
                col_name = col['name']
                # col.pop('name')
                dict_columns_info[col_name] = col

            for column in columns:
                column.update(dict_columns_info[column['name']])

        result = {'columns': columns, 'data': data}
        return result

    # 解析sql语句，拆分为list，将select语句独立出来。
    # 结果为：[{'type':'EXECUTE','sql':sql_stmt},{'type':'SELECT','sql':sql_stmt}]
    def _parse_select_sql(self, sql):
        sql_list = []
        for parse in sqlparse.parse(sql):
            stmt = sqlparse.sql.Statement(parse.tokens)
            sql_stmt = stmt.value
            sql_type = stmt.get_type()
            if sql_type == 'UNKOWN' or sql_type is None:
                pass
            elif sql_type == 'SELECT':
                sql_list.append({'type': 'SELECT', 'sql': sql_stmt})
            elif sql_list and sql_list[-1].get('type', None) == 'EXECUTE':
                sql_list[-1]['sql'] = sql_list[-1]['sql'] + '\n' + sql_stmt
            else:
                sql_list.append({'type': 'EXECUTE', 'sql': sql_stmt})
        return sql_list

    # 执行语句,并根据export_type输出结果
    def execute(self, sql, params=None, export_type=None, columns_info=None):
        """
        执行sql代码
        :param sql: sql代码，str。
        :param params: 参数
        :param export_type: 值为None、'json'、'df'、'records'、'raw', 'dataframe'。
        :param columns_info：输出字段属性。{col_name:{'label':col_label,'width':xxxx,.....},..}
        :return:    若export=None则仅执行语句，返回None。结果类型由export_type决定。
        注:若原查询语句中已含有order by语
        """
        if not self.conn:
            self.connect()
        with closing(self.conn.cursor()) as cur:
            # 若需要输出，则将其中的select语句拆分出来单独执行out
            if export_type:
                result = []
                sql_list = self._parse_select_sql(sql)
                for i in sql_list:
                    type = i.get('type', None)
                    sql = i.get('sql', None)
                    if type == 'SELECT':
                        result.append(self._export(cur, sql, params, export_type, columns_info))
                    else:
                        cur.execute(sql, params)
                        self.conn.commit()
            else:
                cur.execute(sql, params)
                self.conn.commit()
                result = None

            if self.autoclose:
                self.close()

            return result

    # 复杂查询
    def complex_execute(self, sql, params=None, export_type=None):
        """
        执行sql代码
        :param sql: sql代码，一般为sql语句字符串，也可以是拆分为list的sql代码或OrderDict。
                    在不需要输出的情况下后两者与字符串效果一样。
        :param params: 参数
        :param export_type: 值为None、'json'、'df'、'records'、'raw'、'store'。
        :return:    若export=None则仅执行语句，返回None。
                    若export='store',则将结果保存至驻场服务站项目所用的后台数据库中，此时sql必须为字典结构
                    ，每个查询对应的key将作为存储表名，为保证存储时没有重复表，应保证库中没有与key同名的表。
                    若为其他值，则将结果输出。
                    若sql为字符串或列表，则根据查询语句顺序返回：orderDict['0':结果1,'1':结果2,'3':结果3...]；
                    若sql为字典，则根据字典对应的sql语句，返回orderDict['sql_key1':结果1,'sql_key2':结果2...]，
                    若一个sql_key对应多个查询语句，则替换为['sql_key1_0':结果1,'sql_key1_1':结果2...]。
                    上述的“结果”结构为字典：{'columns':['column_name1','column_name2'...],'data':query_data}。
                    query_data的类型由export_type决定。
        """
        # 若需要导出结果，则将sql中的查询语句拆分开，每当遇到查询语句则中止一次获取结果
        if isinstance(sql, OrderedDict):
            for key in sql:
                sql_list = self._parse_select_sql(sql.get(key))

    # 批量执行（一般用于insert语句）
    def execute_many(self, sql, params=None):
        if not self.conn:
            self.connect()
        with closing(self.conn.cursor()) as cur:
            cur.execute_many(sql, params)
            self.conn.commit()
        if self.autoclose:
            self.close()

    # 输出数据
    def output(self, sql, params, export_type='json'):
        self.execute(sql, params, export_type)

    # 插入数据
    def insert(self, table, file):
        pass

    # 获取oracle连接配置，用于调用sqlldr
    def _get_oracle_cfg(self):
        pass
        return '''aedev/\\"nGsp@2020\\"@10.51.1.2/orcl'''

    # 导入数据
    def load(self, table, file):
        """
        :param table: 待导入数据表名，本组实际使用场景中一般为临时表。
        :param file: 数据文件绝对路径名。
        为保证加载效率，导入数据源必须为格式固定的CSV文件。格式如下：
        1、必须含表头；
        2、分隔符为逗号；
        3、换行符为\\n；
        4、\\N表示null值；
        5、字符串中含逗号、换行符时，用双引号包裹；
        6、字符串中含双引号时，用另一个双引号转义；
        该函数优先尝试各数据库的批量加载命令，若失败则使用insert命令加载。
        """
        try:
            # postgres使用copy from命令导入
            if self.dbtype in ('postgres','postgre','elk'):
                with closing(self.conn.cursor()) as cur:
                    sql = '''
                    COPY {table} FROM STDIN 
                    WITH CSV
                    DELIMITER ','
                    NULL '\\N'
                    HEADER 
                    QUOTE '"'
                    ESCAPE '"'
                    ;             
                    '''.format(table=table)
                    with closing(self.conn.cursor()) as cur:
                        with open(file, 'rb') as f:
                            cur.copy_expert(sql, f,size=102400)
                            self.conn.commit()
            # mysql使用load data local infile导入，数据库服务器端必须设置GLOBAL local_infile = 1才能使用。
            elif self.dbtype == 'mysql':
                columns = _get_csv_header(file)
                sql = '''
                LOAD DATA LOCAL INFILE '{file}' REPLACE
                INTO TABLE {table}
                FIELDS
                    TERMINATED BY ','
                    OPTIONALLY ENCLOSED BY '"'
                    ESCAPED BY '"'
                LINES
                    TERMINATED BY '\\n'
                IGNORE 1 LINES
                '''.format(table=table, file=file)
                col_sets = []
                for col in columns:
                    col_sets.append("""{col} = CASE WHEN @{col}!='\\N' THEN @{col} END""".format(col=col))
                sql = sql + "(" + ",".join(['@' + col for col in columns]) + ')\nSET\n' + ',\n'.join(col_sets)
                with closing(self.conn.cursor()) as cur:
                    cur.execute('SET GLOBAL local_infile = 1;')
                    self.conn.commit()
                    cur.execute(sql)
                    self.conn.commit()
            # oracle使用sql loader导入到实体表，再从实体表转入table，最后删除中间实体表。客户端必须安装sqlldr才能使用。
            elif self.dbtype == 'oracle':
                sqlldr_table = 'tmp_sqlldr_' + str(uuid.uuid4()).replace('-', '')
                str_oracle_cfg = self._get_oracle_cfg()
                columns = _get_csv_header(file)
                sqlldr_ctl = '''
                OPTIONS(SKIP=1,SILENT=(FEEDBACK))
                LOAD DATA
                INFILE "{file}"
                TRUNCATE
                INTO TABLE {sqlldr_table}
                FIELDS TERMINATED BY ","
                OPTIONALLY ENCLOSED BY '"'
                TRAILING NULLCOLS
               '''.format(sqlldr_table=sqlldr_table, file=file)
                col_sets = ["{col} NULLIF {col}='\\\\N'".format(col=col) for col in columns]
                sqlldr_ctl = sqlldr_ctl + '(\n' + ',\n'.join(col_sets) + '\n)'

                ctl_file = ''
                with open(ctl_file, 'w') as f:
                    f.write(sqlldr_ctl)
                with closing(self.conn.cursor()) as cur:
                    cur.execute('create table {sqlldr_table} as (select * from {table} where 1 = 0)'.format(
                        sqlldr_table=sqlldr_table, table=table))
                    self.conn.commit()
                    subprocess.check_call(["sqlldr", str_oracle_cfg, "control=" + os.path.abspath(ctl_file)])
                    cur.execute(
                        'insert into {table} select * from {sqlldr_table}'.format(sqlldr_table=sqlldr_table,
                                                                                  table=table))
                    cur.execute('drop table sqlldr_table')
                    self.conn.commit()
        except:
            self.insert(table, file)
            raise

        finally:
            if self.autoclose:
                self.close()

    # 导出表数据
    def dump_table(self, table, file, encoding='utf8'):
        '''
        导出表数据。
        :param table: 待导出数据的表名
        :param file:  csv文件名
        :param encoding: 输出文件的字符集，默认utf8，若数据源的字符集与其不同，则将结果csv转化为该参数指定的字符集。
        '''
        db_client_encoding = self.encoding
        if self.dbtype == 'elk':
            db_client_encoding = 'latin1'
        if self.dbtype in ('postgres', 'postgre', 'elk'):
            sql = '''
            COPY {table} TO STDOUT 
            WITH CSV
            DELIMITER ','
            NULL '\\N'
            HEADER 
            QUOTE '"'
            ESCAPE '"'
            ;             
            '''.format(table=table)
            self.conn.set_client_encoding(db_client_encoding)
            with closing(self.conn.cursor()) as cur:
                with open(file, 'w', encoding=db_client_encoding) as f:
                    cur.copy_expert(sql, f)
                    f.truncate(f.tell())
                    self.conn.commit()
            self.conn.set_client_encoding(self.encoding)
        elif self.dbtype in ('mysql', 'oracle'):
            sql = 'select * from {table}'.format(table=table)
            self.dump_query(sql, file)
        if self.autoclose:
            self.close()
        _trans_file_coding(file, source_coding=db_client_encoding, target_coding=encoding)

    # 导出查询语句数据
    def dump_query(self, query, file, params={}, encoding='utf8'):
        '''
        导出SQL语句结果。
        :param query: SQL语句，必须以查询语句作结尾，结果将导出该查询语句的数据。
        :param file: csv文件名
        :param params: SQL语句参数
        '''
        if self.dbtype in ('postgres', 'postgre', 'elk'):
            table_name = 'tmp_dump_' + str(uuid.uuid4()).replace('-', '')
            select_stmt = sqlparse.split(query)[0].rstrip(';')
            sql = '''
            drop table if exists {table_name};
            create temp table {table_name} as (
              {select_stmt}
            );
            '''.format(table_name=table_name, select_stmt=select_stmt)
            self.execute(sql, params)
            self.dump_table(table_name, file, encoding)
        elif self.dbtype in ('oracle', 'mysql'):
            with closing(self.conn.cursor()) as cur:
                cur.execute(query, params)
                columns = [col[0] for col in cur.description]
                with open(file, mode='w', buffering=1024) as f:
                    csv_writer = csv.writer(f, dialect='csv')
                    csv_writer.writerow(_to_native_types(columns))
                    while True:
                        res = cur.fetchmany(10000)
                        if res:
                            csv_writer.writerows(_to_native_types(res))
                        else:
                            break
                        # with open(file, mode='a', buffering=1024) as f:
                        #     if res:
                        #         open(file, 'a', encoding=target_coding, errors=errors).write(res)
                        #     else:
                        #         break

                # res = cur.fetchall()
                # with open(file, mode='w', buffering=1024) as f:
                #     csv_writer = csv.writer(f, dialect='csv')
                #     csv_writer.writerow(_to_native_types(columns))
                #     csv_writer.writerows(_to_native_types(res))


# Django连接
class DjangoDbHook(DbHook):
    def __init__(self):
        super().__init__('')
        try:
            from django.db import connection
            self.conn_id = 'django_default'
            self.conn = connection
            self.mode = 'auto'
            self.autocommit = True
            self.autoclose = False
            self.conn = connection
            self.dbtype = 'postgres'
            self.encoding = 'utf8'
        except:
            raise

    def close(self) -> object:
        pass

    def connect(self):
        pass

    def query_table(self, table, columns_info=None, filters=None, page_size=None, page_number=None, order_by=None):
        '''
        :param table: 待查询表名
        :param columns_info: 列信息
        :param filters: 筛选条件。{'col1':'value1','col2':'value2'} => where col1=value1 and col2=value2
        :param page_size: 每页记录数
        :param page_number: 当前页数
        :param order_by: 排序条件。['col1','-col2'] => order by col1,-col2
        :return:
        '''
        page_total_num = 1
        where_stmt = ' where 1 = 1'
        order_stmt = ''
        offset_stmt = ''
        params = []
        if filters:
            filters = json.loads(filters)
            for col in filters.keys():
                col = '"' + col + '"'
                where_stmt = where_stmt + 'and {col} = %s'.format(col=col)
                params.append(filters[col])
        if order_by:
            col = '"' + order_by + '"'
            if order_by[0] == '-':
                order_stmt = ' order by "{order_by}" desc'.format(order_by=order_by[1:])
            else:
                order_stmt = ' order by "{order_by}"'.format(order_by=order_by)
        sql = 'select * from {table}'.format(table=table) + where_stmt + order_stmt
        count_sql = 'with t as (' + sql + ') select count(1) as n from t'

        row_total_num = self.execute(count_sql, params, 'raw')[0]['data'][0][0]
        if page_size and page_number:
            page_size = int(page_size)
            page_number = int(page_number)
            start = (page_number - 1) * page_size
            end = page_number * page_size
            params.append(start)
            params.append(end)
            offset_stmt = ' offset %s limit %s'
            page_total_num = int(row_total_num / page_size) + 1
        sql = sql + offset_stmt
        result = self.execute(sql, params, 'json', columns_info)[0]
        result['page_total_num'] = page_total_num
        result['row_total_num'] = row_total_num
        return result


if __name__ == '__main__':
    sql = '''
drop table if exists temp_ceshi_sec;
create temp table temp_ceshi_sec(
     sec_code char(6),
     sec_name  varchar,
     free_date  timestamp
);

insert into temp_ceshi_sec values('603486','科沃斯','20190528');
insert into temp_ceshi_sec values('601330','绿色动力','20190611');
insert into temp_ceshi_sec values('601990','南京证券','20190613');
insert into temp_ceshi_sec values('601066','中信建投','20190620');
insert into temp_ceshi_sec values('603105','芯能科技','20190709');
insert into temp_ceshi_sec values('601606','长城军工','20190806');
insert into temp_ceshi_sec values('603590','康辰药业','20190827');
insert into temp_ceshi_sec values('603583','捷昌驱动','20190924');
insert into temp_ceshi_sec values('601577','长沙银行','20190926');
insert into temp_ceshi_sec values('601162','天风证券','20191021');
insert into temp_ceshi_sec values('603220','中贝通信','20191115');
insert into temp_ceshi_sec values('601319','中国人保','20191118');
insert into temp_ceshi_sec values('603187','海容冷链','20191129');
insert into temp_ceshi_sec values('601860','紫金银行','20200103');
insert into temp_ceshi_sec values('601298','青岛港','20200121');
insert into temp_ceshi_sec values('603700','宁水集团','20200122');
insert into temp_ceshi_sec values('601615','明阳智能','20200123');
insert into temp_ceshi_sec values('601865','福莱特','20200217');
insert into temp_ceshi_sec values('603956','威派格','20200224');
insert into temp_ceshi_sec values('600928','西安银行','20200302');
insert into temp_ceshi_sec values('603379','三美股份','20200402');
insert into temp_ceshi_sec values('603068','博通集成','20200416');
insert into temp_ceshi_sec values('603317','天味食品','20200416');
insert into temp_ceshi_sec values('603267','鸿远电子','20200515');
insert into temp_ceshi_sec values('603327','福蓉科技','20200525');



DROP TABLE IF EXISTS tmp_utl_nego_acct;
CREATE LOCAL TEMPORARY TABLE tmp_utl_nego_acct
    WITH(ORIENTATION = COLUMN)
    DISTRIBUTE BY HASH(trade_date,acct_id)
AS ( 
select
    a.trade_date::varchar,
    a.sec_code,
    a.acct_id,
    a.free_vol, ---解禁数量 
    a.Tot_Cap   ---解禁后总股本
from pd_data_ap.utl_nego_acct a, temp_ceshi_sec b 
where a.sec_code = b.sec_code 
and a.trade_date = b.free_date
);

select * from tmp_utl_nego_acct    
    '''

    db = DbHook('oracle')
    print(db)


    # import time
    # import pymysql
    # import sqlalchemy
    #
    # db = DbHook('ps_244_dev')
    # db.connect()
    # db.autoclose = False
    # try:
    #     sql = '''
    #     select 1 as a
    #     '''
    #     db.dump_query(sql, 'd:/test.csv', encoding='utf8')
    # except:
    #     raise
    # finally:
    #     db.close()

    # _trans_file_coding('d:/test.csv','latin1','utf8')
    # with open('d:/test.csv','r',encoding='latin1') as f:
    #     s = f.read()
    #     print(s.encode('latin1').decode('gbk',errors='replace').encode('utf8',errors='replace'))
    #     # print(s.encode('gbk',errors='replace'))
    #     # print(s)
    # shutil.copy('d:/test.csv','d:/test2.csv')
    # _trans_file_coding('d:/test2.csv', 'latin1', 'utf8')
