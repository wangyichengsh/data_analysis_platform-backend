# -*- coding: utf-8 -*-
from threading import local
from contextlib import closing
from collections import OrderedDict
from uuid import uuid4
from jinja2 import Environment
from jinja2.ext import Extension
from jinja2.lexer import Token
from jinja2.utils import Markup
from jinja2 import nodes
import sqlparse
# import dbutils

from utils.db import DbHook

_thread_local = local()
_thread_local.index = 0


# jinja模板sql表名或字段名过滤器
def sql_safe(value):
    """Filter to mark the value of an expression as safe for inserting
    in a SQL statement"""
    return Markup(value)

# jinja模板sql参数过滤器，用作查询参数替换
def bind(value, name, db=None):
    """A filter that prints %s, and stores the value
    in an array, so that it can be bound using a prepared statement

    This filter is automatically applied to every {{variable}}
    during the lexing stage, so developers can't forget to bind
    """
    if not value:
        return
    if isinstance(value, Markup):
        return value
    elif isinstance(value, (list, tuple)):
        if isinstance(value[0], (list, tuple)):
            return _bin_insert_clause(value, name, db)
        return _bind_in_clause(value, name, db)
    elif isinstance(value, dict):
        raise Exception("""
            Got a dictionary when trying to bind parameter, expected 
            a scalar value.
            """)
    else:
        return _bind_param(_thread_local.bind_params[_thread_local.index], name, value, db)

def _bin_insert_clause(value, name, db=None):
    values = list(value)
    clauses = []
    for v in values:
        clauses.append(_bind_in_clause(v,name,db))

    clause = ",".join(clauses)
    return clause

# sql参数为list时调用
def _bind_in_clause(value, name, db=None):
    values = list(value)
    results = []

    for v in values:
        keys = list(_thread_local.bind_params[_thread_local.index].keys())
        key = "%s_%d" % (name,len(keys))
        results.append(_bind_param(_thread_local.bind_params[_thread_local.index], key, v, db))

    clause = ",".join(results)
    clause = "(" + clause + ")"
    return clause

# sql参数为字符或数字时调用
def _bind_param(already_bound, key, value, db=None):
    already_bound[key] = value
    if db == 'oracle':
        return ":%s" % key
    elif db in ('psql','postgres','elk','mysql'):
            return "%%(%s)s" % key
    else:
        raise AssertionError("无效的数据库连接%s" % db)

# 按原jinja格式返回值
def init(value):
    return value


class BaseScriptExtension(Extension):
    def __init__(self, environment):
        super(BaseScriptExtension, self).__init__(environment)
        self.lineno = None
        self.body = None
        self.args = None

    # 获取参数名
    def extract_param_name(self, tokens):
        name = ""
        for token in tokens:
            if token.test("variable_begin"):
                continue
            elif token.test("name"):
                name += token.value
            elif token.test("dot"):
                name += token.value
            else:
                break
        if not name:
            name = "bind#0"
        return name

    # 覆盖父类函数
    def parse(self, parser):
        # 第一个token从tag后开始。此处获取行数
        self._set_lineno(parser)
        # 将block中的信息依次放入参数中
        self._set_args(parser)
        # 获取body
        self._set_body(parser)
        # 调用_parse，返回列表
        return nodes.CallBlock(
            self.call_method("_parse", self.args, lineno=self.lineno), [], [], self.body
        ).set_lineno(self.lineno)

    # 设置lineno
    def _set_lineno(self,parser):
        self.lineno = parser.stream.current.lineno

    # 将block中的参数放入列表
    def _set_args(self,parser):
        # 将conn:xx,xx括号中的连接id取出，传入args中
        self.args = []
        while parser.stream.current.type != 'block_end':
            # tag名称
            if not self.args:
                token = parser.stream.current
                next(parser.stream)
            elif len(self.args) == 1:
                if parser.stream.skip_if('colon'):
                    token = parser.stream.expect('name')
                else:
                    break
            elif parser.stream.skip_if('comma'):
                token = parser.stream.expect('name')
            else:
                raise Exception("{%%}中含非法分割符")
            self.args.append(nodes.Const(token.value, lineno=token.lineno))
        return parser

    # 获取body
    def _set_body(self, parser):
        self.body = [nodes.Output([][:], lineno=self.lineno)]

    # 将代码段解析入列表
    def _parse(self, *args, caller=None):
        try:
            _thread_local.index = _thread_local.index + 1
        except:
            _thread_local.index = 1
        _thread_local.bind_params[_thread_local.index] = OrderedDict()
        args = list(args)
        type = args[0]
        params = args[1:]
        statment = caller()
        info = {
            'type':type,
            'info':params,
            'statment': statment.strip(),
            'params':_thread_local.bind_params[_thread_local.index],
        }

        return info


class SqlExtension(BaseScriptExtension):
    """
    解析sql代码块
    {% sql:elk|mysql|oracle|postgres:param|execute|output  %}
    {% endsql %}
    """
    tags = {"sql"}

    # 覆盖父类
    def filter_stream(self, stream):
        is_sql = False
        db = None
        while not stream.eos:
            token = next(stream)
            # 开始进入block
            if token.test("block_begin"):
                block_expr = []
                # 将block中的token放入block_expr中
                while not token.test("block_end"):
                    block_expr.append(token)
                    token = next(stream)
                block_expr.append(token)
                # 判断block类型
                block_name = block_expr[1].value
                # db = block_expr[3].value
                if block_name == 'sql':
                    is_sql = True
                    try:
                        db = block_expr[3].value
                    except:
                        pass
                elif block_name == 'endsql':
                    is_sql = False
                    db = None
                for token in block_expr:
                    yield token
            # 若block为sql语句，则对参数默认增加bind过滤器
            elif is_sql and token.test("variable_begin"):
                var_expr = []
                while not token.test("variable_end"):
                    var_expr.append(token)
                    token = next(stream)
                variable_end = token
                filters = [token.value for token in var_expr if token.test("name")]
                except_filters = ('bind', 'sqlsafe', 'safe', 'init')
                if not (set(filters) & set(except_filters)):
                    param_name = self.extract_param_name(var_expr)
                    # don't bind twice
                    var_expr.append(Token(10, 'pipe', u'|'))
                    var_expr.append(Token(10, 'name', u'bind'))
                    var_expr.append(Token(2, 'lparen', u'('))
                    var_expr.append(Token(10, 'string', param_name))
                    var_expr.append(Token(2, 'comma', u','))
                    var_expr.append(Token(10, 'string', db))
                    var_expr.append(Token(2, 'rparen', u')'))
                var_expr.append(variable_end)
                for token in var_expr:
                    yield token
            elif is_sql and db != 'oracle' and token.type == 'data':
                token = Token(token.lineno,token.type,token.value.replace('%','%%'))
                yield token
            else:
                yield token

    def _set_body(self, parser):
        self.body = parser.parse_statements(["name:endsql"], drop_needle=True)


class PythonExtension(BaseScriptExtension):
    """
    解析python代码块
    {% python %}
    {% endpython %}
    """
    tags = {"python"}

    def _set_body(self, parser):
        self.body = parser.parse_statements(["name:endpython"], drop_needle=True)


class ConnectExtension(BaseScriptExtension):
    """
    解析连接代码块
    {% conn:elk|oracle|mysql|postgres %} : 创建连接，可创建多个连接，以“,”分割，连接名即为冒号后的名称。
    {% endconn:elk|oracle|mysql|postgres %} : 关闭连接，可关闭多个连接，以“,”分割，连接名即为冒号后的名称。
    若不填写待关闭的连接，则关闭当前全部连接。若代码完成时未关闭连接，则关闭全部连接。
    """
    tags = {"conn","endconn"}


class DataTransExtension(BaseScriptExtension):
    """
    解析数据传输代码块，可跨数据库传输，也可从数据库传输给python
    {% datatrans:sql.elk.data1,sql.oracle.data2 %}
    {% datatrans:sql.elk.data1,python.data2 %}
    """
    tags = {"datatrans"}

    # 将block中的参数放入列表
    def _set_args(self,parser):
        # 将conn(xx,xx)括号中的连接id取出，传入args中
        self.args = []
        value = ''
        if_new_value = True
        while parser.stream.current.type != 'block_end':
            # tag名称
            token = parser.stream.current
            if token.type == 'name' or token.type == 'dot':
                value = value + token.value
            else:
                if value:
                    self.args.append(nodes.Const(value, lineno=token.lineno))
                value = ''
            next(parser.stream)
        if value:
            self.args.append(nodes.Const(value, lineno=token.lineno))

        return parser


class JinjaScript(object):
    def __init__(self):
        self.env = Environment()
        self.conn = {}
        self.data = {}
        self.source = ""
        self.result = OrderedDict()
        self.uid = str(uuid4()).replace('-','')
        self.index = 0

    # jinja环境变量准备
    def _prepare_environment(self):
        # self.env.autoescape = True
        self.env.add_extension(ConnectExtension)
        self.env.add_extension(SqlExtension)
        self.env.add_extension(PythonExtension)
        self.env.add_extension(DataTransExtension)
        # self.env.add_extension('jinja2.ext.autoescape')
        self.env.filters["bind"] = bind
        self.env.filters["sqlsafe"] = sql_safe
        self.env.filters["init"] = init

    def _del_extensions(self):
        extensions = list(self.env.extensions.keys())
        if extensions:
            for extension in extensions:
                self.env.extensions.pop(extension,None)

    # 将待解析文本拆分为列表，再逐个解析。该方法的目的是处理参数问题
    def _prepare_render(self):
        stream = self.env._tokenize(self.source, None)
        sources = []
        tmp_source = []
        wait_end = True
        in_block = False
        i = 0
        while not stream.eos:
            token = stream.current
            value = token.value
            next(stream)
            if token.type in ("variable_begin","block_begin","name"):
                value = str(value) + " "
            # elif token.type in ("variable_end","block_end"):
            #     value = " " + value
            # elif token.type == 'name':
            #     value = str(value) + ' '
            tmp_source.append(str(value))
            if token.type == 'block_begin':
                in_block = True
            elif token.type == 'block_end' and wait_end and i == 1:
                in_block = False
                if wait_end:
                    sources.append("".join(tmp_source))
                    tmp_source = []
            elif in_block and token.type == 'name' and token.value in ('sql', 'python'):
                wait_end = False
            elif in_block and token.type == 'name' and token.value in ('endsql', 'endpython', 'conn', 'endconn','datatrans'):
                wait_end = True
                i = 0
            i = i + 1

        return sources

    # 更新参数sql:params中的参数替换
    def _update_params(self, script):
        """
        :param result: dict。
        :return:
        """
        script_info = script.get('info', [])
        script_statment = script.get('statment','')
        script_params = script.get('params',{})
        assert script_info, 'sql标签后必须跟数据库连接id'
        assert script_info, 'sql语句不能为空'
        conn_id = script_info[0]
        try:
            self._conn(conn_id)
            conn = self.conn[conn_id]
            cur = conn.cursor()
            for sql in sqlparse.split(script_statment):
                sql = sql.replace('\r\n','').strip()
                if sql:
                    cur.execute(sql,script_params)
                    columns = [x[0] for x in cur.description]
                    rownum = cur.rowcount
                    res = cur.fetchall()
                    if rownum == 1:
                        for i in range(len(columns)):
                            self.data[columns[i]] = res[0][i]
                    elif rownum > 1:
                        for i in range(len(columns)):
                            self.data[columns[i]] = [x[i] for x in res]
                    else:
                        for col in columns:
                            self.data[col] = None
        except:
            raise

    # 渲染jinja模板，返回渲染后的语句，每个block以字典形式存放（尚未对sql和python之间互相传参做处理）
    def render(self, source, data=None, close_conn=True):
        """
        :param source: 待解析代码文本
        :param data: 参数字典
        :param close_conn: 关闭所有数据库连接。默认True用于返回渲染的语句，execute函数调用时使用False，用于执行语句。
        """
        self.source = source
        self.data = data
        try:
            _thread_local.bind_params = {}
            self._del_extensions()
            # 拆分模板
            sources = self._prepare_render()
            # 在模板拆分后再加载扩展，否则sql扩展会报错
            self._prepare_environment()
            # 解析模板
            scripts = []
            if sources:
                for source in sources:
                    template = self.env.from_string(source)
                    vars = template.new_context(self.data)
                    stream = template.stream(vars)
                    for script in stream:
                        if isinstance(script,dict):
                            scripts.append(script)
                            # 更新sql语句获取的新参数
                            script_type = script.get('type', None)
                            script_info = script.get('info', [])
                            script_statment = script.get('statment', '')
                            script_params = script.get('params', {})
                            if script_type == 'sql' and 'param' in script_info:
                                conn_id = script_info[0]
                                self._sql(conn_id, ['param'], script_statment, script_params)
                        # 删除无用的空字符
                        elif isinstance(script,str) and script.strip():
                            scripts.append(script)
            elif not scripts and source:
                template = self.env.from_string(source)
                vars = template.new_context(self.data)
                stream = template.stream(vars)
                for script in stream:
                    if isinstance(script, dict):
                        scripts.append(script)
                        # 更新sql语句获取的新参数
                        script_type = script.get('type', None)
                        script_info = script.get('info', [])
                        script_statment = script.get('statment', '')
                        script_params = script.get('params', {})
                        if script_type == 'sql' and 'param' in script_info:
                            conn_id = script_info[0]
                            self._sql(conn_id, ['param'], script_statment, script_params)
                    # 删除无用的空字符
                    elif isinstance(script, str) and script.strip():
                        scripts.append(script)
            # 关闭数据库连接
            if close_conn:
                self._close_conn()
            return scripts
        except:
            self._close_conn()
            raise
        finally:
            _thread_local.index = 0

    # 执行程序，将结果放入self.result中
    def execute(self, source, data=None):
        """
        :param source: 待解析代码文本
        :param data: 参数字典
        """
        self.source = source
        self.data = data
        try:
            _thread_local.bind_params = {}
            self._del_extensions()
            # 拆分模板
            sources = self._prepare_render()
            # 在模板拆分后再加载扩展，否则sql扩展会报错
            self._prepare_environment()
            # 解析模板
            # scripts = []
            if sources:
                for source in sources:
                    template = self.env.from_string(source)
                    vars = template.new_context(self.data)
                    stream = template.stream(vars)
                    for script in stream:
                        if isinstance(script,dict):
                            # scripts.append(script)
                            # 更新sql语句获取的新参数
                            script_type = script.get('type', None)
                            script_info = script.get('info', [])
                            script_statment = script.get('statment', '')
                            script_params = script.get('params', {})

                            print(script_statment)

                            if script_type == 'conn':
                                self._conn(conn_id=script_info)
                            elif script_type == 'endconn':
                                self._close_conn(conn_id=script_info)
                            elif script_type == 'sql':
                                conn_id = script_info[0]
                                sql_type = script_info[1:]
                                self._sql(conn_id, sql_type, script_statment, script_params)
                            elif script_type == 'python':
                                self._python(script_statment)
                            elif script_type == 'datatrans':
                                self._datatrans(script_info[0], script_info[1])
            elif source and not sources:
                template = self.env.from_string(source)
                vars = template.new_context(self.data)
                stream = template.stream(vars)
                for script in stream:
                    if isinstance(script, dict):
                        # scripts.append(script)
                        # 更新sql语句获取的新参数
                        script_type = script.get('type', None)
                        script_info = script.get('info', [])
                        script_statment = script.get('statment', '')
                        script_params = script.get('params', {})

                        if script_type == 'conn':
                            self._conn(conn_id=script_info)
                        elif script_type == 'endconn':
                            self._close_conn(conn_id=script_info)
                        elif script_type == 'sql':
                            conn_id = script_info[0]
                            sql_type = script_info[1:]
                            self._sql(conn_id, sql_type, script_statment, script_params)
                        elif script_type == 'python':
                            self._python(script_statment)
                        elif script_type == 'datatrans':
                            self._datatrans(script_info[0], script_info[1])
            return self.result

        except:
            self._close_conn()
            raise
        finally:
            _thread_local.index = 0
            self._close_conn()

    # 执行sql语句
    def _sql(self,conn_id,sql_type,sql_statment,sql_params):
        self._conn(conn_id)
        if not sql_type:
            sql_type = ["output"]
        with closing(self.conn[conn_id].cursor()) as cur:
            parsed = sqlparse.parse(sql_statment)
            for p in parsed:
                stmt = p.tokens
                s = sqlparse.sql.Statement(stmt)
                stmt_sql = s.value
                stmt_type = s.get_type()
                if stmt_type == 'UNKNOWN':
                    continue
            # for sql in sqlparse.split(sql_statment.strip()):
                cur.execute(stmt_sql,sql_params)
                if {"output", "param"} & set(sql_type) and cur.description:
                    if stmt_type == 'SELECT':
                        res = cur.fetchall()
                        if "output" in sql_type:
                            self.result[self.index] = res
                        if "param" in sql_type:
                            columns = [x[0] for x in cur.description]
                            if len(res) == 1:
                                for i in range(len(columns)):
                                    self.data[columns[i]] = res[0][i]
                            elif len(res) > 1:
                                for i in range(len(columns)):
                                    self.data[columns[i]] = [x[i] for x in res]
                            else:
                                for col in columns:
                                    self.data[col] = None

    # 执行python语句
    def _python(self,python_statment):
        pass

    # 执行数据传输
    def _datatrans(self, data_source, data_target):
        print(data_source,data_target)

    # 创建连接
    def _conn(self, conn_id=None):
        """
        :param conn_id: str|list|None。若不传入conn_id，则不做操作；
        """
        if not conn_id:
            pass
        elif isinstance(conn_id, list):
            for id in conn_id:
                self._conn(id)
        elif isinstance(conn_id, str):
            if conn_id not in self.conn:
                db_hook = DbHook(conn_id)
                db_hook.connect()
                db_hook.autoclose=False
                self.conn[conn_id] = db_hook.conn

    # 关闭连接
    def _close_conn(self, conn_id=None):
        """
        :param conn_id: str|list|None。若不传入conn_id，则关闭所有连接；
        """
        if (not conn_id) and self.conn:
            conn_id = list(self.conn.keys())
            self._close_conn(conn_id)
        elif isinstance(conn_id, list):
            for id in conn_id:
                self._close_conn(id)
        elif isinstance(conn_id, str):
            try:
                self.conn[conn_id].close()
                print("%s closed"%conn_id)
            except:
                pass
            self.conn.pop(conn_id,None)
