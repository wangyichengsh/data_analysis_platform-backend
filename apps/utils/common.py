import logging,os
import configparser
import psycopg2
# import cx_Oracle
# import pymysql
import time
from decimal import Decimal, getcontext
# import pyodbc
import sqlparse
import sys
from datetime import datetime

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__),'conf/db.conf'))
logger = logging.getLogger('main.utils')

fetchsize = 100000
getcontext().prec = 8

class DataTypeException(Exception):
    def __init__(self, err="不支持的数据类型"):
        Exception.__init__(self, err) 

def Exec(code,l,replace_dict,orc_all,psgconn,fz,p01):
    try:
        logger.info(code)
        exec(code)
        return {'msg':'','res':[]}
    except Exception as e:
        return {'msg':str(e)}

def getConn(conf_name):
    try:
        db_type = config[conf_name]['db_type']
        if 'postgre' == db_type:            
            if sys.platform == "linux":
                conn = psycopg2.connect(host=config[conf_name]['host'], \
                                       user=config[conf_name]['user'], \
                                       password=config[conf_name]['password'], \
                                       port=int(config[conf_name]['port']), \
                                       database=config[conf_name]['database'])
                conn.set_client_encoding('latin1')
                conn.set_session(True)
                return conn
            else:
                conn = psycopg2.connect(host=config[conf_name]['host'], \
                                       user=config[conf_name]['user'], \
                                       password=config[conf_name]['password'], \
                                       port=int(config[conf_name]['port']), \
                                       database=config[conf_name]['database'])
                conn.set_client_encoding('latin1')
                conn.set_session(True)
                return conn
        elif 'oracle' == db_type:
            conn = cx_Oracle.connect(config[conf_name]['user'],config[conf_name]['password'],str(config[conf_name]['host'])+':'+str(config[conf_name]['port'])+'/'+str(config[conf_name]['database']),encoding='gbk')
            return conn
        elif 'mysql' == db_type:
            conn = pymysql.connect(host=config[conf_name]['host'], user=config[conf_name]['user'],passwd = config[conf_name]['password'],port =int(config[conf_name]['port']),db=config[conf_name]['database']) 
            return conn
    except Exception as e:
        conn = None
        logger.error('Conn Init Error:'+str(e))
        return conn
    
def execcode(sql,conf_name,conn=None, orient='records', **kwargs):
    res_all = []
    if 'exec'== conf_name:
        Exec(sql,kwargs['l'],kwargs['replace_dict'],kwargs['orc_all'],kwargs['psgconn'],kwargs['fz'],kwargs['p01'])
        return {"res":res_all,"msg":"","sql":""}
    try:
        P = sys.platform
        if conn == None:
            conn = getConn(conf_name)
        # sql = sql.strip()
        # if sql.endswith(';'):
            # sql = sql.split(';')[:-1]
        # else:
            # sql = sql.split(';')
        sql_list = sqlparse.split(sql)
        for code in sql_list:
            if len(code.strip())==0:
                continue
            try:
                if not hasattr(conn, 'ping'):
                    if hasattr(conn, 'set_client_encoding'):
                        conn.set_client_encoding('gbk') 
                else:
                    code = code.strip().strip(';')
                cur = conn.cursor()
                # code = code.encode('gbk').decode('latin1')
                cur.execute(code)
                conn.commit()
                if not hasattr(conn, 'ping'):
                    if hasattr(conn, 'set_client_encoding'):
                        conn.set_client_encoding('latin1')
                if cur.description != None:
                    res = list(cur.fetchmany(fetchsize))
                    if orient=='records':
                        rowname = [x[0] for x in cur.description]
                    elif orient=='raw':
                        rowname = [list(x) for x in cur.description]
                    result = []
                    if len(res) < fetchsize:
                        if orient=='records':
                            for row in res:
                                d ={}
                                for i,col in enumerate(rowname):
                                    if isinstance(row[i],Decimal) or isinstance(row[i],float) or isinstance(row[i],int):
                                        d[col] = float(row[i])
                                    elif type(row[i]) == str and P == "linux" and not hasattr(conn, 'ping'):
                                        d[col] = row[i].encode('latin1').decode('gbk',errors='replace')
                                    else:
                                        d[col] = row[i]
                                result.append(d)
                        elif orient=='raw':
                            result.extend(list(res))
                        if orient=='raw':
                            t = {}
                            t['description'] = rowname
                            t['data'] = result
                            result = t
                        res_all.append(result)
                    else:
                        while len(res)>0:
                            if orient=='records':
                                for row in res:
                                    d ={}
                                    for i,col in enumerate(rowname):
                                        if isinstance(row[i],Decimal) or isinstance(row[i],float) or isinstance(row[i],int):
                                            d[col] = float(row[i])
                                        elif type(row[i]) == str and P == "linux" and  not hasattr(conn, 'ping'):
                                            d[col] = row[i].encode('latin1').decode('gbk',errors='replace')
                                        else:
                                            d[col] = row[i]
                                    result.append(d)
                            elif orient=='raw':
                                result.extend(list(res))                 
                            res = list(cur.fetchmany(fetchsize))
                        if orient=='raw':
                            t = {}
                            t['description'] = rowname
                            t['data'] = result
                            result = t
                        res_all.append(result)
                cur.close()
            except Exception as e:
                logger.error('--------------------------------------------------')
                logger.error(conf_name+' Error:'+str(e))
                logger.error('SQL:'+str(code))
                res_all.append([{'msg':str(e),'sql':str(code)}])
                logger.error('--------------------------------------------------')
        return {"res":res_all,"msg":"","sql":""}
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        logger.error('--------------------------------------------------')
        logger.error(conf_name+' Error:'+str(e))
        logger.error('SQL:'+str(sql))
        logger.error('--------------------------------------------------')
        return {"res":[{'msg':str(e),'sql':str(sql)}],"msg":str(e),"sql":str(sql)}

def get_db_type(conn):
    if not hasattr(conn, 'ping'):
        if hasattr(conn, 'set_client_encoding'):
            return 'Postgre'
        else:
            return 'Mysql'
    else:
        return 'Oracle'

def get_table_type(description,from_db_type, time2varchar):
    res = []
    if from_db_type == 'Oracle':
    ############# Oracle 数据结构映射 ###############
        for t in description:
            if t[1] == cx_Oracle.FIXED_CHAR:
                res.append([t[0],'char',t[3]])
            elif t[1] == cx_Oracle.STRING:
                res.append([t[0],'varchar2',t[3]])
            elif t[1] == cx_Oracle.NUMBER:
                res.append([t[0],'number',t[4],t[5]])
            elif t[1] == cx_Oracle.TIMESTAMP:
                if not time2varchar:
                    res.append([t[0],'TIMESTAMP'])
                else:
                    res.append([t[0],'varchar2',10])
            elif t[1] == cx_Oracle.DATETIME:
                res.append([t[0],'date'])
            else:
                print(t[1])
                raise DataTypeException()
        return res
    else:
    ############# Postgre 数据结构映射 ###############
        for t in description:
            if t[1] == 1042:
                res.append([t[0],'char',t[3]])
            elif t[1] == 1043:
                res.append([t[0],'varchar2',t[3]])
            elif t[1] == 1700:
                res.append([t[0],'number',t[4],t[5]])
            elif t[1] == 1114 or t[1]== 1082:
                if not time2varchar:
                    res.append([t[0],'TIMESTAMP'])
                else:
                    res.append([t[0],'varchar2',10])
            elif t[1] == 25:
                res.append([t[0],'varchar2',100])
            elif t[1] == 20:
                res.append([t[0],'number',10,0])
            elif t[1] == 23:
                res.append([t[0],'number',10,0])
            else:
                print(t[1])
                raise DataTypeException()
        return res

def get_data_type(t,db_type):
    if db_type == 'Oracle':
        if t[1] == 'char':
            return 'char('+str(t[2])+')'
        elif t[1] == 'varchar2' and t[2]!= -1:
            return 'varchar2('+str(t[2])+')'
        elif t[1] == 'varchar2' and t[2]== -1:
            return 'varchar2(100)'        
        elif t[1] == 'number':
            if t[2] == None:
                int_len = '18'
            else:
                int_len = str(t[2])
            if t[3] == None:
                dec_len = '2'
            else:
                dec_len = str(t[3])
            return 'number('+str(int_len)+','+str(dec_len)+')'
        elif t[1] == 'TIMESTAMP':
            return 'TIMESTAMP'
        elif t[1] == 'date':
            return 'data'
    else:
    ############# Postgre 数据结构映射 ###############
        if t[1] == 'char':
            return 'char('+str(t[2])+')'
        elif t[1] == 'varchar2' and t[2]!= -1:
            return 'varchar('+str(t[2])+')'
        elif t[1] == 'varchar2' and t[2]== -1:
            return 'varchar'        
        elif t[1] == 'number':
            return 'decimal('+str(t[2])+','+str(t[3])+')'
        elif t[1] == 'TIMESTAMP':
            return 'TIMESTAMP'

def get_create_sql(description, from_db_type, to_db_type, table_name, temp= True , time2varchar=False):
    table_type = get_table_type(description,from_db_type, time2varchar)
    if to_db_type == 'Postgre':
        if temp == True:
            c_temp_sql = 'DROP TABLE IF EXISTS '+ table_name +';\n'
            c_temp_sql += "CREATE LOCAL TEMPORARY TABLE " + table_name + '(\n'
        else:
            c_temp_sql = 'DROP TABLE IF EXISTS '+ table_name+';'
            c_temp_sql += 'CREATE TABLE '+ table_name +' (\n'
    else:
        if temp == True:
            c_temp_sql = 'truncate table '+ str(table_name) +';drop table '+ str(table_name)+';'
            c_temp_sql += 'CREATE GLOBAL TEMPORARY TABLE '+ table_name +'(\n'
        else:
            c_temp_sql = 'truncate table '+ str(table_name) +';drop table '+ str(table_name)+';'
            c_temp_sql += 'CREATE  TABLE '+ table_name +'(\n'
    flag = 1
    for col in table_type:
        if flag == 1:
            flag = 0
        else:
            c_temp_sql += ','
        c_temp_sql += str(col[0]) + '  ' + str(get_data_type(col,to_db_type))+'\n'
    if to_db_type=='Postgre':
        if temp == True:
            c_temp_sql += ')\nWITH (ORIENTATION = COLUMN)\nON COMMIT PRESERVE ROWS;' 
        else:
            c_temp_sql += ')TABLESPACE hdfs_pd_temp_jcb distribute BY hash('+str(table_type[0][0])+');'
    else:
        if temp == True:
            c_temp_sql += ')\nON COMMIT PRESERVE ROWS;'
        else:
            c_temp_sql += ')\n;'
    return c_temp_sql

def get_insert_sql_once(col_list, data,  from_db_type, to_db_type, table_name, time2varchar):
    if to_db_type == 'Postgre':
        i_temp_sql = 'INSERT INTO '+ table_name + '('
        col_sql = ','.join(col_list)
        i_temp_sql += col_sql+') VALUES '
        d_sql = ','.join(['('+','.join(["'"+str(value)+"'" for value in row]) +')' for row in data])
        i_temp_sql += d_sql + ';'
    else:
        i_temp_sql = 'INSERT ALL \n'
        fix_sql = 'INTO '+ str(table_name) + '('+','.join(col_list)+') VALUES '
        if not time2varchar:
            i_temp_sql += ''.join([fix_sql+'('+','.join(["'"+str(value)+"'" if not isinstance(value, datetime) else "to_date('"+str(value)+"','YYYY-MM-DD HH24:MI:SS')" for value in row])+')\n' for row in data])
        else:
            i_temp_sql += ''.join([fix_sql+'('+','.join(["'"+str(value)+"'" if not isinstance(value, datetime) else "'"+value.strftime('%Y%m%d')+"'" for value in row])+')\n' for row in data])
        i_temp_sql += 'select 1 from dual'
    i_temp_sql = i_temp_sql.replace("'None'",'null')
    if from_db_type== 'Postgre':
        i_temp_sql = i_temp_sql.encode('latin1').decode('gbk',errors='replace')
    return i_temp_sql

def save_data(data,col_list):
    csv_name = 'temp'+str(int(time.time()*1000))+'.csv'    
    if col_list == None and len(data)>0:
        col_list = data[0].keys()
    with open(csv_name,mode='w',encoding='utf-8') as f:
        for row in data:
            row_str = ''
            for i in row:
                if isinstance(i, float):
                    row_str += str(i)+','
                elif isinstance(i,str):
                    temp_c = str(i).replace(',','，').replace('\n',' ')
                    row_str+=temp_c+','
                elif isinstance(i,datetime):
                    temp_c = i.strftime('%Y%m%d')
                    row_str+=temp_c+','
                elif i==None:
                    temp_c = 'None'
                    row_str+=temp_c+','
                else:
                    temp_c = str(i).replace(',','，').replace('\n',' ')
                    row_str+=temp_c+','
            row_str = row_str[:-1]+'\n'
            f.write(row_str)
    return csv_name

def copy_data_to(path, to_conn, table_name,col_list):
    sql = 'delete from '+table_name
    data = execcode(sql, 'None', to_conn, orient='raw')
    cur = to_conn.cursor()
    with open(path,mode='r',encoding='utf-8') as f:
        cur.copy_from(f,table_name, sep=',', null='None', columns=col_list)
    to_conn.commit()
    cur.close()
    return 0
    
def insert_data(description, data, to_conn, from_db_type, table_name, time2varchar):
    to_db_type = get_db_type(to_conn)
    if to_db_type == 'Postgre' and len(data)>=100000:
        col_list = [str(col[0]) for col in description]
        csv_name = save_data(data, col_list)
        copy_data_to(csv_name, to_conn, table_name, col_list)
        os.remove(csv_name)
        return {'res':[],'msg':''}
    if to_db_type == 'Oracle': 
        batch_size = 100
    else:
        batch_size = 10000
    col_list = [str(col[0]) for col in get_table_type(description,from_db_type, time2varchar)]    
    if len(data)<batch_size:
        insert_sql = get_insert_sql_once(col_list, data, from_db_type, to_db_type, table_name,time2varchar)
        res = execcode(insert_sql, 'None', to_conn, orient='raw')        
    else:
        for s in range(0, len(data),batch_size):
            insert_sql = get_insert_sql_once(col_list, data[s:s+batch_size], from_db_type, to_db_type, table_name,time2varchar)
            res = execcode(insert_sql, 'None', to_conn, orient='raw')
    return res

def if_close(conn, db_type):
    if db_type == 'Oracle':
        try:
            conn.ping()
        except:
            return 1
        else:
            return 0
    else:
        return conn.closed


def tranData(from_conn,to_conn, sql,table_name, create = True, to_conn_retry=None, temp = True, time2varchar= 'auto'):
    if isinstance(table_name,str):
        table_name = table_name.split(',')
    if isinstance(from_conn,str):
        from_conn = getConn(from_conn)
    if isinstance(to_conn,str):
        to_conn_retry = to_conn
        to_conn = getConn(to_conn)
    data = execcode(sql, 'None', from_conn, orient='raw')
    if(len(data['msg'])!=0):
        if if_close(to_conn, to_db_type):
            to_conn = getConn(to_conn_retry)
        return data,from_conn,to_conn
    else:
        for i,table in enumerate(data['res']):
            to_db_type = get_db_type(to_conn)
            from_db_type = get_db_type(from_conn)
            if time2varchar == 'auto':
                if to_db_type == 'Oracle':
                    time2varchar = True
                else:
                    time2varchar = False
            if create == True:
                if temp == False and to_db_type=='Postgre':
                    old_isolation_level = to_conn.isolation_level
                    to_conn.set_isolation_level(0)
                create_sql = get_create_sql(table['description'], from_db_type, to_db_type, table_name[i], temp, time2varchar)
                res = execcode(create_sql, 'None', to_conn, orient='raw')
                if temp == False and to_db_type == 'Postgre':
                    to_conn.set_isolation_level(old_isolation_level)
                if(len(res['msg'])!=0):
                    return res,from_conn,to_conn
            if not if_close(to_conn, to_db_type):
                res = insert_data(table['description'], table['data'], to_conn, from_db_type, table_name[i], time2varchar)
                if(len(res['msg'])!=0):
                    return res,from_conn,to_conn
            else:
                to_conn = getConn(to_conn_retry)
                res = insert_data(table['description'], table['data'], to_conn, from_db_type, table_name[i], time2varchar)
                if(len(res['msg'])!=0):
                    return res,from_conn,to_conn
    return {"res":[],"msg":"","sql":""},from_conn,to_conn

def drop_temp_table(table_name,conn,drop= True):
    if isinstance(table_name,str):
        table_name = table_name.split(',')
    for name in table_name:
        if drop == True:
            drop_sql = 'truncate table '+ str(name) +';drop table '+ str(name)
        else:
            drop_sql = 'truncate table '+ str(name) +';'
        execcode(drop_sql, 'DROP TEMP', conn, orient='raw')


if __name__ =="__main__":
    from_conn = getConn('PSGQuery')
    table_name = 'test_date'
    sql = 'select * from pd_data.ctl_tx_date'
    tranData(from_conn,from_conn, sql,table_name, create = True, to_conn_retry= 'PSGQuery', temp = True, time2varchar= 'auto')
    sql_1 = 'select * from test_date limit 5 '
    print(execcode(sql_1,'',from_conn,orient='raw'))
    from_conn.close()
