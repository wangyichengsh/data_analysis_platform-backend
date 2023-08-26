from django.shortcuts import render
from django.db import connection
import logging
from datetime import datetime

logger = logging.getLogger('main.NormalTask_views')

from rest_framework.views import APIView
from rest_framework.response import Response

from utils import getConn,execcode, tranData, drop_temp_table

def get_data_size():
    conn = connection
    sql_count = 'select count(1) from jcb_data.data_kcb_screen_main;'
    cur = conn.cursor()
    try:
        cur.execute(sql_count)
    except:
        return 0
    d = cur.fetchall()
    cur.close()
    return d[0][0]

def get_data_all(page,size,order):
    conn = connection
    start = (int(page)-1) * int(size)
    if len(str(order))== 0:
        order_str ='sec_code'
    else:
        order_str = str(order)[1:]+' '
        if str(order).startswith('-'):
            order_str += 'desc'        
    sql = '''
    select  sec_code
                ,sec_name
                ,first_price
                ,first_vol
                ,new_price
                ,curr_sec_rf
                ,open_price
                ,open_sec_rf
                ,sy_ratio
                ,trade_amt
                ,nego_rate
                ,first_buy_acct_name
                ,first_sell_acct_name
                ,ccrc_cnt
                ,order_cnt_ratio
                ,rz_buy_amt
                ,rq_sell_vol
                ,COALESCE(cnt_alarm    ,0) AS cnt_alarm    
                ,COALESCE(cnt_warn     ,0) AS cnt_warn     
                ,COALESCE(cnt_stop_acct,0) AS cnt_stop_acct
                ,COALESCE(cnt_stop_sec ,0) AS cnt_stop_sec 
        from jcb_data.data_kcb_screen_main
        order by %(order_str)s nulls last
        offset %(start)d
        limit %(size)d        
    ''' % {'order_str':order_str,'start':start,'size':size}
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error('获取数据出错:'+str(e))
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
            elif row[i] == None:
                d[col] = 0
            else:
                d[col] = row[i]
        result.append(d)
    return result


class KcdWebView(APIView):
    """
    科创板大屏
    """
    def post(self, request, format=None):
        param = request.data
        # param = request.query_params
        page = param.get('page',1)
        size = param.get('size',25)
        order = param.get('order','+sec_code')
        res = {}
        res['count'] = get_data_size()
        res['results'] = get_data_all(page, size, order)
        return Response(res, content_type="application/json")

def get_data_bottom():
    conn = connection
    sql = '''
    select ccrc_id_cnt_total
                ,trade_amt_total
                ,avg_sec_rf
                ,total_rz_buy_amt
                ,total_rq_sell_vol
                ,afrter_trade_amt
                ,cnt_alarm_all
                ,max_1
                ,min_2
                ,avge_price
                ,avge_nego_rate
                ,avge_sy_ratio
        from jcb_data.data_kcb_screen_bottom
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error('获取数据出错:'+str(e))
        logger.error(sql)
        return []
    data = cur.fetchall()
    if len(data)>0:
        row = data[0]
    else:
        return {}
    d = {}
    rowname = [x[0] for x in cur.description]
    for i,col in enumerate(rowname):
        if isinstance(row[i],datetime):
            d[col] = row[i].strftime('%Y/%m/%d %H:%M:%S')
        elif row[i] == None:
            d[col] = 0
        else:
            d[col] = row[i]
    return d 
   
class KcbBottomView(APIView):
    """
    科创板底部数据
    """
    def post(self, request, format=None):
        param = request.data
        # param = request.query_params
        res = {}
        res['results'] = get_data_bottom()
        return Response(res, content_type="application/json")

def get_time_list():
    """
    获取时间刻度
    """
    res = ["09:30","09:31","09:32","09:33","09:34","09:35","09:36","09:37","09:38","09:39","09:40","09:41","09:42","09:43","09:44","09:45","09:46","09:47","09:48","09:49","09:50","09:51","09:52","09:53","09:54","09:55","09:56","09:57","09:58","09:59","10:00","10:01","10:02","10:03","10:04","10:05","10:06","10:07","10:08","10:09","10:10","10:11","10:12","10:13","10:14","10:15","10:16","10:17","10:18","10:19","10:20","10:21","10:22","10:23","10:24","10:25","10:26","10:27","10:28","10:29","10:30","10:31","10:32","10:33","10:34","10:35","10:36","10:37","10:38","10:39","10:40","10:41","10:42","10:43","10:44","10:45","10:46","10:47","10:48","10:49","10:50","10:51","10:52","10:53","10:54","10:55","10:56","10:57","10:58","10:59","11:00","11:01","11:02","11:03","11:04","11:05","11:06","11:07","11:08","11:09","11:10","11:11","11:12","11:13","11:14","11:15","11:16","11:17","11:18","11:19","11:20","11:21","11:22","11:23","11:24","11:25","11:26","11:27","11:28","11:29","11:30","13:00","13:01","13:02","13:03","13:04","13:05","13:06","13:07","13:08","13:09","13:10","13:11","13:12","13:13","13:14","13:15","13:16","13:17","13:18","13:19","13:20","13:21","13:22","13:23","13:24","13:25","13:26","13:27","13:28","13:29","13:30","13:31","13:32","13:33","13:34","13:35","13:36","13:37","13:38","13:39","13:40","13:41","13:42","13:43","13:44","13:45","13:46","13:47","13:48","13:49","13:50","13:51","13:52","13:53","13:54","13:55","13:56","13:57","13:58","13:59","14:00","14:01","14:02","14:03","14:04","14:05","14:06","14:07","14:08","14:09","14:10","14:11","14:12","14:13","14:14","14:15","14:16","14:17","14:18","14:19","14:20","14:21","14:22","14:23","14:24","14:25","14:26","14:27","14:28","14:29","14:30","14:31","14:32","14:33","14:34","14:35","14:36","14:37","14:38","14:39","14:40","14:41","14:42","14:43","14:44","14:45","14:46","14:47","14:48","14:49","14:50","14:51","14:52","14:53","14:54","14:55","14:56","14:57","14:58","14:59","15:00"]
    return res

def getOrcConf():
    try:
        with open('OrcConf','r') as f:
            s = f.read()
        s = str(s)[:3]
        if s == 'N02':
            return 'ORCQueryN02'
        else:
            return 'ORCQueryN03'
    except:
        return 'ORCQueryN03' 

def get_index_data(code='000001'):
    """
    获取指数行情数据
    """
    res = {}
    timeList = get_time_list()
    today = datetime.today().strftime('%Y%m%d')
    # today = '20200719'
    res['timeList'] = timeList
    # conf_conn = getOrcConf() 
    # conn = getConn(conf_conn)
    conn = connection
    cur = conn.cursor()
    sql = '''
         select
        tim,
        S3,
        S8,
         case when tim = '0930' then curr_trade_amt
          else curr_trade_amt-COALESCE(pre_trade_amt,0) end as trade_amt,
        curr_trade_amt
    from(
        select
            tim,
            S3,
            S8,
            s5 as curr_trade_amt,
            lag(s5,1)over(order by tim) as pre_trade_amt
        from jcb_data.show2003) as foo 
    -- where s1 = '%(code)s' and tod = '%(today)s' 
   --  and tim<='1800' and tim>='1640')
   -- and tim >= '0925' and tim <= '1500'
   -- )where tim = '0925'  or (tim between '0930' and '1130')
   --  or (tim between '1300' and '1500')
   --  order by tim    
    ''' % {'today':today,'code':code}
    cur.execute(sql)
    data = cur.fetchall()
    # data =data[1:]
    if len(data)==0:
        res['lastCloseData'] = 0
        res['lineData'] = []
        res['barData'] = []
        res['ampliToday'] = 0 
        res['curr_trade_vol'] = 0
        return res
    else:
        res['lastCloseData'] = data[0][1]
        res['lineData'] = [i[2]  for i in data]
        res['barData'] = [i[3] if i[3]!= None else 0 for i in data  ]
        res['ampliToday'] = round(max([ abs(i - res['lastCloseData']) for i in res['lineData'] ]),2)
        res['curr_trade_vol'] = round(data[-1][4])
        return res

class KcbWeb2ChartView(APIView):
    """
    科创板底部数据
    """
    def post(self, request, format=None):
        param = request.data
        # param = request.query_params
        code = param.get('code','000001')
        res = {}
        res['results'] = get_index_data(code)
        return Response(res, content_type="application/json")
    
def get_data_all_KcbWebNewPlus(order):
    conn = connection
    if len(str(order))== 0:
        order_str ='sec_code'
    else:
        order_str = str(order)[1:]+' '
        if str(order).startswith('-'):
            order_str += 'desc'        
    sql = '''
   select
    a.*
    from (
    select
     a.trade_date 
    ,a.fresh_time    ----刷新时间 
    ,a.sec_code      --证券代码
    ,a.sec_name  
    ,a.list_date     --上市时间
    ,a.tot_amt       --总市值亿元
    ,a.first_vol     --流通股本
    ,a.tot_free_vol  --解禁股票数量
    ,a.open_price    --开盘价
    ,a.new_price     --最新价
    ,a.trade_amt     --成交额 亿元
    ,a.sy_ratio      --市盈率
    ,a.Nego_Rate     --换手率
    ,a.curr_sec_rf   --涨跌幅
    ,a.open_sec_rf    --开盘涨跌幅 
    ,a.kc_gxds        -- 科创50贡献点数
    ,b.ccrc_cnt       --一码通数量
    ,b.net_buy_amt    --最大净买入金额
    ,b.first_buy_acct_name           ---最大净买入账户名称
    ,b.net_sell_amt                  ---最大净卖出金额 
    ,b.first_sell_acct_name          ---最大净卖出账户名称
    ,b.rz_buy_amt                    ---融资买入金额
    ,b.rq_sell_vol                    -----融券卖出量  
    from jcb_data.sec_info_trade_info_res a 
    left join jcb_data.sec_trade_ctr_res b 
    on a.trade_date = b.trade_date
    and a.sec_code = b.sec_code
    ) a order by %(order_str)s nulls last
    ''' % {'order_str':order_str}
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error('获取数据出错:'+str(e))
        logger.error(sql)
        return []
    data = cur.fetchall()
    rowname = [x[0] for x in cur.description]
    result = []
    for row in data:
        d = {}
        for i,col in enumerate(rowname):
            col = col.lower()
            if isinstance(row[i],datetime):
                d[col] = row[i].strftime('%Y/%m/%d %H:%M:%S')
            elif row[i] == None:
                d[col] = '-'
            else:
                d[col] = row[i]
        result.append(d)
    return result
    
class KcbWebNewPlusView(APIView):
    """
    科创50表格页面
    """
    def post(self, request, format=None):
        param = request.data
        # param = request.query_params
        order = param.get('order','+sec_code')
        res = {}
        res['count'] = 50
        res['results'] = get_data_all_KcbWebNewPlus(order)
        return Response(res, content_type="application/json")

def get_data_PriceOverView():
    """
    行情概括 取数
    """
    conn = connection
    sql = '''
   select
          a.trade_date,  
          a.fresh_time,
          a.avg_sec_rf,  --平均涨跌幅
          a.rise_sec_num ,  --涨数量
          a.no_sec_num ,    --平
          a.drop_sec_num,  --跌数量
          a.jj_trade_amt, --竞价成交金额
          a.max_sec_name,                           ---涨幅最大名称
          a.max_sec_rf,         ---涨幅最大
          a.min_sec_name,                           ---跌幅最大名称
          a.min_sec_rf,        ---跌幅最大
          b.active_buy_ratio,   --主动买
          b.active_sell_ratio,  ---主动卖占比
          c.free_sell_amt,      ---解禁卖出金额（亿元）
            c.csum_free_sell_amt,  ---两日累计解禁卖出金额 
          c.ccrc_cnt,   --解禁卖出账户数
          c.free_sell_vol_ratio, --当日解禁占竞价可减持余额占比
          c.csum_free_sell_vol_ratio, ---累计解禁卖出占可卖出股数的比例
          c.free_sell_vol_ratio2,  ----当日解禁卖出占解禁总股数的比例
          c.csum_free_sell_vol_ratio2, ---  累计解禁卖出占解禁总股数的比例
          c.max_ctr_sell_sec_name, ---解禁卖出最大金额股票 名称
          c.max_free_sell_sec,  ---解禁卖出最大金额
          c.max_free_sell_ratio_name,  --当日解禁卖出占比最大个股名称
          c.max_free_sell_ratio_sec,    --当日解禁卖出占比最大
          d.sz_gxds_tot,   --上证综指贡献点数合计
          e.sz_gxds_tot as kc_gxds_tot --科创50g贡献点数合计
    from jcb_data.sec_quoation_detail a 
    left join jcb_data.sec_quoation_detail2 b 
    on 1 = 1
    left join jcb_data.sec_kc_first_free_sell_total c 
    on 1 = 1 
    left join (select sz_gxds_tot from jcb_data.sec_gzds where index_code = '000001')d 
    on 1 = 1 
    left join (select sz_gxds_tot from jcb_data.sec_gzds where index_code = '000688')e 
    on 1 = 1; 
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error('获取数据出错:'+str(e))
        logger.error(sql)
        return []
    data = cur.fetchall()
    rowname = [x[0] for x in cur.description]
    d = {}
    if len(data)==0:
        return d
    else:
        row = data[0]
    for i,col in enumerate(rowname):
        col = col.lower()
        if isinstance(row[i],datetime):
            d[col] = row[i].strftime('%Y/%m/%d %H:%M:%S')
        elif row[i] == None:
            d[col] = '-'
        else:
            d[col] = row[i]
    return d

class KcbLinePriceOverView(APIView):
    """
    科创板首批股票解禁减持监控 行情概括
    """
    def post(self, request, format=None):
        param = request.data
        # param = request.query_params
        res = {}
        d =  get_data_PriceOverView()
        res['results'] = d        
        return Response(res, content_type="application/json")

def get_data_HeatMap():
    """
    热力图 取数
    """
    conn = connection
    sql = '''
    select a.*
    from(
    select
       a.sec_code,
       a.sec_name,
       a.fresh_time,
       a.tot_amt, ---总市值
       a.sec_rf,----涨跌幅
       b.free_sell_vol_ratio    --当日解禁卖出占竞价可卖出股数的比例   
    from jcb_data.sec_hot_pic a 
    left join jcb_data.sec_kc_first_free_sell_dtl b 
    on a.sec_code = b.sec_code
    ) a order by tot_amt desc nulls last;
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error('获取数据出错:'+str(e))
        logger.error(sql)
        return []
    data = cur.fetchall()
    rowname = [x[0] for x in cur.description]
    result = []
    for row in data:
        d = {}
        for i,col in enumerate(rowname):
            col = col.lower()
            if isinstance(row[i],datetime):
                d[col] = row[i].strftime('%Y/%m/%d %H:%M:%S')
            elif row[i] == None:
                d[col] = 0
            else:
                d[col] = row[i]
        result.append(d)
    return result

class HeatMapView(APIView):
    """
    科创板首批股票解禁减持监控 热力图
    """
    def post(self, request, format=None):
        param = request.data
        # param = request.query_params
        res = {}
        res['results'] = get_data_HeatMap()
        return Response(res, content_type="application/json")

def get_data_simpletable():
    """
    对上证综指影响程度前三的科创板个股
    """
    # conn = getConn('PGDev')
    # conn.set_client_encoding('utf-8')
    conn = connection
    sql = '''
    select
      a.index_code, ---指数代码 
      a.gxd_type,  --标题
      a.sec_code,
      a.sec_name,   
      a.trade_amt,  ---成交金额
      a.sec_rf,    ---涨跌幅
      a.gxds,
      b.free_sell_amt, ---当日解禁卖出金额
      b.free_sell_vol_ratio     --当日解禁卖出占竞价可卖出股数的比例      
    from jcb_data.sec_a_gzds a 
    left join jcb_data.sec_kc_first_free_sell_dtl b 
    on a.sec_code = b.sec_code;    
    '''
    res = execcode(sql,'对上证综指影响程度前三的科创板个股',conn)['res']
    conn.close()
    d = {}
    res_mod = []
    if 1==1:
        for i in res[0]:
            val = d.get(i['gxd_type'].strip(),[])
            i['gxd_type'] = i['gxd_type'].strip()
            val.append(i)
            d[i['gxd_type']] = val
        for key in d.keys():
            if key.startswith('正面') and '前三'  in key:
                val = d[key]
                val = sorted(val,key=lambda x:float(x['gxds']), reverse=True)
                d[key] = val
            elif key.startswith('负面') and '前三' in key:
                val = d[key]
                val = sorted(val,key=lambda x:float(x['gxds']), reverse=False)
                d[key] = val
        temp_d = d.get('正面影响点数前三科创个股',[])
        if len(temp_d)<3:
            temp = [{'gxd_type':'正面影响点数前三科创个股','sec_code':'-','sec_name':'-','trade_amt':'-','sec_rf':'-','gxds':'-','free_sell_amt':'-','free_sell_vol_ratio':'-'}] * (3-len(temp_d))
            temp_d.extend(temp)
        res_mod.extend(temp_d)
        res_mod.extend(d.get('正面影响点数第一主板个股',[]))
        if len(d.get('正面影响点数第一主板个股',[]))==0:
            res_mod.append({'gxd_type':'正面影响点数第一主板个股','sec_code':'-','sec_name':'-','trade_amt':'-','sec_rf':'-','gxds':'-','free_sell_amt':'-','free_sell_vol_ratio':'-'})
        temp_e = d.get('负面影响点数前三科创个股',[])
        if len(temp_e)<3:
            temp = [{'gxd_type':'负面影响点数前三科创个股','sec_code':'-','sec_name':'-','trade_amt':'-','sec_rf':'-','gxds':'-','free_sell_amt':'-','free_sell_vol_ratio':'-'}] * (3-len(temp_e))
            temp_e.extend(temp)
        res_mod.extend(temp_e)
        res_mod.extend(d.get('负面影响点数第一主板个股',[]))
        if len(d.get('负面影响点数第一主板个股',[]))==0:
            res_mod.append({'gxd_type':'负面影响点数第一主板个股','sec_code':'-','sec_name':'-','trade_amt':'-','sec_rf':'-','gxds':'-','free_sell_amt':'-','free_sell_vol_ratio':'-'})
        for i,row in enumerate(res_mod):
            if row['trade_amt'] == None:
                res_mod[i]['trade_amt'] = '-'
            if row['free_sell_amt'] == None:
                res_mod[i]['free_sell_amt'] = '-'
            if row['free_sell_vol_ratio'] == None:
                res_mod[i]['free_sell_vol_ratio'] = '-'
        for (i,row) in enumerate(res_mod):
            res_mod[i]['gxd_type'] = res_mod[i]['gxd_type'].replace('点数', '')
        return res_mod
    else:
        return res_mod

class SimpleTableView(APIView):
    """
    科创板首批股票解禁减持监控 对上证综指影响程度前三的科创板个股
    """
    def post(self, request, format=None):
        param = request.data
        # param = request.query_params
        res = {}
        res['results'] = get_data_simpletable()
        return Response(res, content_type="application/json")

def get_data_simpletable2():
    """
    对科创50影响程度前三的科创板个股
    """
    # conn = getConn('PGDev')
    # conn.set_client_encoding('utf-8')
    conn = connection
    sql = '''
   select
      a.index_code, ---指数代码 
      a.gxd_type,  --标题
      a.sec_code,
      a.sec_name,   
      a.trade_amt,  ---成交金额
      a.sec_rf,    ---涨跌幅
      a.gxds,
      b.free_sell_amt, ---当日解禁卖出金额
      b.free_sell_vol_ratio     --当日解禁卖出占竞价可卖出股数的比例      
from jcb_data.sec_a_gzds a 
left join jcb_data.sec_kc_first_free_sell_dtl b 
on a.sec_code = b.sec_code;
     '''
    res = execcode(sql,'对上证综指影响程度前三的科创板个股',conn)['res']
    conn.close()
    d = {}
    res_mod = []
    if 1==1:
        for i in res[0]:
            val = d.get(i['gxd_type'].strip(),[])
            i['gxd_type'] = i['gxd_type'].strip()
            val.append(i)
            d[i['gxd_type']] = val
        for key in d.keys():
            if key.startswith('正面') and '前三'  in key:
                val = d[key]
                val = sorted(val,key=lambda x:float(x['gxds']), reverse=True)
                d[key] = val
            elif key.startswith('负面') and '前三' in key:
                val = d[key]
                val = sorted(val,key=lambda x:float(x['gxds']), reverse=False)
                d[key] = val
        temp_d = d.get('正面影响点数前三个股',[])
        if len(temp_d)<3:
            temp = [{'gxd_type':'正面影响点数前三个股','sec_code':'-','sec_name':'-','trade_amt':'-','sec_rf':'-','gxds':'-','free_sell_amt':'-','free_sell_vol_ratio':'-'}] * (3-len(temp_d))
            temp_d.extend(temp)
        res_mod.extend(temp_d)
        temp_e = d.get('负面影响点数前三个股',[])
        if len(temp_e)<3:
            temp = [{'gxd_type':'负面影响点数前三个股','sec_code':'-','sec_name':'-','trade_amt':'-','sec_rf':'-','gxds':'-','free_sell_amt':'-','free_sell_vol_ratio':'-'}] * (3-len(temp_e))
            temp_e.extend(temp)
        res_mod.extend(temp_e)
        for i,row in enumerate(res_mod):
            if row['trade_amt'] == None:
                res_mod[i]['trade_amt'] = '-'
            if row['free_sell_amt'] == None:
                res_mod[i]['free_sell_amt'] = '-'
            if row['free_sell_vol_ratio'] == None:
                res_mod[i]['free_sell_vol_ratio'] = '-'
        for (i,row) in enumerate(res_mod):
            res_mod[i]['gxd_type'] = res_mod[i]['gxd_type'].replace('点数', '')
        return res_mod
    else:
        return res_mod

class SimpleTable2View(APIView):
    """
    科创板首批股票解禁减持监控 对科创50影响程度前三的科创板个股
    """
    def post(self, request, format=None):
        param = request.data
        # param = request.query_params
        res = {}
        res['results'] = get_data_simpletable2()
        return Response(res, content_type="application/json")

def get_data_all_KcbWebNew(order):
    conn = connection
    if len(str(order))== 0:
        order_str ='sec_code'
    else:
        order_str = str(order)[1:]+' '
        if str(order).startswith('-'):
            order_str += 'desc'        
    sql = '''
    select a.*
    from(
    select
    a.*,
    b.free_sell_amt, ---当日解禁卖出金额 
    b.max_free_sell_amt, --当日解禁卖出金额第一
    b.max_acct_name,  ---解禁卖出金额第一的账户名称
    b.free_sell_vol_ratio, --当日解禁卖出占竞价可卖出股数的比例
    b.free_sell_vol_ratio2, --当日解禁卖出占解禁总股数的比例
    b.free_sell_vol_ratio3,---当日解禁卖出占成交量比例  
    b.csum_free_sell_vol_ratio2 --累计解禁卖出占解禁总股数的比例
    from jcb_data.sec_kc_first_free_info a 
    left join jcb_data.sec_kc_first_free_sell_dtl b 
    on a.sec_code = b.sec_code
    and a.trade_date = b.trade_date
    ) a order by %(order_str)s nulls last
    ''' % {'order_str':order_str}
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error('获取数据出错:'+str(e))
        logger.error(sql)
        return []
    data = cur.fetchall()
    rowname = [x[0] for x in cur.description]
    result = []
    for row in data:
        d = {}
        for i,col in enumerate(rowname):
            col = col.lower()
            if isinstance(row[i],datetime):
                d[col] = row[i].strftime('%Y/%m/%d %H:%M:%S')
            elif row[i] == None:                                
                d[col] = '-'
            else:
                d[col] = row[i]
        result.append(d)
    return result

class KcbWebNewView(APIView):
    """
    科创板表格25只
    """
    def post(self, request, format=None):
        param = request.data
        # param = request.query_params
        order = param.get('order','+sec_code')
        res = {}
        res['results'] = get_data_all_KcbWebNew(order)
        return Response(res, content_type="application/json")
