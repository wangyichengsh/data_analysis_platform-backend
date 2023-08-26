from utils.jinja_sql import JinjaScript
from contextlib import closing
import os
import datetime

with open('D:\\jzhang\\Git\\anls_django\\apps\\ComplexApp\\jsql\\acct_relation\\execute.jsql','r',encoding='utf8') as f:
    jsql = f.read()

data = {
    'acct_id':[[1,'A']],
    'sec_code':[[1,'B']],
    'start_date':datetime.date.today(),
    'end_date':datetime.date.today()
}


j = JinjaScript()

sql_list = j.render(jsql, data)
print(sql_list)
with open('d:/a.sql','w+') as f:
    for sql in sql_list:
        f.write(sql['statment'])

