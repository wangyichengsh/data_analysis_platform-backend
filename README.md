Data Analysis Platform Backend
==============================
A system allowed SQL developers customize query web pages including inputs,outputs and SQL etc.

[Demo](http://129.211.26.73:8080) (You can login in as a [developer](http://129.211.26.73:8080/login?password=deve%401234%21&username=deve1) or as a [user](http://129.211.26.73:8080/login?password=12345678%21&username=user1)) 

[User guide](https://github.com/wangyichengsh/data_analysis_platform-frontend/blob/main/docs/user_guide.md) 


Installation
============
```bash
pip3 install -r requirements.txt
```

unzip css files and js files for frontend(optional)
```
cd static && unzip admin.zip && unzip codemirror.zip
rm admin.zip codemirror.zip
cd ..
```


## Configuration
Web configurations are in `AnlsTool/setting/setting.py`. Modify it with database setting, as following:
```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'anls',
        'USER': 'user',
        'PASSWORD': 'password',
        'HOST': 'localhost',
        'PORT': 5432,
    }
}
```

Database configurations are in `apps/utils/conf/db.conf`. Modify it as following:

```
[PSGQuery]
db_type = postgre
host = localhost
port = 5432
database = opendata
user = username
password = password

[ORCQuery]
db_type = oracle
host = localhost
port = 1521
database = orcl
user = username
password = password

[MysqlQuery]
db_type = mysql
host = localhost
port = 3306
database = name
user = user
password = passowrd
```
Also edit the `func_name` in `db_tools/import_nt_data.py` according to the name in `apps/utils/conf/db.conf` as following:
```python
exec_info = [{'exec_id':'1','chinese_name':'Postgre','func_name':'PSGQuery','db_type':0},
             {'exec_id':'2','chinese_name':'Oracle','func_name':'ORCQuery','db_type':1},
             {'exec_id':'3','chinese_name':'MySQL','func_name':'MysqlQuery','db_type':2}]
```
|db_type|database|
|-------|--------|
|0|Postgre|
|1|Oracle|
|2|MySQL| 

Create database
```sql
CREATE DATABASE anls OWNER user;
```

Run the following command in Terminal:
```bash
python3 manage.py makemigrations
python3 manage.py migrate
```

Create super user
```bash
python3 manage.py createsuperuser
```

Create base data
```bash
python3 db_tools/import_nt_data.py
```


Run
=====
```console
$ uwsgi --ini uwsgi.ini
```

