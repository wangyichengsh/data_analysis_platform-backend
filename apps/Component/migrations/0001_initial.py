# Generated by Django 2.2.5 on 2020-07-13 09:09

from django.conf import settings
import django.contrib.postgres.fields
import django.contrib.postgres.fields.jsonb
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='AppConfig',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('is_valid', models.BooleanField(default=True, verbose_name='是否有效')),
                ('data_create_time', models.DateTimeField(auto_now_add=True, verbose_name='数据创建日期')),
                ('data_update_time', models.DateTimeField(auto_now=True, verbose_name='数据更新日期')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='AppModelConfig',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('is_valid', models.BooleanField(default=True, verbose_name='是否有效')),
                ('data_create_time', models.DateTimeField(auto_now_add=True, verbose_name='数据创建日期')),
                ('data_update_time', models.DateTimeField(auto_now=True, verbose_name='数据更新日期')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='DateCanlendar',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('calendar_date', models.DateField(verbose_name='日期')),
                ('day_of_week', models.IntegerField(verbose_name='周内第几天')),
                ('day_of_month', models.IntegerField(verbose_name='月内第几天')),
                ('day_of_year', models.IntegerField(verbose_name='年内第几天')),
                ('is_mkt_sh', models.IntegerField(verbose_name='是否沪市交易日（1：是，0：否）')),
                ('is_week_end', models.IntegerField(verbose_name='是否周末（1：是，0：否）')),
            ],
            options={
                'verbose_name': '日历',
                'verbose_name_plural': '日历',
                'db_table': 'component_date_canlendar',
            },
        ),
        migrations.CreateModel(
            name='AppExecuteHistory',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('is_valid', models.BooleanField(default=True, verbose_name='是否有效')),
                ('data_create_time', models.DateTimeField(auto_now_add=True, verbose_name='数据创建日期')),
                ('data_update_time', models.DateTimeField(auto_now=True, verbose_name='数据更新日期')),
                ('query_id', models.UUIDField(verbose_name='查询id')),
                ('app_type', models.CharField(max_length=50, verbose_name='应用类型')),
                ('app_id', models.IntegerField(verbose_name='应用')),
                ('app_name', models.CharField(blank=True, default='', max_length=200, null=True, verbose_name='应用名称')),
                ('exec_status', models.SmallIntegerField(choices=[(-1, '查询失败'), (0, '查询中'), (1, '查询成功')], default=0, verbose_name='执行状态')),
                ('pid', models.IntegerField(blank=True, null=True, verbose_name='进程id')),
                ('parameter', django.contrib.postgres.fields.jsonb.JSONField(blank=True, null=True, verbose_name='应用参数')),
                ('result_table', django.contrib.postgres.fields.ArrayField(base_field=models.CharField(blank=True, max_length=200), null=True, size=None, verbose_name='应用结果表')),
                ('result_file_name', models.CharField(blank=True, default='', max_length=100, null=True, verbose_name='结果文件名')),
                ('result_file_path', models.CharField(blank=True, default='', max_length=255, null=True, verbose_name='结果文件路径')),
                ('execute_start_time', models.DateTimeField(auto_now_add=True, verbose_name='执行开始时间')),
                ('execute_end_time', models.DateTimeField(blank=True, default=None, null=True, verbose_name='执行结束时间')),
                ('log_file_name', models.CharField(blank=True, default='', max_length=100, null=True, verbose_name='日志文件名')),
                ('log_file_path', models.CharField(blank=True, default='', max_length=100, null=True, verbose_name='日志文件路径')),
                ('remark', models.CharField(blank=True, default='', max_length=200, null=True, verbose_name='备注')),
                ('has_viewed', models.BooleanField(default=False, verbose_name='已查看')),
                ('user', models.ForeignKey(db_constraint=False, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL, verbose_name='执行人员')),
            ],
            options={
                'verbose_name': '应用执行历史',
                'verbose_name_plural': '应用执行历史',
                'db_table': 'component_app_exec_his',
                'permissions': (('view_all', '查看全部记录'), ('view_group', '查看本组记录'), ('view_log', '查看日志'), ('kill_pid', '关闭进程')),
            },
        ),
    ]
