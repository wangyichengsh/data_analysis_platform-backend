from django.db import models
from django.contrib.postgres.fields import JSONField,ArrayField
from django.contrib.auth import get_user_model
from utils.base_models import BaseIsvalidModel

User = get_user_model()


# 日历表
class DateCanlendar(models.Model):
    class Meta:
        verbose_name = '日历'
        verbose_name_plural = verbose_name
        db_table = 'component_date_canlendar'

    calendar_date = models.DateField(verbose_name='日期')
    day_of_week = models.IntegerField(verbose_name='周内第几天')
    day_of_month = models.IntegerField(verbose_name='月内第几天')
    day_of_year = models.IntegerField(verbose_name='年内第几天')
    is_mkt_sh = models.IntegerField(verbose_name='是否沪市交易日（1：是，0：否）')
    is_week_end = models.IntegerField(verbose_name='是否周末（1：是，0：否）')


# 应用执行历史
class AppExecuteHistory(BaseIsvalidModel):
    class Meta:
        verbose_name = '应用执行历史'
        verbose_name_plural = verbose_name
        db_table = 'component_app_exec_his'
        permissions = (
            ('view_all','查看全部记录'),
            ('view_group','查看本组记录'),
            ('view_log','查看日志'),
            ('kill_pid','关闭进程')
        )
        ordering=('-id',)

    Status = (
        (-1, '查询失败'),
        (0, '查询中'),
        (1, '查询成功'),
        # (2, '已强行中断'),
    )
    query_id = models.UUIDField(verbose_name='查询id')
    user = models.ForeignKey(verbose_name='执行人员', to=User, on_delete=models.PROTECT, db_constraint=False)
    app_type = models.CharField(verbose_name='应用类型', max_length=(50))
    # 后续应该为外键
    app_id = models.IntegerField(verbose_name='应用')
    app_name = models.CharField(verbose_name='应用名称',max_length=200,null=True,blank=True,default='')
    exec_status = models.SmallIntegerField(verbose_name='执行状态',default=0,choices=Status)
    pid = models.IntegerField(verbose_name='进程id',null=True,blank=True)
    parameter = JSONField(verbose_name='应用参数',null=True,blank=True)
    result_table = ArrayField(verbose_name='应用结果表', base_field=models.CharField(max_length=200, blank=True),null=True)
    result_file_name = models.CharField(verbose_name='结果文件名',max_length=100,null=True,blank=True,default='')
    result_file_path = models.CharField(verbose_name='结果文件路径',max_length=255,null=True,blank=True,default='')
    execute_start_time = models.DateTimeField(verbose_name='执行开始时间',auto_now_add=True)
    execute_end_time = models.DateTimeField(verbose_name='执行结束时间',null=True,blank=True,default=None)
    log_file_name = models.CharField(verbose_name='日志文件名',max_length=(100),null=True,blank=True,default='')
    log_file_path = models.CharField(verbose_name='日志文件路径',max_length=100,null=True,blank=True,default='')
    remark = models.CharField(verbose_name='备注',max_length=200,null=True,blank=True,default='')
    has_viewed = models.BooleanField(verbose_name='已查看',default=False)


# 应用模型配置
class AppModelConfig(BaseIsvalidModel):
    pass


# 应用配置
class AppConfig(BaseIsvalidModel):
    pass