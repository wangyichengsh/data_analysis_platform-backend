from django.db import models
from django.db.models import DateField, DateTimeField, sql,Max,Q
from django.contrib.auth import get_user_model
from datetime import datetime


from NormalTask.models import File2Task, File, ParentTask, Version, Task, ExecFunction, Input, InputFileSheet, InputFileColumn, OutputSheet, OutputColumn, SqlCode, JobHistory, ChangeHistory
# 引用自定义User模型
# User = get_user_model()
AUTH_USER_MODEL = 'Auth.User'

# Create your models here.

class FeedRecord(models.Model):
    """
    反馈记录
    """
    demind_id = models.ForeignKey(to=ParentTask, null=True, blank=True, on_delete=models.CASCADE, verbose_name='反馈针对需求')
    demind_seq = models.IntegerField(verbose_name='需求SEQ')
    task_id = models.ForeignKey(verbose_name='反馈针对子任务', to=Task , on_delete=models.CASCADE, null=True, blank=True)
    model_name =  models.CharField(verbose_name='模型',max_length=50)
    model_id = models.IntegerField(verbose_name='反馈针对id')
    model_title = models.CharField(verbose_name='模型名称', max_length=100, default='')
    feedto = models.TextField('提问内容', default='', help_text='提问内容')
    feedto_user = models.ForeignKey(verbose_name='提问用户', related_name='feedto_user',to=AUTH_USER_MODEL, on_delete=models.CASCADE)
    feedto_time = models.DateTimeField('提问时间', default=datetime.now)
    feedback = models.TextField('反馈内容', default='', help_text='反馈内容')
    feedback_user =  models.ForeignKey(verbose_name='反馈用户', related_name='feedback_user', to=AUTH_USER_MODEL, on_delete=models.CASCADE, null=True, blank=True)
    feedback_time = models.DateTimeField('反馈时间',  null=True, blank=True)
    if_feed = models.BooleanField(verbose_name='是否反馈(t:反馈,f:未反馈)', default=False)
