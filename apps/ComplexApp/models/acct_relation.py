'''
账户关联模型
'''
from django.db import models
from django.contrib.auth import get_user_model
from utils.base_models import BaseIsvalidModel, BaseModel
from utils.db import DjangoDbHook

User = get_user_model()


# 无效终端
class InvalidDevice(BaseIsvalidModel):
    """
    根据业务规则，直接剔除的无效终端
    """
    class Meta:
        verbose_name = "无效终端"
        verbose_name_plural = '无效终端'
        db_table = 'app_invalid_device'

    DeviceType = (
        ('ip', 'IP'),
        ('mac', 'MAC'),
        ('telephone', '联系电话'),
        ('hardware', '硬盘序列号'),
    )

    device_type = models.CharField(verbose_name='无效终端类型', max_length=20)
    device = models.CharField(verbose_name='无效终端', max_length=50)
    create_by = models.ForeignKey(verbose_name='创建人员', to=User,on_delete=models.SET_NULL,db_constraint=False,null=True,blank=True,related_name='invalid_device_create_by')
    reason = models.CharField(verbose_name='原因', max_length=200)


