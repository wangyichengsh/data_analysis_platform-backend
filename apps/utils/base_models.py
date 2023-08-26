from django.db import models


class BaseModel(models.Model):
    # 设置管理者
    # objects = BaseModelManager()

    class Meta:
        # 抽象表，不会在数据库中创建，作为其他模型的基表使用
        abstract = True

    # 标记逻辑删除
    is_valid = models.BooleanField(verbose_name='是否有效',default=True)
    data_create_time = models.DateTimeField(verbose_name='数据创建日期',auto_now_add=True)
    data_update_time = models.DateTimeField(verbose_name='数据更新日期', auto_now=True)

    # 重写delete方法，执行delete时将is_valid置为False
    def delete(self, using=None, keep_parents=False):
        self.is_valid = False
        self.save()

    @property
    def exclude_fields(self):
        return ['is_valid','data_create_time','data_update_time',self._meta.pk.name]


# 仅能查询is_valid的模型
class BaseIsvalidModel(BaseModel):
    class Meta:
        # 抽象表，不会在数据库中创建，作为其他模型的基表使用
        abstract = True


class BaseIsvalidModelManager(models.Manager):
    class Meta:
        # 抽象表，不会在数据库中创建，作为其他模型的基表使用
        abstract = True

    # 仅可查询is_valid=True的数据
    def get_queryset(self):
        return super(BaseIsvalidModel,self).get_queryset().filter(is_valid = True).defer('is_valid')




# 外键等关联字段，加入db_constraint=False,则可去除模型的关联，且仍能通过orm关联查询。此时应通过逻辑代码自行保持数据一致性。
