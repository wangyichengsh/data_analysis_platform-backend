from django.db import models
from django.contrib.auth import get_user_model
from Auth.models import UserGroup

User = get_user_model()

# Create your models here.

'''
class LogDtl(models.Model):
    class Meta:
        verbose_name = "审计明细"
        verbose_name_plural = '审计明细'
        db_table = 'audit_log_dtl'

    # user_id = models.IntegerField(verbose_name='用户id')           # 数据量较大，不使用外键
    user_id = models.ForeignKey(verbose_name='用户id',to=User,on_delete=models.SET_NULL())
    user_name = models.CharField(verbose_name='用户名',max_length=150,)
    full_name = models.CharField(verbose_name='人员名称',max_length=150,)
    ip = models.IPAddressField(verbose_name='ip地址')
    module_type = models.CharField(verbose_name='模块类型',max_length=150,)
    module_name = models.CharField(verbose_name='模块名称',max_length=150,)
    acct_ids = models.TextField(verbose_name='账户代码')
    sec_codes = models.TextField(verbose_name='股票代码')
    log_dtl = models.TextField(verbose_name='日志明细')
    log_datetime = models.DateTimeField(verbose_name='日志时间')


class LogAcct(models.Model):
    class Meta:
        db_table = 'audit_log_acct'

    log_id = models.IntegerField(verbose_name='日志id')           # 数据量较大，不使用外键
    acct_id = models.CharField(verbose_name='账户代码',max_length=10,)


class LogSec(models.Model):
    class Meta:
        db_table = 'audit_log_sec'

    log_id = models.IntegerField(verbose_name='日志id')           # 数据量较大，不使用外键
    sec_code = models.CharField(verbose_name='股票代码',max_length=6,)
'''

# 系统访问量按月变化情况表
# class AuditSystemMonth(models.Model):
#     class Meta:
#         db_table = 'audit_system_month'
#         verbose_name = '系统访问量按月变化情况表'
#         verbose_name_plural = '系统访问量按月变化情况表'
#
#     sys_name = models.CharField(verbose_name='系统名称',max_length=50)
#     group_name = models.CharField(verbose_name='小组名称',max_length=50)
#     month = models.CharField(verbose_name='月份', max_length=6, )
#     cnt_query = models.IntegerField(verbose_name='查询次数')


# 系统访问量按周变化情况表
# class AuditSystemWeek(models.Model):
#     class Meta:
#         db_table = 'audit_system_week'
#         verbose_name = '系统访问量按周变化情况表'
#         verbose_name_plural = '系统访周' \
#                               '' \
#                               '量按月变化情况表'
#
#     sys_name = models.CharField(verbose_name='系统名称',max_length=50)
#     group_name = models.CharField(verbose_name='小组名称',max_length=50)
#     month = models.CharField(verbose_name='月份', max_length=6, )
#     cnt_query = models.IntegerField(verbose_name='查询次数')


# 模块访问量按月变化情况表


# 模块访问量按周变化情况表



AuditTimeType = {
    1:'月度审计',
    2:'季度审计'
}

AuditRuleType = (
    (1,'跨组查看股票'),
    (2,'频繁查看股票'),
    (3,'频繁查看账户'),
    (4,'查询敏感账户'),
    (5,'非监控室IP登录'),
    (6,'非交易日登录'),
    (7,'实时组查询分类投资者情况'),
    (8,'分析组查询个股情况'),
)

# 审计规则列表
class AuditRuleList(models.Model):
    class Meta:
        db_table = 'audit_rule_list'
        verbose_name = '审计规则列表'
        verbose_name_plural = '审计规则列表'
        permissions = (
            ('audit_group_editor','可编辑本组审计数据'),
            ('audit_export','可导出部门审计报告'),
        )

    rule_name = models.CharField(verbose_name='审计规则名称',max_length=50)
    model_name = models.CharField(verbose_name='模型名称',max_length=50,default='')


# 审计规则明细表
class AuditRuleDetail(models.Model):
    class Meta:
        db_table = 'audit_rule_detail'
        verbose_name = '审计规则明细'
        verbose_name_plural = '审计规则明细'

    # audit_rule_type = models.IntegerField(verbose_name='审计规则类型',default=1,choices=AuditRuleType)
    audit_rule_type = models.CharField(verbose_name='审计规则名称', max_length=50,blank=True,null=True)
    group_name = models.CharField(verbose_name='小组名称',max_length=50)
    full_name = models.CharField(verbose_name='人员名称', max_length=50, )
    ip = models.CharField(verbose_name='ip地址',max_length=20,blank=True,null=True)
    sys_name = models.CharField(verbose_name='系统名称',max_length=50,blank=True,null=True)
    module_type = models.CharField(verbose_name='模块类型',max_length=255,blank=True,null=True)
    module_name = models.CharField(verbose_name='模块名称', max_length=255,blank=True,null=True)
    acct_ids = models.TextField(verbose_name='账户代码',blank=True,null=True)
    sec_codes = models.TextField(verbose_name='股票代码',blank=True,null=True)
    log_dtl = models.TextField(verbose_name='日志明细',blank=True,null=True)
    log_time = models.DateTimeField(verbose_name='操作时间',blank=True,null=True)
    month = models.IntegerField(verbose_name='月份')




# 跨组查看股票(汇总)
class AuditOtherGroupSec(models.Model):
    class Meta:
        db_table = 'audit_other_group_sec'
        verbose_name = '跨组查看股票(汇总)'
        verbose_name_plural = '跨组查看股票(汇总)'
        ordering = ['week_start','group_name','full_name','sys_name','sec_code']

    sys_name = models.CharField(verbose_name='系统名称',max_length=50,blank=True,null=True)
    group_name = models.CharField(verbose_name='小组名称',max_length=50)
    full_name = models.CharField(verbose_name='人员名称', max_length=50, )
    sec_code = models.CharField(verbose_name='股票代码',max_length=6,blank=True,null=True)
    sec_group = models.CharField(verbose_name='股票所属组名',max_length=50,blank=True,null=True)
    week_start = models.DateField(verbose_name='周初',blank=True,null=True)
    week_end = models.DateField(verbose_name='周末',blank=True,null=True)
    cnt_query = models.IntegerField(verbose_name='查询次数',blank=True,null=True)
    cnt_date = models.IntegerField(verbose_name='查询天数',blank=True,null=True)
    remark =  models.CharField(verbose_name='备注', max_length=200,blank=True,null=True)
    month = models.IntegerField(verbose_name='月份')


# 频繁查看股票(汇总)
class AuditFreqSec(models.Model):
    class Meta:
        db_table = 'audit_freq_sec'
        verbose_name = '频繁查看股票(汇总)'
        verbose_name_plural = '频繁查看股票(汇总)'
        ordering = ['week_start','group_name','full_name','sys_name','sec_code']

    sys_name = models.CharField(verbose_name='系统名称', max_length=50)
    group_name = models.CharField(verbose_name='小组名称', max_length=50)
    full_name = models.CharField(verbose_name='人员名称', max_length=50, )
    sec_code = models.CharField(verbose_name='股票代码', max_length=6)
    week_start = models.DateField(verbose_name='周初')
    week_end = models.DateField(verbose_name='周末')
    cnt_query = models.IntegerField(verbose_name='查询次数')
    cnt_date = models.IntegerField(verbose_name='查询天数')
    remark = models.CharField(verbose_name='备注', max_length=200, default='',null=True)
    month = models.IntegerField(verbose_name='月份')


# 频繁查看账户(汇总)
class AuditFreqAcct(models.Model):
    class Meta:
        db_table = 'audit_freq_acct'
        verbose_name = '频繁查看账户(汇总)'
        verbose_name_plural = '频繁查看账户(汇总)'
        ordering = ['week_start','group_name','full_name','sys_name','acct_id']

    sys_name = models.CharField(verbose_name='系统名称', max_length=50)
    group_name = models.CharField(verbose_name='小组名称', max_length=50)
    full_name = models.CharField(verbose_name='人员名称', max_length=50, )
    acct_id = models.CharField(verbose_name='账户代码', max_length=10)
    week_start = models.DateField(verbose_name='周初')
    week_end = models.DateField(verbose_name='周末')
    cnt_query = models.IntegerField(verbose_name='查询次数')
    cnt_date = models.IntegerField(verbose_name='查询天数')
    remark = models.CharField(verbose_name='备注', max_length=200, default='',null=True)
    month = models.IntegerField(verbose_name='月份')


# 查询敏感账户(汇总)
class AuditSensAcct(models.Model):
    class Meta:
        db_table = 'audit_sens_acct'
        verbose_name = '查询敏感账户(汇总)'
        verbose_name_plural = '查询敏感账户(汇总)'
        ordering = ['week_start','group_name','full_name','sys_name','acct_id']

    sys_name = models.CharField(verbose_name='系统名称', max_length=50)
    group_name = models.CharField(verbose_name='小组名称', max_length=50)
    full_name = models.CharField(verbose_name='人员名称', max_length=50, )
    acct_id = models.CharField(verbose_name='账户代码', max_length=10)
    week_start = models.DateField(verbose_name='周初')
    week_end = models.DateField(verbose_name='周末')
    cnt_query = models.IntegerField(verbose_name='查询次数')
    cnt_date = models.IntegerField(verbose_name='查询天数')
    remark = models.CharField(verbose_name='备注', max_length=200, default='',null=True)
    month = models.IntegerField(verbose_name='月份')


# 非监控室IP登录(汇总)
class AuditOtherIp(models.Model):
    class Meta:
        db_table = 'audit_other_ip'
        verbose_name = '非监控室IP登录(汇总)'
        verbose_name_plural = '非监控室IP登录(汇总)'
        ordering = ['month','group_name','full_name','sys_name','ip']

    sys_name = models.CharField(verbose_name='系统名称', max_length=50)
    group_name = models.CharField(verbose_name='小组名称', max_length=50)
    full_name = models.CharField(verbose_name='人员名称', max_length=50, )
    ip = models.CharField(verbose_name='ip地址', max_length=15)
    # week_start = models.DateField(verbose_name='周初')
    # week_end = models.DateField(verbose_name='周末')
    module_type = models.CharField(verbose_name='模块类型',max_length=255,blank=True,null=True)
    module_name = models.CharField(verbose_name='模块名称', max_length=255,blank=True,null=True)
    cnt_query = models.IntegerField(verbose_name='查询次数')
    remark = models.CharField(verbose_name='备注', max_length=200, default='',blank=True,null=True)
    month = models.IntegerField(verbose_name='月份')


# 非交易日登录(汇总)
class AuditNontrade(models.Model):
    class Meta:
        db_table = 'audit_nontrade'
        verbose_name = '非交易日登录(汇总)'
        verbose_name_plural = '非交易日登录(汇总)'
        ordering = ['log_date','group_name','full_name','sys_name','module_name']

    sys_name = models.CharField(verbose_name='系统名称', max_length=50)
    group_name = models.CharField(verbose_name='小组名称', max_length=50)
    full_name = models.CharField(verbose_name='人员名称', max_length=50, )
    # ip = models.CharField(verbose_name='ip地址', max_length=15,null=True)
    module_type = models.CharField(verbose_name='模块类型',max_length=255,blank=True,null=True)
    module_name = models.CharField(verbose_name='模块名称', max_length=255,blank=True,null=True)
    log_date = models.DateField(verbose_name='操作日期',blank=True,null=True)
    start_time = models.TimeField(verbose_name='起始时间')
    end_time = models.TimeField(verbose_name='截止时间')
    cnt_query = models.IntegerField(verbose_name='查询次数')
    remark = models.CharField(verbose_name='备注', max_length=200, default='',blank=True,null=True)
    month = models.IntegerField(verbose_name='月份')


# 实时组查询分类投资者情况(汇总)
class AuditQueryAcctType(models.Model):
    class Meta:
        db_table = 'audit_query_acct_type'
        verbose_name = '实时组查询分类投资者情况(汇总)'
        verbose_name_plural = '实时组查询分类投资者情况(汇总)'
        ordering = ['week_start','group_name','full_name','sys_name','module_name']

    sys_name = models.CharField(verbose_name='系统名称', max_length=50)
    group_name = models.CharField(verbose_name='小组名称', max_length=50)
    full_name = models.CharField(verbose_name='人员名称', max_length=50, )
    module_type = models.CharField(verbose_name='模块类型',max_length=255,blank=True,null=True)
    module_name = models.CharField(verbose_name='模块名称', max_length=255,blank=True,null=True)
    week_start = models.DateField(verbose_name='周初')
    week_end = models.DateField(verbose_name='周末')
    cnt_query = models.IntegerField(verbose_name='查询次数')
    remark = models.CharField(verbose_name='备注', max_length=200, default='',blank=True,null=True)
    month = models.IntegerField(verbose_name='月份')


# 分析组查询个股情况(汇总)
class AuditQuerySec(models.Model):
    class Meta:
        db_table = 'audit_query_sec'
        verbose_name = '分析组查询个股情况(汇总)'
        verbose_name_plural = '分析组查询个股情况(汇总)'
        ordering = ['week_start','group_name','full_name','sys_name','module_name']

    sys_name = models.CharField(verbose_name='系统名称', max_length=50)
    group_name = models.CharField(verbose_name='小组名称', max_length=50)
    full_name = models.CharField(verbose_name='人员名称', max_length=50, )
    module_type = models.CharField(verbose_name='模块类型',max_length=255,blank=True,null=True)
    module_name = models.CharField(verbose_name='模块名称', max_length=255,blank=True,null=True)
    week_start = models.DateField(verbose_name='周初')
    week_end = models.DateField(verbose_name='周末')
    cnt_query = models.IntegerField(verbose_name='查询次数')
    remark = models.CharField(verbose_name='备注', max_length=200, default='',blank=True,null=True)
    month = models.IntegerField(verbose_name='月份')


# 月份表
class AuditMonth(models.Model):
    class Meta:
        db_table = 'audit_month'
        verbose_name = '审计月份'
        verbose_name_plural = '审计月份'
        ordering = ["-month_start"]

    month_show = models.CharField(verbose_name='月份',max_length=10)
    month = models.IntegerField(verbose_name='月份(yyyymmdd)')
    month_start = models.DateField(verbose_name='月初')
    month_end = models.DateField(verbose_name='月末')


    def __str__(self):
        return '%s' % (self.month_show)
