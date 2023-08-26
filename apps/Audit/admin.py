from django.contrib import admin
from Audit.models import AuditRuleDetail,AuditOtherGroupSec,AuditFreqSec,AuditFreqAcct,AuditSensAcct,AuditOtherIp,AuditNontrade,AuditQueryAcctType,AuditQuerySec,AuditMonth
# Register your models here.


@admin.register(AuditRuleDetail)
class AuditRuleDetailAdmin(admin.ModelAdmin):
    pass


@admin.register(AuditOtherGroupSec)
class AuditOtherGroupSecAdmin(admin.ModelAdmin):
    pass


@admin.register(AuditFreqSec)
class AuditFreqSecAdmin(admin.ModelAdmin):
    pass


@admin.register(AuditFreqAcct)
class AuditFreqAcctAdmin(admin.ModelAdmin):
    pass


@admin.register(AuditSensAcct)
class AuditSensAcctAdmin(admin.ModelAdmin):
    pass


@admin.register(AuditOtherIp)
class AuditOtherIpAdmin(admin.ModelAdmin):
    pass


@admin.register(AuditNontrade)
class AuditNontradeAdmin(admin.ModelAdmin):
    pass


@admin.register(AuditQueryAcctType)
class AuditQueryAcctTypeAdmin(admin.ModelAdmin):
    pass


@admin.register(AuditQuerySec)
class AuditRuleDetailAdmin(admin.ModelAdmin):
    pass


@admin.register(AuditMonth)
class AuditMonthAdmin(admin.ModelAdmin):
    pass



