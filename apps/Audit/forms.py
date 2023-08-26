from django.forms import ModelForm
from django import forms
from Audit.models import AuditMonth
# from Audit.models import AuditRuleDetail,AuditOtherGroupSec,AuditFreqSec,AuditFreqAcct,AuditSensAcct,AuditOtherIp,AuditNontrade,AuditQueryAcctType,AuditQuerySec,AuditMonth,AuditRuleList


class MonthChoiceForm(forms.Form):
    start_month = forms.ModelChoiceField(queryset=AuditMonth.objects.all(),label="起始月份",initial=True)
    end_month = forms.ModelChoiceField(queryset=AuditMonth.objects.all(),label="截止月份",initial=True)


# class AuditOtherGroupSecForm(forms.ModelForm):
#     pass
