import json
from rest_framework import serializers
from Audit.models import AuditRuleDetail,AuditOtherGroupSec,AuditFreqSec,AuditFreqAcct,AuditSensAcct,AuditOtherIp,AuditNontrade,AuditQueryAcctType,AuditQuerySec,AuditMonth,AuditRuleList
from .models import AuditRuleList, AuditRuleDetail, AuditOtherGroupSec, AuditFreqSec,  AuditFreqAcct, AuditSensAcct, AuditOtherIp, AuditNontrade, AuditQueryAcctType, AuditQuerySec, AuditMonth

class AuditRuleListSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditRuleList 
        fields = "__all__"


class AuditRuleDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditRuleDetail
        # exclude = ['month']
        fields = "__all__"


class AuditOtherGroupSecSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditOtherGroupSec
        # exclude = ['month']
        fields = "__all__"


class AuditFreqSecSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditFreqSec
        # exclude = ['month']
        fields = "__all__"


class AuditFreqAcctSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditFreqAcct
        # exclude = ['month']
        fields = "__all__"


class AuditSensAcctSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditSensAcct
        # exclude = ['month']
        fields = "__all__"


class AuditOtherIpSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditOtherIp
        # exclude = ['month']
        fields = "__all__"


class AuditNontradeSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditNontrade
        # # exclude = ['month']
        fields = "__all__"


class AuditQueryAcctTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditQueryAcctType
        # exclude = ['month']
        fields = "__all__"


class AuditQuerySecSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditQuerySec
        # exclude = ['month']
        fields = "__all__"


# class AuditRuleListSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = AuditRuleList
#         # # exclude = ['month']
#         fields = "__all__"


class AuditMonthSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditMonth
        # exclude = ['month']
        fields = "__all__"
