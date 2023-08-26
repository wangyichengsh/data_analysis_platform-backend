import json
from rest_framework import serializers
from Component.models import DateCanlendar, AppExecuteHistory
from datetime import timedelta

# 日历表
class DateCanlendarSerializer(serializers.ModelSerializer):
    class Meta:
        model = DateCanlendar
        fields = "__all__"


# 程序执行历史信息
class AppExecuteHistorySerializer(serializers.ModelSerializer):
    user_name = serializers.ReadOnlyField(source='user.full_name')
    use_time = serializers.SerializerMethodField()

    def get_use_time(self, obj):
        if obj.execute_end_time:
            seconds = (obj.execute_end_time-obj.execute_start_time).seconds
            return str(seconds//3600)+':'+str(seconds//60-seconds//3600*60).zfill(2)+':'+str(seconds-seconds//60*60).zfill(2)
        return ''

    class Meta:
        model = AppExecuteHistory
        fields = ["id","user_id","user_name","exec_status","result_file_name","execute_start_time","execute_end_time","remark","has_viewed","parameter","use_time","query_id"]
        write_only=['has_viewed','remark']

