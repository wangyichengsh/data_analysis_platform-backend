import json

from rest_framework import serializers

from .models import FeedRecord
from NormalTask.models import OutputSheet, OutputColumn, ParentTask


class FeedRecordSerializer(serializers.ModelSerializer):
    parent_id = serializers.SerializerMethodField()
    task_name = serializers.SerializerMethodField()
    table_name = serializers.SerializerMethodField()
    demand_user = serializers.SerializerMethodField()

    def get_parent_id(self, obj):
        if obj.model_name == 'OutputColumn':
            try:
                return OutputColumn.objects.get(id=obj.model_id).sheet.sheet_output_id
            except Exception as e:
                return -1 
        else:
            return -1

    def get_task_name(self, obj):
        if obj.model_name == 'OutputColumn': 
           return obj.task_id.title
        else:
            return ''

    def get_table_name(self, obj):
        if obj.model_name == 'OutputColumn':
            try:
                return OutputColumn.objects.get(id=obj.model_id).sheet.name
            except Exception as e:
                return ''
        else:
            return ''

    def get_demand_user(self, obj):
        return obj.demind_id.create_by.id

    class Meta:
        model = FeedRecord 
        fields = "__all__"



