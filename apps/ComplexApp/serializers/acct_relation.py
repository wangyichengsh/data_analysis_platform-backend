from rest_framework import serializers
from ComplexApp.models import InvalidDevice


class InvalidDeviceSerializer(serializers.ModelSerializer):
    class Meta:
        model = InvalidDevice
        fields = ('device_type', 'device', 'reason')