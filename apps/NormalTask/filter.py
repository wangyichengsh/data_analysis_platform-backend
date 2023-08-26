import django_filters

from .models import File

class FileListFilter(django_filters.CharFilter):

    def filter(self, qs, value):
        value = list(filter(None, value.split(",")))
        return super(FileListFilter, self).filter(qs=qs, value=value)


class FileFilter(django_filters.rest_framework.FilterSet):
    id = FileListFilter(field_name='id',lookup_expr='in')

    class Meta:
        model = File
        fields = ['id']
