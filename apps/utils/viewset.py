from collections import OrderedDict, namedtuple
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from rest_framework.viewsets import ViewSet,GenericViewSet


class Pagination(PageNumberPagination):
    '''
    接口默认分页设置
    '''
    page_size = None
    page_size_query_param = 'page_size'
    page_query_param = 'page'

    # def get_paginated_response(self, data):
    #     return Response(OrderedDict([
    #         ('count', self.page.paginator.count),
    #         ('next', self.get_next_link()),
    #         ('previous', self.get_previous_link()),
    #         ('results', data)
    #     ]))

class ListModelApiMixin:
    """
    必须定义get_columns_info方法或self.columns属性
    page、page_size_query_param、page_query_param用于定义分页属性，也可通过覆盖get_pagination_class方法修改分页类
    get方法，返回{'columns':[],'data':data}
    """
    columns = None
    page_size = None
    page_size_query_param = 'page_size'
    page_query_param = 'page'

    def get_pagination_class(self):
        pagination_class = Pagination
        pagination_class.page_size = self.page_size
        pagination_class.page_query_param = self.page_query_param
        return pagination_class

    def get_columns_info(self):
        assert self.columns is not None, (
            "'%s' should either include a `columns` attribute, "
            "or override the `get_columns_info()` method."
            % self.__class__.__name__
        )
        return self.columns

    def list(self, request, *args, **kwargs):
        self.pagination_class = self.get_pagination_class()
        queryset = self.filter_queryset(self.get_queryset())
        columns = self.get_columns_info()
        page = self.paginate_queryset(queryset)

        if page is not None:
            serializer = self.get_serializer(page, many=True)
            response_data = {'columns': columns, 'data': serializer.data}
            return self.get_paginated_response(response_data)

        serializer = self.get_serializer(queryset, many=True)

        response_data =  {'columns': columns, 'data': serializer.data}
        return Response(response_data)


# class ListModelViewSet(ListModelApiMixin, GenericViewSet):
#     """
#     List a queryset.
#     """
#     def list(self, request, *args, **kwargs):
#         queryset = self.filter_queryset(self.get_queryset())
#
#         page = self.paginate_queryset(queryset)
#         if page is not None:
#             serializer = self.get_serializer(page, many=True)
#             columns = dict([(field.name, field.verbose_name) for field in serializer._meta.model._meta.fields])
#             return self.get_paginated_response(serializer.data)
#
#         serializer = self.get_serializer(queryset, many=True)
#         columns = dict([(field.name, field.verbose_name) for field in serializer.meta.model._meta.fields])
#         data = serializer.data
#         response_data =  {'columns': columns, 'data': data}
#         return Response(response_data)


