from django.urls import path,re_path
from rest_framework.routers import DefaultRouter
from Component.views import DateCanlendarViewSet, AppExecuteHistoryViewSet

router = DefaultRouter()

# 审计模块接口

router.register(r'dateCanlendar', DateCanlendarViewSet, basename='datecanlendar')
router.register(r'appExecuteHistory', AppExecuteHistoryViewSet, basename='appExecuteHistory')


urlpatterns = [
    # path('appExecuteHistory/',AppExecuteHistoryView.as_view()),
    # re_path('appExecuteHistory/(?P<pk>\d+)/$',AppExecuteHistoryView.as_view()),
]

urlpatterns = urlpatterns + router.urls