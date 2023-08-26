from django.urls import path, include
from django.urls import path
from rest_framework.routers import DefaultRouter
from ComplexApp.views.acct_relation import InvalidDeviceViewSet,AcctRelationsView,AcctGraphView,FileDownloadView,DocumentView

router = DefaultRouter()

# 审计模块接口

router.register(r'invalidDevice', InvalidDeviceViewSet, basename='acct_relation_invalid_device')


urlpatterns = [
    # path('data/',AuditView.as_view()),
    path('table/',AcctRelationsView.as_view()),
    path('graph/',AcctGraphView.as_view()),
    path('downloadFile/',FileDownloadView.as_view()),
    path('document/',DocumentView.as_view()),
]

urlpatterns = urlpatterns + router.urls