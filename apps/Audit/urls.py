from django.urls import path, include
from django.urls import path
from rest_framework.routers import DefaultRouter
from .views import *

router = DefaultRouter()

# 审计模块接口

router.register(r'month', AuditMonthViewSet, basename='month')


urlpatterns = [
    path('data/',AuditView.as_view()),
]

urlpatterns = urlpatterns + router.urls