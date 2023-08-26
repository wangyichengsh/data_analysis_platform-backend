from django.urls import path, include
from .views import *
from rest_framework.routers import DefaultRouter
from rest_framework_jwt.views import obtain_jwt_token,refresh_jwt_token,verify_jwt_token

router = DefaultRouter()

# 用户信息列表
router.register(r'user', UserViewSet, basename='user')
# router.register(r'password_change2', ChangePasswordViewSet, basename='password_change2')

urlpatterns = [
    # ngsp_token登录
    path('ngsp_login/', Token2TokenView.as_view()),
    # 用户密码登录
    # path('login/', LoginView.as_view()),
    path('login/',obtain_jwt_token),
    path('verify_token/', verify_jwt_token),
    path('refresh_token/', refresh_jwt_token),
    # 修改密码
    path('password_change/', ChangePasswordView.as_view()),

    # path('login2/',LoginApiView.as_view())
]

urlpatterns = urlpatterns + router.urls