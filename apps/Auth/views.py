import json
from urllib.parse import unquote
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import viewsets, mixins
from rest_framework.authtoken.models import Token
import django_filters
from django.contrib.auth import get_user_model
from rest_framework.authtoken import views

from .serializers import UserSerializer
from .common import token2user

User = get_user_model()


class LoginLockException(Exception):
    def __init__(self, err="账户锁定"):
        Exception.__init__(self, err)


class UserViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['id', 'username', 'is_staff']


class LoginView(views.ObtainAuthToken):
    def post(self, request, *args, **kwargs):
        username = request.data['username']
        user_login = User.objects.get(username=username)
        if user_login.fail_login >= 5:
            # raise LoginLockException()
            return Response({'msg': '账号锁定'})
        try:
            serializer = self.serializer_class(data=request.data,
                                               context={'request': request})
            serializer.is_valid(raise_exception=True)
            user = serializer.validated_data['user']
            token, created = Token.objects.get_or_create(user=user)
            user.fail_login = 0
            user.save()
            return Response({'token': token.key})
        except Exception as e:
            user_login.fail_login += 1
            user_login.save()
            raise LoginLockException()


class ChangePasswordView(APIView):
    """
    执行代码返回结果
    """

    def options(self, request, format=None):
        response = Response()
        response['Access-Control-Allow-Method'] = "POST, OPTIONS"
        response['Access-Control-Allow-Origin'] = "*"
        response['Access-Control-Allow-Headers'] = "Content-Type,Cookie,Authorization"
        return response

    def post(self, request, format=None):
        try:
            if request.user.is_authenticated:
                user = request.user
                password_param = request.data
                old_password = password_param['old_password']
                new_password = password_param['new_password']
                if user.check_password(old_password):
                    user.set_password(new_password)
                    user.save()
                    return Response({'status': 'sucess', 'msg': ''}, content_type="application/json")
                else:
                    return Response({'status': 'Error', 'msg': '您的旧密码不正确。请重新输入。'}, content_type="application/json")
            else:
                return Response({'status': 'Error', 'msg': 'Can not find user'}, content_type="application/json")
        except Exception as e:
            return Response({'status': 'Error', 'msg': str(e)}, content_type="application/json")


class Token2TokenView(APIView):
    """
    解析新监察token返回服务站token
    """

    def options(self, request, format=None):
        response = Response()
        response['Access-Control-Allow-Method'] = "GET, POST, OPTIONS"
        response['Access-Control-Allow-Origin'] = "*"
        response['Access-Control-Allow-Headers'] = "Content-Type,Cookie,Authorization"
        return response

    def get(self, request, format=None):
        param = request.query_params
        ngsp_token = param.get('ngsp_token', '')
        user = token2user(ngsp_token)
        if user != None:
            token = Token.objects.get(user=user)
            if token:
                return Response({'name': str(user.username), 'token': str(token)}, content_type="application/json")
            else:
                return Response({'msg': 'No Token'}, content_type="application/json")
        else:
            return Response({'msg': 'No User'}, content_type="application/json")


class HealthView(APIView):
    """
    健康检查
    """

    def get(self, request, format=None):
        return Response({'status': 'success'}, content_type="application/json")

