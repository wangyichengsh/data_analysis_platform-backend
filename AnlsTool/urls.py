"""AnlsTool URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include, re_path
from rest_framework.routers import DefaultRouter
from rest_framework.authtoken import views

from NormalTask.views import TaskInDevelopView, ResFileView, ParentTaskViewSet, VersionViewSet, TaskViewSet, ExecFuncViewSet, InputViewSet, InputFileSheetViewSet, InputFileColumnViewSet, OutputSheetViewSet, OutputColumnViewSet, SqlCodeViewSet, \
ExecTaskView, JobHistoryViewSet, ConfigUpdateView, ConfigRenewView, OutputRenewView, CreateVersionView, RenewVersionView, DeleteVersionView, ConfigView, ConfigPickleView, OutputSimpleViewSet, InputFileSimpleViewSet, TempTableSqlView, \
FileUpLoadView, DemandView, FileViewSet, DemandStatusChange, SonTaskView, ExtDemand, ChangeHistoryViewSet, ParentTaskSimpleViewSet, SeqFile, DeveQueue, UnfinishedViewSet, ExecSqlTestView, ShowResView
from Auth.views import ChangePasswordView, UserViewSet, Token2TokenView
from Feedback.views import FeedRecordViewSet, HasNewFeedViewSet, HasNewFeedViewSetTask, ConfirmSonTask, FeedPageViewSet
from NormalTask.web_views import HistoryView
from KcbWeb.views import KcdWebView, KcbBottomView, KcbWeb2ChartView, KcbWebNewPlusView, KcbLinePriceOverView, HeatMapView, SimpleTableView, SimpleTable2View, KcbWebNewView

router = DefaultRouter()

router.register(r'parenttask', ParentTaskViewSet, basename='parenttask')
router.register(r'version', VersionViewSet, basename='version')
router.register(r'task', TaskViewSet, basename='task')
router.register(r'taskindeve', TaskInDevelopView, basename='taskInDev')
router.register(r'execfunc', ExecFuncViewSet, basename='execfunc')
router.register(r'input', InputViewSet, basename='input')
router.register(r'inputfilesheet', InputFileSheetViewSet, basename='inputfilesheet')
router.register(r'inputfilesimple', InputFileSimpleViewSet, basename='inputfilesimple')
router.register(r'inputfilecolumn', InputFileColumnViewSet, basename='inputfilecolumn')
router.register(r'outputsheet', OutputSheetViewSet, basename='outputsheet')
router.register(r'outputsimple',OutputSimpleViewSet, basename='outputsheet_simple')
router.register(r'outputcolumn', OutputColumnViewSet, basename='outputcolumn')
router.register(r'sqlcode', SqlCodeViewSet, basename='sqlcode')
router.register(r'history', JobHistoryViewSet, basename='history')
router.register(r'user', UserViewSet, basename='user')
router.register(r'file', FileViewSet, basename='file')
router.register(r'changehistory', ChangeHistoryViewSet, basename='changehistory')
router.register(r'parenttasksimple', ParentTaskSimpleViewSet, basename='parenttasksimple')
router.register(r'unfinished', UnfinishedViewSet, basename='unfinished')
# 审计模块接口
# router.register(r'othergroupsec', AuditOtherGroupSecViewSet, basename='othergroupsec')
# router.register(r'freqsec', AuditFreqSecViewSet, basename='freqsec')
# router.register(r'freqacct', AuditFreqAcctViewSet, basename='freqacct')
# router.register(r'sensacct', AuditSensAcctViewSet, basename='sensacct')
# router.register(r'otherip', AuditOtherIpViewSet, basename='otherip')
# router.register(r'nontrade', AuditNontradeViewSet, basename='nontrade')
# router.register(r'queryaccttype', AuditQueryAcctTypeViewSet, basename='queryaccttype')
# router.register(r'querysec', AuditQuerySecViewSet, basename='querysec')
# router.register(r'month', AuditMonthViewSet, basename='month')
# 反馈模块接口
router.register(r'feedrecord', FeedRecordViewSet, basename='feedrecord')
router.register(r'feedpage', FeedPageViewSet, basename='feedpage')



urlpatterns = [
    # 后台管理界面
    path('admin/', admin.site.urls),
    # 接口视图
    path('api/api-token-auth/',views.obtain_auth_token),
    path('api/ngsp_token/', Token2TokenView.as_view()),
    path('api/', include(router.urls)),
    path('api/exectask/<int:task_id>/', ExecTaskView.as_view()),
    path('api/exectest/<int:task_id>/', ExecSqlTestView.as_view()),
    path('api/createversion/<int:parentTask_id>/', CreateVersionView.as_view()),
    path('api/renewversion/<int:parentTask_id>/', RenewVersionView.as_view()),
    path('api/deleteversion/', DeleteVersionView.as_view()),
    path('api/config/<int:task_id>/', ConfigView.as_view()),
    path('api/configpickle/<int:task_id>/', ConfigPickleView.as_view()),
    path('api/updateconfig/<int:task_id>/', ConfigUpdateView.as_view()),
    path('api/renewconfig/<int:task_id>/', ConfigRenewView.as_view()),
    path('api/renewoutput/<int:task_id>/', OutputRenewView.as_view()),
    path('api/tempsql/<int:input_id>/', TempTableSqlView.as_view()),
    path('api/password_change/', ChangePasswordView.as_view()),
    path('api/fileupload/' , FileUpLoadView.as_view()),
    path('api/demand/' , DemandView.as_view()),
    path('api/demandstatus/<int:demand_id>/' , DemandStatusChange.as_view()),
    path('api/sontask/', SonTaskView.as_view()),
    path('api/resfile/', ResFileView.as_view()),
    path('api/extdemand/', ExtDemand.as_view()),
    path('api/seqfile/<int:seq_id>/', SeqFile.as_view()),
    path('api/devequeue/', DeveQueue.as_view()),
    path('api/newfeed/<int:demand_id>/',HasNewFeedViewSet.as_view()),
    path('api/newfeedtask/<int:task_id>/',HasNewFeedViewSetTask.as_view()),
    path('api/confirmsontask/',ConfirmSonTask.as_view()),
    path('api/showres/',ShowResView.as_view()),
    # web视图
    path('web/history/',HistoryView, name='历史记录'),
    # 科创板大屏
    path('api/kcbview/',KcdWebView.as_view()),
    path('api/kcbbottom/',KcbBottomView.as_view()),
    path('api/kcbweb2/index/',KcbWeb2ChartView.as_view()),
    path('api/kcbweb2/priceoverview/', KcbLinePriceOverView.as_view()),
    path('api/kcbweb2/heatmap/', HeatMapView.as_view()),
    path('api/kcbweb2/simpletable/', SimpleTableView.as_view()),
    path('api/kcbweb2/simpletable2/', SimpleTable2View.as_view()),
    path('api/kcbnewplus/',KcbWebNewPlusView.as_view()),
    path('api/kcbnew/',KcbWebNewView.as_view()),
    # 审计
    path('api/audit/', include('Audit.urls')),
    # 复杂应用
    path('api/complexApp/', include('ComplexApp.urls')),
    # 公共组件
    path('api/component/', include('Component.urls'))

]
