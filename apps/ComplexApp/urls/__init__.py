from django.urls import path, include

from .acct_relation import urlpatterns as url_acct_relation

urlpatterns = [
    path('acctRelation/', include(url_acct_relation)),
]