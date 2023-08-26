from django.shortcuts import render

def HistoryView(request):
    return render(request, 'history_get.html')