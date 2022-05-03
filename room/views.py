from django.shortcuts import render
from .models import EscapeJbStats

# Create your views here.

def index(request):
    # return HttpResponse('안녕하세요. roomona에 오신것을 환영합니다!')
    theme_list = EscapeJbStats.objects.order_by('-star_stat')
    context = {'theme_list': theme_list}
    return render(request, 'room/theme_list.html', context)
