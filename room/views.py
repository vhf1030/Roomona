from django.shortcuts import render, get_object_or_404
from .models import EscapeJbStats

# Create your views here.


def index(request):
    # return HttpResponse('안녕하세요. roomona에 오신것을 환영합니다!')
    theme_list = EscapeJbStats.objects.order_by('-star_stat')
    context = {'theme_list': theme_list}
    return render(request, 'room/theme_list.html', context)


def detail(request, theme_id):
    # theme_detail = get_object_or_404(EscapeJbStats.objects.filter(theme_id=theme_id))  # 오브젝트로 가져오는 경우 처리방법 확인 필요
    theme_detail = EscapeJbStats.objects.filter(theme_id=theme_id)
    context = {'theme_detail': theme_detail.values()[0],
               'theme_detail_tup': theme_detail.values_list()[0],
               'test': [k + ': ' + str(theme_detail.values()[0][k]) for k in theme_detail.values()[0]]}
    return render(request, 'room/theme_detail.html', context)

