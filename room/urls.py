from django.urls import path

from . import views

app_name = 'room'

urlpatterns = [
    path('', views.index, name='index'),
    path('<int:theme_id>/', views.detail, name='detail'),
]

