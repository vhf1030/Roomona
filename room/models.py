from django.db import models

# Create your models here.

# 기존 db와 연동하는 경우:
# 1. python manage.py inspectdb 로 확인 및 복사하여 class 생성
# # settings.py - INSTALLED_APPS 에 app class 추가
# 2. python manage.py makemigrations
# 3. python manage.py migrate / 또는 python manage.py migrate --fake


class EscapeJbStats(models.Model):
    cafe_id = models.SmallIntegerField(blank=True, null=True)
    cafe_name = models.CharField(max_length=63, blank=True, null=True)
    location_category = models.CharField(max_length=15, blank=True, null=True)
    location_detail = models.CharField(max_length=63, blank=True, null=True)
    home_url = models.CharField(max_length=255, blank=True, null=True)
    # primary_key가 db에 설정이 안되어있는 경우 직접 입력해야 함
    # theme_id = models.SmallIntegerField(blank=True, null=True)
    theme_id = models.SmallIntegerField(primary_key=True)
    theme_name = models.CharField(max_length=63, blank=True, null=True)
    genre = models.CharField(max_length=63, blank=True, null=True)
    activity = models.CharField(max_length=15, blank=True, null=True)
    recom_user = models.CharField(max_length=15, blank=True, null=True)
    review_n = models.BigIntegerField()
    star_stat = models.DecimalField(max_digits=6, decimal_places=5, blank=True, null=True)
    medal_stat = models.DecimalField(max_digits=7, decimal_places=4, blank=True, null=True)
    diff_stat = models.DecimalField(max_digits=7, decimal_places=4, blank=True, null=True)
    success_rate = models.DecimalField(max_digits=7, decimal_places=4, blank=True, null=True)
    left_stat = models.DecimalField(max_digits=9, decimal_places=4, blank=True, null=True)
    limit_time = models.SmallIntegerField(blank=True, null=True)
    hint_use = models.DecimalField(max_digits=9, decimal_places=4, blank=True, null=True)
    review_updated = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'escape_jb_stats'

