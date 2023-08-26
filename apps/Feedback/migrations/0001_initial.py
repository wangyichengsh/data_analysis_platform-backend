# Generated by Django 2.2.5 on 2020-06-12 20:48

import datetime
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('NormalTask', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='FeedRecord',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('demind_seq', models.IntegerField(verbose_name='需求SEQ')),
                ('model_name', models.CharField(max_length=50, verbose_name='模型')),
                ('model_id', models.IntegerField(verbose_name='反馈针对id')),
                ('model_title', models.CharField(default='', max_length=100, verbose_name='模型名称')),
                ('feedto', models.TextField(default='', help_text='提问内容', verbose_name='提问内容')),
                ('feedto_time', models.DateTimeField(default=datetime.datetime.now, verbose_name='提问时间')),
                ('feedback', models.TextField(default='', help_text='反馈内容', verbose_name='反馈内容')),
                ('feedback_time', models.DateTimeField(blank=True, null=True, verbose_name='反馈时间')),
                ('if_feed', models.BooleanField(default=False, verbose_name='是否反馈(t:反馈,f:未反馈)')),
                ('demind_id', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='NormalTask.ParentTask', verbose_name='反馈针对需求')),
                ('feedback_user', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='feedback_user', to=settings.AUTH_USER_MODEL, verbose_name='反馈用户')),
                ('feedto_user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='feedto_user', to=settings.AUTH_USER_MODEL, verbose_name='提问用户')),
                ('task_id', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='NormalTask.Task', verbose_name='反馈针对子任务')),
            ],
        ),
    ]
