# Generated by Django 4.0.4 on 2022-05-05 16:01

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0027_alter_report_how_diagnosis_alter_report_why_new_way'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='report',
            name='drugs',
        ),
    ]