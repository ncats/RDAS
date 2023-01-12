# Generated by Django 4.0.4 on 2022-05-26 16:51

import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0036_alter_article_article_url'),
    ]

    operations = [
        migrations.AddField(
            model_name='report',
            name='extrapulmonary_site',
            field=django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=32), blank=True, default=list, help_text='Extra pulmonary site', max_length=8, null=True, size=None),
        ),
        migrations.AlterField(
            model_name='report',
            name='adverse_events_outcome',
            field=models.CharField(blank=True, help_text='Adverse events outcome', max_length=64, null=True),
        ),
        migrations.AlterField(
            model_name='report',
            name='site_of_infection',
            field=django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=64), blank=True, default=list, help_text='site of infection', max_length=4, null=True, size=None),
        ),
    ]
