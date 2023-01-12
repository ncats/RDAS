# Generated by Django 4.0.4 on 2022-05-09 17:32

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0031_clinicaltrial_status'),
    ]

    operations = [
        migrations.CreateModel(
            name='JournalArticle',
            fields=[
            ],
            options={
                'proxy': True,
                'indexes': [],
                'constraints': [],
            },
            bases=('core.article',),
        ),
        migrations.CreateModel(
            name='NewsArticle',
            fields=[
            ],
            options={
                'proxy': True,
                'indexes': [],
                'constraints': [],
            },
            bases=('core.article',),
        ),
        migrations.AddField(
            model_name='event',
            name='event_end_time',
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
    ]
