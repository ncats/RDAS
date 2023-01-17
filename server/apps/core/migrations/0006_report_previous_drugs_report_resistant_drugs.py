# Generated by Django 4.0.1 on 2022-03-10 20:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0005_remove_report_severity_report_adverse_events_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='report',
            name='previous_drugs',
            field=models.ManyToManyField(blank=True, related_name='report_previous_drugs', to='core.Drug'),
        ),
        migrations.AddField(
            model_name='report',
            name='resistant_drugs',
            field=models.ManyToManyField(blank=True, related_name='report_resistant_drugs', to='core.Drug'),
        ),
    ]
