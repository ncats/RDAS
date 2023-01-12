# Generated by Django 4.0.4 on 2022-08-30 21:07

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0053_alter_patient_ethnicity'),
    ]

    operations = [
        migrations.AlterField(
            model_name='curereport',
            name='report_type',
            field=models.CharField(blank=True, default='', max_length=16, null=True),
        ),
        migrations.AlterField(
            model_name='report',
            name='extra_fields',
            field=models.JSONField(blank=True, default=dict, help_text='JSON array for any additional report fields.', null=True),
        ),
    ]
