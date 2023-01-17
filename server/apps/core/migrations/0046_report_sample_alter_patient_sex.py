# Generated by Django 4.0.4 on 2022-08-03 15:36

import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0045_alter_attachedimage_reviewer'),
    ]

    operations = [
        migrations.AddField(
            model_name='report',
            name='sample',
            field=django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=128), blank=True, default=list, help_text='Sample information', max_length=16, null=True, size=None),
        ),
        migrations.AlterField(
            model_name='patient',
            name='sex',
            field=models.CharField(blank=True, choices=[('Male', 'Male'), ('Female', 'Female'), ('Intersex', 'Intersex'), ('Other', 'Other'), ('Not specified', 'Not specified')], max_length=64, null=True),
        ),
    ]
