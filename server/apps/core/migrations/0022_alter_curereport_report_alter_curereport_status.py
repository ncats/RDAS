# Generated by Django 4.0.1 on 2022-04-15 16:14

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0021_merge_20220412_1806'),
    ]

    operations = [
        migrations.AlterField(
            model_name='curereport',
            name='report',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='reports', to='core.report'),
        ),
        migrations.AlterField(
            model_name='curereport',
            name='status',
            field=models.CharField(choices=[('Submitted', 'Submitted'), ('Saved', 'Saved'), ('Approved', 'Approved'), ('Rejected', 'Rejected'), ('Deleted', 'Deleted')], default='Saved', max_length=32),
        ),
    ]
