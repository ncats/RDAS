# Generated by Django 4.0.1 on 2022-04-04 19:53

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0016_alter_article_abstract_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='clinicaltrial',
            name='disease',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, related_name='clinical_trials', to='core.disease'),
        ),
        migrations.AlterField(
            model_name='event',
            name='location',
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
    ]