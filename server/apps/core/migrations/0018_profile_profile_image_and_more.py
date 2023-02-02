# Generated by Django 4.0.1 on 2022-04-06 13:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0017_alter_clinicaltrial_disease_alter_event_location'),
    ]

    operations = [
        migrations.AddField(
            model_name='profile',
            name='profile_image',
            field=models.CharField(blank=True, default='attached-image-filler.jpg', max_length=100, null=True),
        ),
        migrations.AlterField(
            model_name='clinicaltrial',
            name='clinical_trials_gov_id',
            field=models.CharField(max_length=15, unique=True),
        ),
        migrations.AlterField(
            model_name='profile',
            name='favorited_clinical_trials',
            field=models.ManyToManyField(blank=True, help_text='List of ClinicalTrials the user is interested in.', related_name='favorite_clinical_trials', to='core.ClinicalTrial'),
        ),
        migrations.AlterField(
            model_name='profile',
            name='favorited_reports',
            field=models.ManyToManyField(blank=True, help_text='List of reports the user is interested in.', related_name='favorite_reports', to='core.Report'),
        ),
    ]