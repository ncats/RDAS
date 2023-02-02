# Generated by Django 4.0.1 on 2022-03-15 00:02

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('contenttypes', '0002_remove_content_type_name'),
        ('core', '0003_adverseeventsoutcome_comorbidity_organism_race_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='AttachedImage',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', models.DateTimeField(auto_now_add=True, help_text='Datetime when the object was created.')),
                ('updated', models.DateTimeField(auto_now=True, help_text='Datetime when the object was last updated.')),
                ('object_id', models.IntegerField()),
                ('image_name', models.CharField(help_text='The filename that will be displayed. Filler until the file is approved.', max_length=25)),
                ('real_name', models.CharField(help_text='The filename of the image in the S3.', max_length=25)),
                ('content_type', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='+', to='contenttypes.contenttype')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]