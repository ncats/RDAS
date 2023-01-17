# Generated by Django 4.0.1 on 2022-04-17 20:41

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('contenttypes', '0002_remove_content_type_name'),
        ('core', '0022_alter_curereport_report_alter_curereport_status'),
    ]

    operations = [
        migrations.CreateModel(
            name='Newsfeed',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', models.DateTimeField(auto_now_add=True, help_text='Datetime when the object was created.')),
                ('updated', models.DateTimeField(auto_now=True, help_text='Datetime when the object was last updated.')),
                ('action', models.CharField(default='created', help_text="Can be 'created' or 'commented on'.", max_length=25)),
                ('object_id', models.IntegerField()),
                ('pinned', models.BooleanField(default=False)),
                ('content_type', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='+', to='contenttypes.contenttype')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
