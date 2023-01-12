# Generated by Django 4.0.4 on 2022-05-18 23:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0033_alter_attachedimage_real_name_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='article',
            name='publication_name',
            field=models.CharField(blank=True, help_text='Journal name the article was published in.', max_length=256, null=True),
        ),
        migrations.AlterField(
            model_name='report',
            name='organisms',
            field=models.ManyToManyField(blank=True, null=True, related_name='reports_organisms', to='core.organism'),
        ),
    ]
