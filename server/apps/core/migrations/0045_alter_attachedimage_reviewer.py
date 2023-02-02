# Generated by Django 4.0.4 on 2022-07-23 16:05

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0044_delete_journalarticle_delete_newsarticle_cureuser_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='attachedimage',
            name='reviewer',
            field=models.ForeignKey(blank=True, help_text='User account that reviewed the image.', null=True, on_delete=django.db.models.deletion.PROTECT, to='core.cureuser'),
        ),
    ]