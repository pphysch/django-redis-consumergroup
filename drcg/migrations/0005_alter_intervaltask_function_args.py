# Generated by Django 4.0.4 on 2022-05-26 22:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('drcg', '0004_alter_intervaltask_function_name_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='intervaltask',
            name='function_args',
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
