# Generated by Django 5.1.6 on 2025-02-21 12:09

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('django_sourcery', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='eventrecord',
            name='id',
            field=models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID'),
        ),
    ]
