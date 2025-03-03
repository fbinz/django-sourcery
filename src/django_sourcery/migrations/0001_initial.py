# Generated by Django 5.1.6 on 2025-02-17 08:49

from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="EventRecord",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("topic", models.CharField(max_length=50)),
                ("originator_id", models.PositiveBigIntegerField()),
                ("originator_version", models.PositiveBigIntegerField()),
                ("state", models.JSONField()),
                ("timestamp", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("originator_id", "originator_version"),
                        name="record_unique_originator_version",
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="SnapshotRecord",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("topic", models.CharField(max_length=50)),
                ("originator_id", models.PositiveBigIntegerField()),
                ("originator_version", models.PositiveBigIntegerField()),
                ("state", models.JSONField()),
                ("timestamp", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("originator_id", "originator_version"),
                        name="snapshop_unique_originator_version",
                    )
                ],
            },
        ),
    ]
