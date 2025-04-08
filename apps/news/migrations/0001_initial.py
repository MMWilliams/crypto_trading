# Generated by Django 5.1.3 on 2024-11-23 02:09

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='NewsArticle',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.CharField(max_length=255)),
                ('description', models.TextField()),
                ('content', models.TextField()),
                ('url', models.URLField(unique=True)),
                ('url_to_image', models.URLField(blank=True, null=True)),
                ('published_at', models.DateTimeField()),
                ('source', models.CharField(max_length=100)),
                ('summary', models.TextField(blank=True, null=True)),
            ],
        ),
    ]
