# apps/news/models.py

from django.db import models

class NewsArticle(models.Model):
    title = models.CharField(max_length=255)
    description = models.TextField()
    content = models.TextField()
    url = models.URLField(unique=True)
    url_to_image = models.URLField(blank=True, null=True)
    published_at = models.DateTimeField()
    source = models.CharField(max_length=100)
    summary = models.TextField(blank=True, null=True)

    def __str__(self):
        return self.title
