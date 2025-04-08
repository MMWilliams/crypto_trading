# apps/market/models.py

from django.db import models

class MarketData(models.Model):
    symbol = models.CharField(max_length=10)
    name = models.CharField(max_length=100)
    price = models.DecimalField(max_digits=20, decimal_places=8)
    market_cap = models.BigIntegerField()
    volume_24h = models.BigIntegerField()
    change_24h = models.DecimalField(max_digits=10, decimal_places=2)
    last_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.symbol})"
