# apps/market/admin.py

from django.contrib import admin
from .models import MarketData

@admin.register(MarketData)
class MarketDataAdmin(admin.ModelAdmin):
    list_display = ('name', 'symbol', 'price', 'market_cap', 'volume_24h', 'change_24h', 'last_updated')
    search_fields = ('name', 'symbol')
    list_filter = ('symbol',)
