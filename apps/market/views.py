# apps/market/views.py

from django.shortcuts import render
from django.http import HttpResponse

# Example view (if needed)
def index(request):
    return HttpResponse("Market Data Processor is Running.")
