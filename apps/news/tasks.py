import requests
from django.conf import settings
from .models import NewsArticle
from django.utils.dateparse import parse_datetime
import os
from .models import NewsArticle
from apps.openai_integration.services import summarize_article
from celery import shared_task
from django.core.management import call_command
from celery.utils.log import get_task_logger
from datetime import datetime
from firebase_admin import firestore
from dotenv import load_dotenv
from billiard.exceptions import WorkerLostError
import gc
import traceback

load_dotenv()

# Set up logger
logger = get_task_logger(__name__)

NEWS_API_KEY = os.getenv('NEWS_API_KEY')
NEWS_API_URL = 'https://newsapi.org/v2/everything'

@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={'max_retries': 3},
    soft_time_limit=600,  # 10 minutes
    time_limit=900       # 15 minutes
)
def fetch_news_task(self):
    """
    Celery task to fetch news articles with enhanced error handling
    """
    try:
        logger.info(f"Starting fetch_news_task with task_id: {self.request.id}")
        
        # Force garbage collection before starting
        gc.collect()
        
        # Call the management command with logging
        try:
            call_command('fetch_news')  # 5-minute timeout
        except Exception as e:
            logger.error(f"Error in fetch_news command: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Cleanup before re-raising
            gc.collect()
            raise
            
        logger.info(f"Successfully completed fetch_news_task: {self.request.id}")
        
        # Final cleanup
        gc.collect()
        
        return {"status": "success", "task_id": self.request.id}
        
    except WorkerLostError as e:
        logger.error(f"Worker lost error in fetch_news_task: {str(e)}")
        self.retry(countdown=60)  # Retry after 1 minute
        
    except Exception as e:
        logger.error(f"Error in fetch_news_task: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Cleanup before re-raising
        gc.collect()
        raise
