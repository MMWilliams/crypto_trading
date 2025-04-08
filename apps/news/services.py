# apps/news/services.py

from firebase_admin import firestore
from celery.utils.log import get_task_logger
from datetime import datetime
import hashlib
import uuid
import os
from typing import Dict, List, Optional, Union
import hashlib
from google.cloud import firestore
from django.core.management.base import BaseCommand
import logging


# Configuration with fallback
FIRESTORE_COLLECTION_ID = os.getenv('FIRESTORE_COLLECTION_ID', 'news_articles')
if not FIRESTORE_COLLECTION_ID:
    raise ValueError("FIRESTORE_COLLECTION_ID must be set in environment variables")

# Set up logger
logger = get_task_logger(__name__)

# Initialize Firestore client
try:
    db = firestore.client()
except Exception as e:
    logger.error(f"Failed to initialize Firestore client: {e}", exc_info=True)
    raise

def validate_article_data(article_data: Dict) -> List[str]:
    """Validate article data for required fields and data types"""
    required_fields = {
        'title': str,
        'url': str,
        'description': (str, type(None)),  # Optional field
        'content': (str, type(None)),      # Optional field
        'published_at': (str, type(None))  # Optional field
    }
    
    errors = []
    for field, field_type in required_fields.items():
        value = article_data.get(field)
        if field in ['title', 'url'] and not value:
            errors.append(f"Missing required field: {field}")
        elif value and not isinstance(value, field_type):
            errors.append(f"Invalid type for {field}: expected {field_type}, got {type(value)}")
    
    return errors

def generate_document_id(url: str) -> str:
    """
    Generate a Firestore-safe document ID from a URL by creating an MD5 hash.
    This ensures that there are no special characters that Firestore cannot handle.
    """
    # Create an MD5 hash of the URL to ensure a unique ID without special characters
    return hashlib.md5(url.encode('utf-8')).hexdigest()

def generate_document_id(url: str) -> str:
    """Generate a safe document ID from a URL or generate a UUID if no URL"""
    if not url:
        return str(uuid.uuid4())
    
    # Create an MD5 hash of the URL with timestamp to ensure uniqueness
    unique_string = f"{url}_{datetime.utcnow().isoformat()}"
    return hashlib.md5(unique_string.encode()).hexdigest()

def store_article(article_data: Dict, collection_id: Optional[str] = None) -> Dict:
    """Store article data in Firestore with a safe document ID."""
    start_time = datetime.now()
    
    try:
        # Use provided collection_id or default
        collection_id = collection_id or FIRESTORE_COLLECTION_ID
        logger.info(f"Starting article storage in Firestore collection: {collection_id}")
        
        # Validate article data
        validation_errors = validate_article_data(article_data)
        if validation_errors:
            logger.error(f"Validation errors: {validation_errors}")
            return {
                'status': 'error',
                'message': 'Validation errors',
                'errors': validation_errors
            }
        
        # Generate safe document ID
        doc_id = generate_document_id(article_data.get('url'))
        logger.debug(f"Generated document ID: {doc_id} for article: {article_data.get('title')}")
        
        # Prepare article data
        article_data_to_store = {
            **article_data,
            'created_at': firestore.SERVER_TIMESTAMP,
            'updated_at': firestore.SERVER_TIMESTAMP,
            'doc_id': doc_id
        }
        
        # Store in Firestore with explicit document ID
        doc_ref = db.collection(collection_id).document(doc_id)
        doc_ref.set(article_data_to_store, merge=True)  # merge=True to update existing documents
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Successfully stored article in {duration:.2f} seconds. ID: {doc_id}")
        
        return {
            'status': 'success',
            'document_id': doc_id,
            'collection_id': collection_id,
            'duration': duration
        }
        
    except firestore.FirestoreError as e:
        logger.error(f"Firestore error: {e}", exc_info=True)
        return {
            'status': 'error',
            'message': str(e),
            'error_type': 'firestore_error'
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return {
            'status': 'error',
            'message': str(e),
            'error_type': 'unexpected_error'
        }

def bulk_store_articles(articles: List[Dict], collection_id: Optional[str] = None) -> Dict:
    """Store multiple articles in Firestore using batch operation"""
    start_time = datetime.now()
    successful_stores = 0
    failed_stores = 0
    errors = []
    
    try:
        collection_id = collection_id or FIRESTORE_COLLECTION_ID
        logger.info(f"Starting bulk storage of {len(articles)} articles")
        
        # Process articles in batches of 500 (Firestore limit)
        BATCH_SIZE = 500
        for i in range(0, len(articles), BATCH_SIZE):
            batch_articles = articles[i:i + BATCH_SIZE]
            current_batch = db.batch()
            
            for article in batch_articles:
                try:
                    # Validate article data
                    validation_errors = validate_article_data(article)
                    if validation_errors:
                        failed_stores += 1
                        errors.append({
                            'article_title': article.get('title'),
                            'errors': validation_errors
                        })
                        continue
                    
                    # Generate safe document ID
                    doc_id = generate_document_id(article.get('url'))
                    
                    # Prepare article data
                    article_data = {
                        **article,
                        'created_at': firestore.SERVER_TIMESTAMP,
                        'updated_at': firestore.SERVER_TIMESTAMP,
                        'doc_id': doc_id
                    }
                    
                    # Add to batch
                    doc_ref = db.collection(collection_id).document(doc_id)
                    current_batch.set(doc_ref, article_data, merge=True)
                    successful_stores += 1
                    
                except Exception as e:
                    failed_stores += 1
                    errors.append({
                        'article_title': article.get('title'),
                        'error': str(e)
                    })
                    logger.error(f"Error processing article: {e}", exc_info=True)
            
            # Commit current batch
            current_batch.commit()
            
        duration = (datetime.now() - start_time).total_seconds()
        
        return {
            'status': 'success',
            'successful_stores': successful_stores,
            'failed_stores': failed_stores,
            'duration': duration,
            'errors': errors if errors else None
        }
        
    except Exception as e:
        logger.error(f"Bulk storage error: {e}", exc_info=True)
        return {
            'status': 'error',
            'message': str(e),
            'successful_stores': successful_stores,
            'failed_stores': failed_stores + (len(articles) - successful_stores),
            'errors': errors
        }
