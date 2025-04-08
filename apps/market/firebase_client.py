# 
import firebase_admin
from firebase_admin import credentials, firestore
import os
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)

@lru_cache(maxsize=None)
def get_firebase_client():
    """Get or initialize Firebase client with fork safety"""
    try:
        # Try to get existing client first
        return firestore.client()
    except Exception as e:
        try:
            # If not initialized, initialize Firebase
            cred_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if not cred_path:
                raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
            
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred)
            return firestore.client()
        except Exception as init_error:
            logger.error(f"Failed to initialize Firebase client: {init_error}")
            raise