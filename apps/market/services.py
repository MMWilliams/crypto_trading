from firebase_admin import firestore
from celery.utils.log import get_task_logger
from datetime import datetime
import hashlib
import uuid
import requests
import os
from typing import Dict, List, Optional

# Configuration
FIRESTORE_COLLECTION_ID = os.getenv('FIRESTORE_COLLECTION_ID', 'market_data')
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

def generate_document_id(symbol: str, timestamp: str) -> str:
    """Generate a Firestore-safe document ID using a symbol and timestamp"""
    unique_string = f"{symbol}_{timestamp}"
    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()

def fetch_market_data() -> List[Dict]:
    """Fetch market data from Coinbase API (or other market APIs)"""
    try:
        # Example fetching market data from Coinbase API
        response = requests.get('https://api.coinbase.com/v2/exchange-rates?currency=BTC')
        response.raise_for_status()
        data = response.json()
        
        # Example structure of data; adapt as needed
        return [
            {
                'symbol': 'BTC-USD',
                'exchange_rate': data['data']['rates']['USD'],
                'fetched_at': datetime.utcnow().isoformat()
            }
        ]
    except requests.RequestException as e:
        logger.error(f"Failed to fetch market data from API: {e}", exc_info=True)
        return []

def store_market_data(market_data: Dict, collection_id: Optional[str] = None) -> Dict:
    """Store market data in Firestore with a safe document ID"""
    start_time = datetime.now()
    
    try:
        # Use provided collection_id or default
        collection_id = collection_id or FIRESTORE_COLLECTION_ID
        logger.info(f"Starting market data storage in Firestore collection: {collection_id}")
        
        # Generate safe document ID
        doc_id = generate_document_id(market_data.get('symbol'), market_data.get('fetched_at'))
        logger.debug(f"Generated document ID: {doc_id} for market data: {market_data.get('symbol')}")
        
        # Prepare market data
        market_data_to_store = {
            **market_data,
            'created_at': firestore.SERVER_TIMESTAMP,
            'updated_at': firestore.SERVER_TIMESTAMP,
            'doc_id': doc_id
        }
        
        # Store in Firestore with explicit document ID
        doc_ref = db.collection(collection_id).document(doc_id)
        doc_ref.set(market_data_to_store, merge=True)  # merge=True to update existing documents
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Successfully stored market data in {duration:.2f} seconds. ID: {doc_id}")
        
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

def fetch_market_data_and_store():
    """Fetch market data and store it in Firestore"""
    market_data_list = fetch_market_data()
    for market_data in market_data_list:
        store_market_data(market_data)
