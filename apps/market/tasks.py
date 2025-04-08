# apps/market/tasks.py
import requests
from django.conf import settings
from .models import MarketData
import os
from celery import shared_task
from django.core.management import call_command
from celery.utils.log import get_task_logger
import logging
from datetime import datetime
from django.conf import settings
import shutil
import sqlite3  # Add this import
from datetime import datetime, timedelta
import logging 
import psutil
from celery import group
# Make sure to import app
from financellm_backend.celery import app  # Import your Celery app instance

# Set up logger
# logger = logging.getLogger(__name__)

# Set up logger
logger = get_task_logger(__name__)


# @shared_task
# def update_ml_analysis():
#     call_command('update_ml_analysis')


# @shared_task(bind=True)
# def test_task(self):
#     """Here is some test task"""
#     # Don't use print() in Celery tasks, use logger instead
#     logger.info('Starting test task')
#     try:
#         logger.info('Here is a test task')
#         return {'status': 'success', 'message': 'Test task completed'}
#     except Exception as e:
#         logger.error(f'Error in test task: {str(e)}')
#         raise self.retry(exc=e, countdown=60, max_retries=3)

@shared_task(bind=True, queue='maintenance')
def cleanup_beat_database(self):
    """Clean up old entries from the Celery beat database without disrupting running processes"""
    try:
        # Check disk space
        total, used, free = shutil.disk_usage("/")
        free_gb = free // (2**30)
        
        if free_gb < 5:
            logger.critical(f"Critical: Low disk space! Only {free_gb}GB remaining")

        # Find all potential beat database files
        potential_paths = [
            os.path.join(settings.BASE_DIR, 'celerybeat-schedule'),
            os.path.join(settings.BASE_DIR, 'celerybeat-schedule.db'),
            os.path.join(os.getcwd(), 'celerybeat-schedule'),
            os.path.join(os.getcwd(), 'celerybeat-schedule.db')
        ]

        beat_databases = [path for path in potential_paths if os.path.exists(path)]

        if not beat_databases:
            logger.info("No beat database files found")
            return

        for beat_db_path in beat_databases:
            current_size = os.path.getsize(beat_db_path)
            logger.info(f"Processing database: {beat_db_path} (Size: {current_size / 1024 / 1024:.2f} MB)")

            # Try connecting with timeout and busy_timeout to handle locks
            conn = sqlite3.connect(beat_db_path, timeout=20)
            conn.execute("PRAGMA busy_timeout = 30000")  # 30 second timeout
            cursor = conn.cursor()

            try:
                # Get list of tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()

                for table in tables:
                    table_name = table[0]
                    
                    # Skip system tables
                    if table_name in ('sqlite_sequence', 'sqlite_master'):
                        continue
                    
                    try:
                        # Check if table has timestamp column
                        cursor.execute(f"PRAGMA table_info({table_name})")
                        columns = cursor.fetchall()
                        column_names = [col[1] for col in columns]

                        # Get row count before cleanup
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        before_count = cursor.fetchone()[0]
                        
                        # Different cleanup strategies based on table structure
                        if 'timestamp' in column_names:
                            # Delete entries older than 7 days
                            cutoff = datetime.now() - timedelta(days=7)
                            cursor.execute(
                                f"DELETE FROM {table_name} WHERE timestamp < ?", 
                                (cutoff.timestamp(),)
                            )
                        elif 'last_run_at' in column_names:
                            # Delete entries older than 7 days
                            cutoff = datetime.now() - timedelta(days=7)
                            cursor.execute(
                                f"DELETE FROM {table_name} WHERE last_run_at < ?", 
                                (cutoff.isoformat(),)
                            )
                        else:
                            # For tables without timestamps, keep only the most recent 1000 entries
                            cursor.execute(f"""
                                DELETE FROM {table_name} 
                                WHERE rowid NOT IN (
                                    SELECT rowid FROM {table_name} 
                                    ORDER BY rowid DESC 
                                    LIMIT 1000
                                )
                            """)

                        # Get row count after cleanup
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        after_count = cursor.fetchone()[0]
                        
                        rows_deleted = before_count - after_count
                        if rows_deleted > 0:
                            logger.info(f"Cleaned {rows_deleted} rows from {table_name}")

                    except sqlite3.Error as e:
                        logger.warning(f"❌ Error cleaning table {table_name}: {e}")
                        continue

                # Commit changes
                conn.commit()

                try:
                    # Try VACUUM if possible
                    cursor.execute("VACUUM")
                except sqlite3.Error as e:
                    logger.warning(f"Could not VACUUM database: {e}")

                # Log space saved
                new_size = os.path.getsize(beat_db_path)
                space_saved = (current_size - new_size) / 1024 / 1024
                logger.info(f"Cleanup completed for {beat_db_path}. Saved {space_saved:.2f} MB")

            except sqlite3.Error as e:
                logger.error(f"Database error: {e}")
                continue
            finally:
                cursor.close()
                conn.close()

    except Exception as e:
        logger.error(f"❌ Error in cleanup task: {e}", exc_info=True)
        raise
    finally:
        logger.info("Cleanup task completed")


# @shared_task(
#     bind=True,
#     max_retries=3,
#     retry_backoff=True,
#     retry_backoff_max=600,
#     retry_jitter=True,
#     queue='market_data'
# )


# def fetch_market_data_task(self):
#     """Fetch market data with improved error handling and memory management"""
#     try:
#         from django.core.management import call_command
        
#         # Check available memory before starting
#         import psutil
#         process = psutil.Process(os.getpid())
#         memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        
#         if memory_usage > 500:  # If using more than 500MB
#             logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
        
#         # Run the command
#         call_command('fetch_market_data')
        
#     except Exception as e:
#         logger.error(f"❌ Error in fetch_market_data_task: {str(e)}")
#         raise self.retry(exc=e)



# @shared_task(
#     bind=True,
#     max_retries=3,
#     retry_backoff=True,
#     retry_backoff_max=600,
#     retry_jitter=True,
#     queue='market_analysis'
# )
# def generate_recommendations_task(self):
#     """Generate investment recommendations with error handling and memory management"""
#     try:
#         # Check available memory before starting
#         process = psutil.Process(os.getpid())
#         memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        
#         if memory_usage > 500:  # If using more than 500MB
#             logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
        
#         # Run the command
#         call_command('generate_recommendations')
        
#         logger.info("Successfully completed generate_recommendations task")
        
#     except Exception as e:
#         logger.error(f"❌ Error in generate_recommendations_task: {str(e)}")
#         raise self.retry(exc=e)

# # Chain these tasks to run in sequence
# @shared_task(
#     bind=True,
#     max_retries=3,
#     queue='market_data'
# )
# def process_market_data_chain(self):
#     """Chain market data fetching and recommendation generation"""
#     try:
#         from celery import chain
        
#         # Create a chain of tasks
#         task_chain = chain(
#             fetch_market_data_task.s(),
#             generate_recommendations_task.s()
#         )
        
#         # Execute the chain
#         result = task_chain()
        
#         logger.info("Started market data processing chain")
#         return result
        
#     except Exception as e:
#         logger.error(f"❌ Error in process_market_data_chain: {str(e)}")
#         raise self.retry(exc=e)



# @shared_task(bind=True, max_retries=3)
# def fetch_currencies_task(self):
#     """
#     Celery task to fetch currencies and store them in Firebase
#     """
#     try:
#         logger.info("Starting currency data fetch")
#         call_command('fetch_currencies')  # This calls our management command
#         logger.info("Successfully completed currency data fetch")
#     except Exception as exc:
#         logger.error(f"Failed to execute currency fetch: {exc}", exc_info=True)
#         raise self.retry(exc=exc, countdown=5)  # Retry after 5 seconds



# kraken trading commands 
@shared_task(bind=True)
def surface_signals(self):
    """Fetch market data with improved error handling and memory management"""
    try:
        from django.core.management import call_command
        
        # Check available memory before starting
        import psutil
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        
        if memory_usage > 500:  # If using more than 500MB
            logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
        
        # Run the command
        call_command('surface_signals')
        
    except Exception as e:
        logger.error(f"❌ Error in surface_signals: {str(e)}")
        raise self.retry(exc=e)
    


@shared_task(bind=True)
def trade_agent(self):
    """Fetch market data with improved error handling and memory management"""
    try:
        from django.core.management import call_command
        
        # Check available memory before starting
        import psutil
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        
        if memory_usage > 500:  # If using more than 500MB
            logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
        
        # Run the command
        call_command('trade_agent')
        
    except Exception as e:
        logger.error(f"❌ Error in trade_agent: {str(e)}")
        raise self.retry(exc=e)
    

@shared_task(bind=True)
def order_watch(self):
    """Fetch market data with improved error handling and memory management"""
    try:
        from django.core.management import call_command
        
        # Check available memory before starting
        import psutil
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        
        if memory_usage > 500:  # If using more than 500MB
            logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
        
        # Run the command
        call_command('order_watch')
        
    except Exception as e:
        logger.error(f"❌ Error in order_watch: {str(e)}")
        raise self.retry(exc=e)
    

def kraken_profit(self):
    """Fetch market data with improved error handling and memory management"""
    try:
        from django.core.management import call_command
        
        # Check available memory before starting
        import psutil
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        
        if memory_usage > 500:  # If using more than 500MB
            logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
        
        # Run the command
        call_command('kraken_profit')
        
    except Exception as e:
        logger.error(f"❌ Error in kraken_profit: {str(e)}")
        raise self.retry(exc=e)
    

# def kraken_forex(self):
#     """Fetch market data with improved error handling and memory management"""
#     try:
#         from django.core.management import call_command
        
#         # Check available memory before starting
#         import psutil
#         process = psutil.Process(os.getpid())
#         memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        
#         if memory_usage > 500:  # If using more than 500MB
#             logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
        
#         # Run the command
#         call_command('kraken_forex')
        
#     except Exception as e:
#         logger.error(f"❌ Error in fetch_market_data_task: {str(e)}")
#         raise self.retry(exc=e)
@shared_task(bind=True)
def close_positions(self):
    """Fetch market data with improved error handling and memory management"""
    try:
        from django.core.management import call_command
        
        # Check available memory before starting
        import psutil
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        
        if memory_usage > 500:  # If using more than 500MB
            logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
        
        # Run the command
        call_command('close_positions')
        
    except Exception as e:
        logger.error(f"❌ Error in close_positions: {str(e)}")
        raise self.retry(exc=e)
    
# Then your implementation:
@app.task
def run_parallel_tasks():
    parallel_tasks = group([
        surface_signals.s(),
        trade_agent.s(),
        order_watch.s()
    ])
    result = parallel_tasks.apply_async()
    return result.id