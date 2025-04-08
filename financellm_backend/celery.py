# financellm_backend/celery.py

from __future__ import absolute_import, unicode_literals
import os
import platform
from celery import Celery
from celery.schedules import crontab
from django.conf import settings
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set the default Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'financellm_backend.settings')

# Cross-platform worker settings
SYSTEM = platform.system().lower()
if SYSTEM == 'darwin':  # macOS
    os.environ.setdefault('OBJC_DISABLE_INITIALIZE_FORK_SAFETY', 'YES')
    worker_pool = 'threads'
    worker_pool_options = {
        'max_tasks_per_child': 100,
        'concurrency': 4  # Can safely use higher values with threads
    }
else:  # Linux and others
    worker_pool = 'prefork'
    worker_pool_options = {
        'prefetch_multiplier': 4,
        'max_tasks_per_child': 1000,
        'concurrency': 4
    }

app = Celery('financellm_backend')

# Configure Celery based on platform
app.conf.update(
    worker_pool=worker_pool,
    worker_pool_options=worker_pool_options,
    worker_max_memory_per_child=150000,  # 150MB
    task_time_limit=3600,  # 1 hour
    task_soft_time_limit=3000,  # 50 minutes
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],

    # Add these event-related settings
    worker_send_task_events=True,
    task_send_sent_event=True,
    task_track_started=True,
    task_track_received=True,
    
    # Debug settings
    task_always_eager=False,
    
    # Broker settings
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0'
)

app.config_from_object('django.conf:settings', namespace='CELERY')
app.conf.beat_schedule = {
    'surface-signals': {
        'task': 'apps.market.tasks.surface_signals',
        'schedule': int(os.getenv('SIGNALS_INTERVAL', 300)),  # 5 minutes
        'options': {
            'expires': int(os.getenv('SIGNALS_INTERVAL', 600)),  # 10 minutes
            'retry': True,
            'max_retries': 3
        }
    },
    'trade-agent': {
        'task': 'apps.market.tasks.trade_agent',
        'schedule': int(os.getenv('TRADE_AGENT_INTERVAL', 1200)),  # 20 minutes
        'options': {
            'expires': int(os.getenv('TRADE_AGENT_INTERVAL', 2400)),  # 40 minutes
            'retry': True,
            'max_retries': 3
        }
    },
    'order-watch': {
        'task': 'apps.market.tasks.order_watch',
        'schedule': int(os.getenv('ORDER_WATCH_INTERVAL', 600)),  # 10 minutes
        'options': {
            'expires': int(os.getenv('ORDER_WATCH_INTERVAL', 1200)),  # 20 minutes
            'retry': True,
            'max_retries': 3
        }
    },

    # Removing the combined task since each task now runs independently
    # 'run-market-tasks': {
    #     'task': 'apps.market.tasks.run_parallel_tasks',
    #     'schedule': int(os.getenv('MARKET_TASKS_INTERVAL', 600)),
    #     'options': {
    #         'expires': int(os.getenv('MARKET_TASKS_INTERVAL', 3200)),
    #         'retry': True,
    #         'max_retries': 3
    #     }
    # },
}

# Task routing - Use separate queues for each task
app.conf.task_routes = {
    # Each task is now in its own queue
    'apps.market.tasks.surface_signals': {'queue': 'kraken_signals'},
    'apps.market.tasks.trade_agent': {'queue': 'kraken_agent'},
    'apps.market.tasks.order_watch': {'queue': 'kraken_monitor'},
}

app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)