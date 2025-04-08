# Celery Setup and Operation Guide
```bash
# enable environment
source /Users/maureesewilliams/financellm_backend/venv/bin/activate
```


## Kill existing celery processes

pkill -f celery

## 1. Checking and Starting the Redis Message Broker

```bash
# Check if Redis is running (should return PONG)
redis-cli ping

# Start Redis if it's not running
brew services start redis
```

## 2. Starting Celery Services

You need two components running simultaneously (in separate terminal windows):

```bash
#run each worker on its own terminal
# Start the worker for signals with its own log file
celery -A financellm_backend worker -Q kraken_signals -n signals_worker@%h --loglevel=info --logfile=logs/kraken_signals.log

# Start the worker for agent with its own log file
celery -A financellm_backend worker -Q kraken_agent -n agent_worker@%h --loglevel=info --logfile=logs/kraken_agent.log

# Start the worker for monitoring with its own log file
celery -A financellm_backend worker -Q kraken_monitor -n monitor_worker@%h --loglevel=info --logfile=logs/kraken_monitor.log

# For the beat scheduler as well
celery -A financellm_backend beat --loglevel=info --logfile=logs/celery_beat.log



```

To run workers for specific queues:

```bash
# Run workers for specific queues
celery -A financellm_backend worker -Q default,market_data,market_analysis,maintenance,news,kraken_trading -l INFO
```

## Monitor the  tasks and workers via Flower in browser UI
 Install flower
 ```bash

pip install flower

# Run flower
celery -A financellm_backend flower --port=5555
```

Then open your browser at http://localhost:5555 


## 3. Executing Tasks Manually

### Run all tasks immediately:

```bash
python3 manage.py run_parallel_market_tasks
```

### Run all scheduled tasks immediately:

```bash
python manage.py shell
```

Then in the Python shell:

```python
from financellm_backend.celery import app

# Get all scheduled tasks
scheduled_tasks = app.conf.beat_schedule

# Run each scheduled task
for task_name, task_config in scheduled_tasks.items():
    task_path = task_config['task']
    task = app.tasks[task_path]
    print(f"Running {task_path}")
    task.delay()
```

## 4. Monitoring Tasks

```bash
# View active tasks
celery -A financellm_backend inspect active

# View scheduled tasks
celery -A financellm_backend inspect scheduled

# Start Flower monitoring interface (if installed)
celery -A financellm_backend flower --port=5555
```


## Monitoring th elogs

```bash
tail -n 20 -f celery.log
```