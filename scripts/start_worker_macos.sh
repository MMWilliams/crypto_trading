# scripts/start_worker_macos.sh
#!/bin/bash

# Set environment variable
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

# Start worker with solo pool
celery -A financellm_backend worker \
    --pool=solo \
    --queues=celery,market_data,news,maintenance \
    --loglevel=INFO \
    -E