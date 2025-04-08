# scripts/start_celery.py

import platform
import os
import subprocess
import sys

def get_celery_command():
    system = platform.system().lower()
    base_cmd = [
        'celery',
        '-A', 'financellm_backend',
        '--queues=celery,market_data,news,maintenance'
    ]
    
    if system == 'darwin':  # macOS
        os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
        worker_cmd = base_cmd + ['worker', '--concurrency=1', '--loglevel=INFO']
    else:  # Linux and others
        worker_cmd = base_cmd + ['worker', '--concurrency=4', '--loglevel=INFO']
        
    return worker_cmd

def main():
    if len(sys.argv) < 2:
        print("Usage: python start_celery.py [worker|beat]")
        sys.exit(1)

    command_type = sys.argv[1]
    
    if command_type == 'worker':
        cmd = get_celery_command()
    elif command_type == 'beat':
        cmd = ['celery', '-A', 'financellm_backend', 'beat', '--loglevel=INFO']
    else:
        print("Invalid command. Use 'worker' or 'beat'")
        sys.exit(1)

    print(f"Running command: {' '.join(cmd)}")
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\nStopping Celery...")
        sys.exit(0)

if __name__ == "__main__":
    main()