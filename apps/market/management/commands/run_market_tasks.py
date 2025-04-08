# apps/market/management/commands/run_market_tasks.py

from django.core.management.base import BaseCommand
from celery import group
from apps.market.tasks import surface_signals, trade_agent, order_watch

class Command(BaseCommand):
    help = 'Runs market analysis tasks in parallel'

    def handle(self, *args, **options):
        self.stdout.write('Starting parallel market tasks...')
        
        parallel_tasks = group([
            surface_signals.s(),
            trade_agent.s(),
            order_watch.s()
        ])
        
        result = parallel_tasks.apply_async()
        self.stdout.write(f'Tasks launched with group ID: {result.id}')