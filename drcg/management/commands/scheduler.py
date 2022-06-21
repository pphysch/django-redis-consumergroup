from django.core.management.base import BaseCommand, CommandError
import redis
import time
from ... import models

class Command(BaseCommand):
    help = "Launch a django-redis-consumergroup scheduler process"

    def add_arguments(self, parser) -> None:
        parser.add_argument('--poll-interval', action='store', dest="poll_interval_seconds", default=10.0, type=float, help="Polling interval (s)")
        return super().add_arguments(parser)

    def handle(self, poll_interval_seconds, **options):
        running = True
        try:
            while running:
                for task in models.Scheduler.objects.ready_to_launch():
                    task.launch()
                time.sleep(poll_interval_seconds)
        except KeyboardInterrupt:
            print()
