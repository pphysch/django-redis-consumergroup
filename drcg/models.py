import json
import logging
from random import random
from django.conf import settings
from django.db import models
from datetime import timedelta
from django.utils import timezone
from functools import cache
from django.utils.functional import lazy

from .redis import redis_client, RedisStream, RedisConsumerGroup, RedisConsumer
from .tasks import task_registry

logger = logging.getLogger(__name__)


class Scheduler(models.Model):
    class QuerySet(models.QuerySet):
        def ready_to_launch(self):
            return self.order_by('ready_time').filter(ready_time__lte=timezone.now())

    objects = QuerySet.as_manager()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._meta.get_field('task_name').choices = lazy(lambda: [(t,t) for t in sorted(task_registry.keys())], list)()
        self._meta.get_field('stream_name').choices = lazy(lambda: [(s,s) for s in sorted(RedisStream.lookup.keys())], list)()

    task_name = models.CharField(
        max_length=255,
        choices=((".","Reload page to view options"),),
    )
    task_args = models.JSONField(
        blank=True,
        default=dict,
    )

    interval = models.DurationField(
        default=timedelta(minutes=15),
        help_text = "HH:MM:SS"
    )
    jitter = models.BooleanField(
        "Apply jitter?",
        default=True,
        help_text="Apply a 5% jitter to the interval (e.g. 10s -> [9.5s, 10.5s])"
    )
    
    stream_name = models.SlugField(
        default="drcg",
        choices=(("drcg","Reload page to view options"),),
    )

    last_message_id = models.CharField(
        max_length=255,
        editable=False,
        null=True,
    )
    ready_time = models.DateTimeField(db_index=True, default=timezone.now)


    def launch(self):
        """Try to launch this task"""
        cg = RedisConsumerGroup(self.stream_name, self.group_name)

        if self.last_message_id and not cg.check_completed(self.last_message_id.encode()):
            logger.debug(f"failed_to_launch_incomplete_task '{self}' pending_id {self.last_message_id}")
            return self.last_message_id

        self.last_message_id = cg.enqueue(self.task_name, **self.task_args).decode()
        if self.jitter:
            self.ready_time = timezone.now() + self.interval * (0.95 + random()/10) # 5% jitter
        else:
            self.ready_time = timezone.now() + self.interval
        self.save()
        logger.debug(f"launched_task '{self}'")
        return self.last_message_id

    def __str__(self):
        return f"{self.task_name}({','.join([k+'='+str(v) for k,v in self.task_args.items()])})/{self.interval}"


class RedisDiagnostics(models.Model):
    """
    An empty, unmanaged class for hooking Redis info into the Django Admin interface.
    """
    class Meta:
        managed=False
        verbose_name_plural = "Redis diagnostics"
