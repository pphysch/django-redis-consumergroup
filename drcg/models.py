import json
import logging
from random import random
from django.conf import settings
from django.db import models
from datetime import timedelta
from django.utils import timezone
from functools import cache

from .validators import validate_task_name

import redis
from .redis_client import redis_client
from .tasks import task_registry

logger = logging.getLogger(__name__)


class Scheduler(models.Model):
    class QuerySet(models.QuerySet):
        def ready_to_launch(self):
            return self.order_by('ready_time').filter(ready_time__lte=timezone.now())

    objects = QuerySet.as_manager()

    task_name = models.CharField(max_length=255, validators=[validate_task_name])
    task_args = models.JSONField(
        blank=True,
        default=dict,
    )

    interval = models.DurationField(
        default=timedelta(hours=1),
        help_text = "HH:MM:SS"
    )
    jitter = models.BooleanField(
        "Apply jitter?",
        default=True,
        help_text="Apply a 5% jitter to the interval (e.g. 10s -> [9.5s, 10.5s])"
    )
    
    stream_name = models.SlugField(
        default="drcg",
    )
    group_name = models.SlugField(
        default="drcg",
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

        self.last_message_id = cg.enqueue(task=self.task_name, args=json.dumps(self.task_args)).decode()
        if self.jitter:
            self.ready_time = timezone.now() + self.interval * (0.95 + random()/10) # 5% jitter
        else:
            self.ready_time = timezone.now() + self.interval
        self.save()
        logger.debug(f"launched_task '{self}'")
        return self.last_message_id

    def __str__(self):
        return f"{self.task_name}({','.join([k+'='+str(v) for k,v in self.task_args.items()])})/{self.interval}"

class RedisInfo(models.Model):
    """
    An empty, unmanaged class for hooking Redis info into the Django Admin interface.
    """
    class Meta:
        managed=False
        verbose_name_plural = "redis info"


class RedisStream:
    lookup = dict()

    def __new__(cls, name: str = "drcg", *args, **kwargs):
        try:
            return RedisStream.lookup[name]
        except KeyError:
            return super().__new__(cls)

    def __init__(self, name: str = "drcg", *args, **kwargs):
        if name in RedisConsumerGroup.lookup:
            return

        self.name = name
        RedisStream.lookup[name] = self

    def enqueue(self, **data):
        return redis_client.xadd(self.name, data)

    def info(self):
        return redis_client.xinfo_stream(self.name)

    def group_info(self):
        return redis_client.xinfo_groups(self.name)

    def trim(self, maxlen=1048576, approximate=True):
        """
        By default, drop all entries from the stream beyond the most recent ~million.
        """
        return redis_client.xtrim(self.name, maxlen=maxlen, approximate=approximate)

    def __str__(self):
        return self.name


class RedisConsumerGroup:
    lookup = dict()

    def __new__(cls, group_name: str = "drcg", stream_name: str = "drcg", *args, **kwargs):
        try:
            return RedisConsumerGroup.lookup[f"{stream_name}:{group_name}"]
        except KeyError:
            return super().__new__(cls)

    def __init__(self, group_name: str = "drcg", stream_name: str = "drcg", mkstream: bool = True, autoclaim_after_ms = 60*60*1000, *args, **kwargs):
        if f"{stream_name}:{group_name}" in RedisConsumerGroup.lookup:
            return

        self.name = group_name
        self.fullname = f"{stream_name}:{group_name}"
        self.autoclaim_after_ms = autoclaim_after_ms

        try:
            redis_client.xgroup_create(stream_name, self.name, '$', mkstream=mkstream)
        except redis.exceptions.ResponseError as e:
            if not str(e).startswith('BUSYGROUP Consumer Group name already exists'):
                raise e
        
        if stream_name not in RedisStream.lookup:
            if not mkstream:
                raise ValueError(f"The stream '{stream_name}' has not been declared and mkstream=False")
            RedisStream.lookup[stream_name] = RedisStream(name=stream_name)

        self.stream = RedisStream.lookup[stream_name]

        RedisConsumerGroup.lookup[self.fullname] = self

    def enqueue(self, **data):
        return self.stream.enqueue(**data)

    def consumer_info(self):
        return redis_client.xinfo_consumers(self.stream.name, self.name)

    def was_delivered(self, id):
        return id <= self.stream.group_info()[0]['last-delivered-id']

    def is_pending(self, id):
        return bool(redis_client.xpending_range(self.stream.name, self.name, id, id, 1))

    def check_completed(self, id):
        """Returns True if the id is neither pending nor undelivered to this ConsumerGroup"""
        if not self.is_pending(id) and self.was_delivered(id):
            return True

        # Check if the ID is outside of the stream's domain and therefore incompletable
        first_entry = self.stream.info()['first-entry']
        if first_entry:
            if id < first_entry[0]:
                return True
        else:
            return True

        return False

    def __str__(self):
        return self.fullname


class RedisConsumer:
    lookup = dict()

    def __new__(cls, consumer_name: str, *args, **kwargs):
        try:
            return RedisConsumer.lookup[consumer_name]
        except KeyError:
            return super().__new__(cls)

    def __init__(self, consumer_name: str, *args, **kwargs):
        if consumer_name in RedisConsumerGroup.lookup:
            return

        self.name = consumer_name

        RedisConsumer.lookup[self.name] = self

    def read(self, group: RedisConsumerGroup, start_id: bytes, count=1, block=10_000):
        """
        Request any number of messages from a ConsumerGroup.
        """
        return redis_client.xreadgroup(group.name, self.name, streams={group.stream.name:start_id}, count=1, block=block)
    
    def autoclaim(self, group, count=1):
        """Claim an excessively pending message (requires Redis >= 6.2.0)"""
        return redis_client.xautoclaim(group.stream.name, group.name, self.name, group.autoclaim_after_ms, count=count)

    def pending_messages(self, group, count=1):
        messages = list()
        for message in redis_client.xpending_range(group.stream.name, group.name, '-', '+', count, consumername=self.name):
            messages.extend(redis_client.xrange(group.stream.name, min=message['message_id'], max=message['message_id'], count=1))

        return messages

    def handle_task(self, task_message: bytes):
        """
        Execute the function(*args) described by `task_message` and return the result.
        """
        func = task_registry[task_message[b'task'].decode()]
        args = json.loads(task_message[b'args'].decode())
        result = func(**args)
        return result

    def run(self, group, start_id='0-0', process_backlog=True, autoclaim=True):
        """
        Launch the consumer and begin consuming messages and executing tasks.
        """
        last_id = start_id
        has_backlog = process_backlog

        running = True
        while running:
            cur_id = last_id if has_backlog else '>'

            messages = self.read(group, cur_id)
            logger.debug(f"received_tasks {messages}")

            if autoclaim and not messages:
                messages = self.autoclaim(group)
                logger.debug(f"recovered_tasks {messages}")
                
            if not messages:
                continue

            has_backlog = (len(messages[0][1]) > 0)

            for id, message in messages[0][1]:
                logger.debug(f"processing_task {id} {message}")
                result = self.handle_task(message)
                redis_client.xack(group.stream.name, group.name, id)
                logger.debug(f"acknowledged_task_complete {id} result='{result}'")
                last_id = id