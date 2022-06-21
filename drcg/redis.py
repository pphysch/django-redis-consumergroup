from django.conf import settings
import django.core.cache
from django.core.exceptions import ImproperlyConfigured
import json
import logging
import redis
from .tasks import task_registry

logger = logging.getLogger(__name__)

redis_client: redis.Redis
if hasattr(settings, 'DRCG_REDIS_HOST'):
    redis_client = redis.Redis(
        host=getattr(settings, "DRCG_REDIS_HOST", "localhost"),
        port=getattr(settings, "DRCG_REDIS_PORT", 6379))
else:
    if isinstance(django.core.cache.cache._cache, django.core.cache.backends.redis.RedisCacheClient):
        redis_client = django.core.cache.cache._cache.get_client()
    else:
        raise ImproperlyConfigured("DRCG couldn't find a valid Redis instance. Please configure a Redis cache backend, or specify `DRCG_REDIS_HOST`.")

class RedisStream:
    lookup = dict()

    def __new__(cls, name: str = "drcg", *args, **kwargs):
        try:
            return RedisStream.lookup[name]
        except KeyError:
            return super().__new__(cls)

    def __init__(self, name: str = "drcg", *args, **kwargs):
        if name in RedisStream.lookup:
            return

        self.name = name
        RedisStream.lookup[name] = self

    def enqueue(self, task_name, **task_kwargs):
        args = json.dumps(task_kwargs)
        message_id = redis_client.xadd(self.name, {"task":task_name, "args":args})
        logger.debug(f"enqueued_task {self.name}:{message_id} {task_name} {args}")
        return message_id

    def info(self):
        return redis_client.xinfo_stream(self.name)

    def group_info(self):
        return redis_client.xinfo_groups(self.name)

    def trim(self, maxlen=1048576, approximate=True):
        """
        By default, drop all entries from the stream beyond the most recent ~million.
        """
        return redis_client.xtrim(self.name, maxlen=maxlen, approximate=approximate)

    def hard_reset(self):
        return redis_client.delete(self.name)

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

    def enqueue(self, **kwargs):
        """See RedisStream.enqueue"""
        return self.stream.enqueue(**kwargs)

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

    def read(self, group: RedisConsumerGroup, start_id: bytes, count=1, block=5000):
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

    def run(self, group, start_id='0-0', process_backlog=True, autoclaim=True, block=5000):
        """
        Launch the consumer and begin consuming messages and executing tasks.
        """
        last_id = start_id
        has_backlog = process_backlog

        logger.info(f"Starting worker '{self.name}' in group '{group.name}' on stream '{group.stream.name}'...")
        running = True
        while running:
            cur_id = last_id if has_backlog else '>'

            messages = self.read(group, cur_id, block=block)
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