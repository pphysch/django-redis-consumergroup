import time
from django.utils import timezone
from drcg import tasks, redis

default_cg = redis.RedisConsumerGroup()

@tasks.register
def test_task(seconds: float) -> None:
    time.sleep(seconds)
    return timezone.now()