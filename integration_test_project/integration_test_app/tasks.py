import time
from drcg import tasks, models

default_cg = models.RedisConsumerGroup()

@tasks.register
def easy_task(n):
    return n+1

@tasks.register
def medium_task(n):
    time.sleep(5)
    return n*2

@tasks.register
def hard_task(n):
    time.sleep(60)
    return n**2