import pytest
import time
import subprocess
from drcg import models
from drcg.redis import redis_client

# NOTE! These tests use all use the same real Redis Stream, so run them sequentially and not in parallel!

def start_consumer(name):
    cmd = f"/opt/django/manage.py consumer {name} --stream=test_drcg --group=test_drcg".split()
    return subprocess.Popen(cmd, stderr=subprocess.PIPE)

def test_consumer_recovery():
    """
    Ensure that a single worker process can restart and complete a claimed task even if it is killed in the middle of working on it.

    This tests worker-level recovery, not task-level recovery; if the worker never recovers, the claimed task will never complete.
    """
    stream = models.RedisStream("test_drcg")
    stream.hard_reset()
    group = models.RedisConsumerGroup("test_drcg", stream_name="test_drcg")
    task_message_id = stream.enqueue("test_task", seconds=5)

    # Start the worker, and stop it before it can complete the task (5s workload)
    consumer = start_consumer("test_worker1")
    time.sleep(1.5)
    consumer.kill()
    print(task_message_id)
    print("consumer stderr:")
    mentioned_task = False
    for line in iter(consumer.stderr.readline, b""):
        if task_message_id in line and b"processing_task" in line:
            mentioned_task = True
        print("\t", line)
    
    pending = models.RedisConsumer("test_worker1").pending_messages(group)
    assert task_message_id in dict(pending)

    # Start the second worker, and make sure it doesn't try to do any work, because all the work has already been claimed
    consumer = start_consumer("test_worker2")
    time.sleep(1.5)
    consumer.kill()
    print(task_message_id)
    print("consumer stderr:")
    mentioned_task = False
    for line in iter(consumer.stderr.readline, b""):
        if task_message_id in line:
            mentioned_task = True
        print("\t", line)
    assert not mentioned_task

    pending = models.RedisConsumer("test_worker2").pending_messages(group)
    assert len(pending) == 0

    # Restart the first worker, and allow it to complete the task
    consumer = start_consumer("test_worker1")
    time.sleep(7)
    consumer.kill()
    print(task_message_id)
    print("consumer stderr:")
    mentioned_task = False
    for line in iter(consumer.stderr.readline, b""):
        if task_message_id in line and b"acknowledged_task_complete" in line:
            mentioned_task = True
        print("\t", line)
    assert mentioned_task

    # Restart the first worker again, and make sure it doesn't pick up the same task because its already done
    consumer = start_consumer("test_worker1")
    time.sleep(3)
    consumer.kill()
    print(task_message_id)
    print("consumer stderr:")
    mentioned_task = False
    for line in iter(consumer.stderr.readline, b""):
        if task_message_id in line and b"processing_task" in line:
            mentioned_task = True
        print("\t", line)
    assert not mentioned_task

    pending = models.RedisConsumer("test_worker1").pending_messages(group)
    assert task_message_id not in dict(pending)


@pytest.mark.skipif(int(redis_client.info()['redis_version'].split('.')[0]) < 6, reason="Redis 6.2+ required for XAUTOCLAIM")
def test_task_recovery():
    """
    Ensure that a stuck task on a dead worker will eventually get reclaimed by another worker and successfully completed.

    (Requires Redis >= 6.2 for XAUTOCLAIM)
    """
    stream = models.RedisStream("test_drcg")
    stream.hard_reset()
    group = models.RedisConsumerGroup("test_drcg", stream_name="test_drcg", autoclaim_after_ms=5000)
    task_message_id = stream.enqueue("test_task", seconds=5)

    # Start the worker, and stop it before it can complete the task (5s workload)
    consumer = start_consumer("test_worker1")
    time.sleep(1.5)
    consumer.kill()
    print(task_message_id)
    print("consumer stderr:")
    mentioned_task = False
    for line in iter(consumer.stderr.readline, b""):
        if task_message_id in line and b"processing_task" in line:
            mentioned_task = True
        print("\t", line)
    
    pending = models.RedisConsumer("test_worker1").pending_messages(group)
    assert task_message_id in dict(pending)

    # A head-start on idle time
    time.sleep(1)

    # After a delay, start the second worker, and make sure it autoclaims and completes the task.
    # Note that workers only attempt to autoclaim when they are otherwise inactive, so give it plenty of time.
    consumer = start_consumer("test_worker2")
    time.sleep(20)
    consumer.kill()
    print(task_message_id)
    print("consumer stderr:")
    mentioned_task = False
    for line in iter(consumer.stderr.readline, b""):
        if task_message_id in line and b"acknowledged_task_complete" in line:
            mentioned_task = True
        print("\t", line)
    assert mentioned_task

    pending = models.RedisConsumer("test_worker1").pending_messages(group)
    assert task_message_id not in dict(pending)