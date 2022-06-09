import json
from django.core.management.base import BaseCommand, CommandError
from django.db import NotSupportedError
from ...redis_client import redis_client
from ... import models
import redis
import time
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Launch a django-redis-consumergroup 'worker' process"

    def add_arguments(self, parser) -> None:
        parser.add_argument('consumer_name', type=str)
        parser.add_argument('--stream', action='store', dest="stream_name", default="drcg", type=str, help="Redis Stream name")
        parser.add_argument('--group', action='store', dest="group_name", default="drcg", type=str, help="Redis ConsumerGroup name")
        return super().add_arguments(parser)

    def handle(self, consumer_name, stream_name, group_name, **options):
        logger.info(f"Starting worker '{consumer_name}' in group '{group_name}' on stream '{stream_name}'...")

        vmajor, vminor, vpatch = redis_client.info()['redis_version'].split('.')
        if int(vmajor) < 5:
            raise NotSupportedError("DRCG relies on Redis Streams from Redis version >= 5.0.")

        use_xautoclaim = int(vmajor) >= 6 and int(vminor) >= 2
        if not use_xautoclaim:
            logger.warning(f"Message recovery via XAUTOCLAIM not available in this Redis version (have {vmajor}.{vminor}.{vpatch}, need >= 6.2.0)")

        group = models.RedisConsumerGroup(group_name, stream_name)
        consumer = models.RedisConsumer(consumer_name)

        consumer.run(group, autoclaim=use_xautoclaim)