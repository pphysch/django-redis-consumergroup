from django.core.management.base import BaseCommand, CommandError
from django.db import NotSupportedError
from ...redis import redis_client, RedisStream, RedisConsumerGroup, RedisConsumer
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

        vmajor, vminor, vpatch = redis_client.info()['redis_version'].split('.')
        if int(vmajor) < 5:
            raise NotSupportedError("DRCG relies on Redis Streams from Redis version >= 5.0.")

        use_xautoclaim = int(vmajor) >= 6 and int(vminor) >= 2
        if not use_xautoclaim:
            logger.warning(f"Message recovery via XAUTOCLAIM not available in this Redis version (have {vmajor}.{vminor}.{vpatch}, need >= 6.2.0)")

        group = RedisConsumerGroup(group_name, stream_name)
        consumer = RedisConsumer(consumer_name)

        try:
            consumer.run(group, autoclaim=use_xautoclaim)
        except KeyboardInterrupt:
            print()