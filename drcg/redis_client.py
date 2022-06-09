import django.core.cache

try:
    if isinstance(django.core.cache.cache._cache, django.core.cache.backends.redis.RedisCacheClient):
        redis_client = django.core.cache.cache._cache.get_client()
    else:
        raise Exception
except:
    import redis
    redis_client = redis.Redis()