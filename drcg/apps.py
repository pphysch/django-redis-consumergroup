from django.apps import AppConfig


class DrcgConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'drcg'
    verbose_name = 'DRCG'

    def ready(self) -> None:
        from . import models
        for s in models.Scheduler.objects.all():
            models.RedisConsumerGroup(s.group_name, s.stream_name)
        return super().ready()