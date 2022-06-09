from django.apps import AppConfig


class IntegrationTestAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'integration_test_project.integration_test_app'

    def ready(self) -> None:
        from . import tasks
        return super().ready()