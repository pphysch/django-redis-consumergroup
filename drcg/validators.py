from django.forms import ValidationError
from django.utils.translation import gettext as _
from .tasks import task_registry


def validate_task_name(task_name: str):
    if task_name not in task_registry:
        raise ValidationError(_("Unknown task name %(task_name)s. If the function exists, make sure it is registered in an imported file."), code="unknown_task", params={'task_name':task_name})