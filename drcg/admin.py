from django.contrib import admin
from . import models, views

# Register your models here.

@admin.register(models.Scheduler)
class SchedulerAdmin(admin.ModelAdmin):
    list_display = ['__str__', 'stream_name', 'group_name', 'ready_time']
    readonly_fields = ['last_message_id', 'ready_time']

@admin.register(models.RedisInfo)
class RedisInfoAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request):
        return False

    def has_module_permission(self, request):
        return request.user.has_module_perms('drcg')

    def changelist_view(self, request):
        return views.redis_admin(request)

    def delete_view(self, request, object_id=0):
        return views.delete_consumer(request)
