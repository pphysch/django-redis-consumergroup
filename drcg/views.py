from django.shortcuts import redirect, render
from django.utils import timezone
from datetime import datetime, timedelta

from django.contrib import admin
from . import models
from .redis import redis_client

def delete_consumer(request):
    try:
        stream, group, consumer = request.GET['stream'], request.GET['group'], request.GET['consumer']
        redis_client.xgroup_delconsumer(stream, group, consumer)
    except KeyError as e:
        raise e

    return redirect('admin:drcg_redisinfo_changelist')

def redis_admin(request):
    """
    Transforms on-demand Redis XINFO output into a useful view.
    """
    streams = list()
    consumergroups = list()
    consumers = list()
    for stream in models.RedisStream.lookup.values():
        # add stream name, parse entries and last ts
        info = stream.info()
        info.update({
            "name": stream.name,
            "first_entry_ts": datetime.fromtimestamp(int(info["first-entry"][0].split(b'-')[0])/1000),
            "last_entry": info["last-entry"],
            "last_entry_ts": datetime.fromtimestamp(int(info["last-entry"][0].split(b'-')[0])/1000),
        })
        streams.append(info)

        for cg in stream.group_info():
            cg.update({
                "stream": stream,
                "last_delivered_id": cg["last-delivered-id"],
                "last_delivered_ts": datetime.fromtimestamp(int(cg["last-delivered-id"].split(b'-')[0])/1000),
                })
            consumergroups.append(cg)

    for cg in models.RedisConsumerGroup.lookup.values():
        for c in cg.consumer_info():
            consumer = models.RedisConsumer(c["name"].decode())
            #pending_messages = consumer.pending_messages(cg, count=1)
            pending_messages = []
            c.update({
                "stream": cg.stream,
                "group": cg,
                "last_action_ts": timezone.now() - timedelta(milliseconds=c["idle"]),
                "pending_message": pending_messages[0] if pending_messages else '-',
            })
            consumers.append(c)

    context = dict(
        admin.site.each_context(request),
        streams=streams,
        consumergroups=consumergroups,
        consumers=consumers,
        )

    return render(request, "drcg/redis-admin.html", context)