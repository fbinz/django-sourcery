import datetime
from itertools import groupby
from django.db import models
from dataclasses import asdict, dataclass

from django_sourcery.helpers import require_transaction


@dataclass(kw_only=True)
class Event:
    version: int

    def apply(self, _):
        pass


class Aggregate(models.Model):
    version = models.PositiveIntegerField(default=1)

    def apply(self, event):
        event.apply(self)
        self.version += 1

    @require_transaction
    def trigger_event(self, event):
        if self.version != event.version:
            raise ValueError(f"Version mismatch: {self.version} != {event.version}")

        topic = event.__class__.__qualname__
        EventRecord.objects.create(
            topic=topic,
            originator_id=self.id,
            originator_version=self.version,
            state=asdict(event),
        )

        self.apply(event)

    @classmethod
    def restore(
        cls,
        *,
        id,
        version: int | None = None,
        timestamp: datetime.datetime | None = None,
    ):
        if len(list(filter(None, [version, timestamp]))) != 1:
            raise ValueError("Either version or timestamp need to be specified")

        instance = cls(id=id)

        topic_prefix = cls.__qualname__

        events = EventRecord.objects.filter(
            originator_id=id,
            topic__startswith=topic_prefix,
        ).order_by("originator_version")

        if version:
            events = events.filter(originator_version__lt=version)
        else:
            events = events.filter(timestamp__lte=timestamp)

        for event in events:
            event_class = getattr(cls, event.topic.split(".")[-1])
            instance.apply(event_class(**event.state))

        return instance

    @classmethod
    def restore_many(cls, *, ids, timestamp: datetime.datetime):
        topic_prefix = cls.__qualname__

        all_events = EventRecord.objects.filter(
            originator_id__in=ids,
            topic__startswith=topic_prefix,
            timestamp__lte=timestamp,
        ).order_by("originator_id", "originator_version")

        instances = []
        for id, events in groupby(all_events, key=lambda e: e.originator_id):
            instance = cls(id=id)

            for event in events:
                event_class = getattr(cls, event.topic.split(".")[-1])
                instance.apply(event_class(**event.state))

            instances.append(instance)

        return instances

    class Meta:
        abstract = True


class EventRecord(models.Model):
    # Topic (e.g. name of aggregate)
    topic = models.CharField(max_length=50)

    # Originator ID (id of an aggregate)
    originator_id = models.PositiveBigIntegerField()

    # Originator version (version of an aggregate)
    originator_version = models.PositiveBigIntegerField()

    # The serialized state of the event
    state = models.JSONField()

    # Timestamp
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    "originator_id",
                    "originator_version",
                ],
                name="record_unique_originator_version",
            )
        ]


class SnapshotRecord(models.Model):
    # Topic (e.g. name of aggregate)
    topic = models.CharField(max_length=50)

    # Originator ID (id of an aggregate)
    originator_id = models.PositiveBigIntegerField()

    # Originator version (version of an aggregate)
    originator_version = models.PositiveBigIntegerField()

    # The serialized state of the aggregate
    state = models.JSONField()

    # Timestamp
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    "originator_id",
                    "originator_version",
                ],
                name="snapshop_unique_originator_version",
            )
        ]
