import datetime
from itertools import groupby
from django.db import models
from dataclasses import asdict, dataclass
from django.db.models.functions import RowNumber
from django.db.models import Q, Window, F
from django.utils.translation import gettext_lazy as _
from django_sourcery.helpers import require_transaction
from django.core.serializers import deserialize, serialize


@dataclass(kw_only=True)
class Event:
    version: int

    def apply(self, _):
        pass


class Aggregate(models.Model):
    version = models.PositiveIntegerField(default=0)

    class Meta:
        abstract = True

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

    def snapshot(self):
        topic = self.__class__.__qualname__

        EventRecord.objects.create(
            type=EventRecord.Type.SNAPSHOT,
            topic=topic,
            originator_id=self.id,
            originator_version=self.version,
            state=serialize("json", [self]),
        )

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

        topic_prefix = cls.__qualname__

        snapshots = EventRecord.objects.filter(
            originator_id=id,
            topic__startswith=topic_prefix,
            type=EventRecord.Type.SNAPSHOT,
        ).order_by("originator_version")

        if version:
            snapshots = snapshots.filter(originator_version__lte=version)
        else:
            snapshots = snapshots.filter(timestamp__lte=timestamp)

        events = EventRecord.objects.filter(
            originator_id=id,
            topic__startswith=topic_prefix,
            type=EventRecord.Type.EVENT,
        ).order_by("originator_version")

        if version:
            events = events.filter(originator_version__lte=version)
        else:
            events = events.filter(timestamp__lte=timestamp)

        snapshot = snapshots.first()
        if snapshot is not None:
            instance = next(deserialize("json", snapshot.state)).object
            events = events.filter(originator_version__gt=snapshot.originator_version)
        else:
            instance = cls(id=id)

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


class EventRecordType(models.IntegerChoices):
    EVENT = 1, _("Event")
    SNAPSHOT = 2, _("Snapshot")


class EventRecord(models.Model):
    # Type
    Type = EventRecordType
    type = models.PositiveSmallIntegerField(
        choices=EventRecordType,
        default=EventRecordType.EVENT.value,
    )

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
                condition=Q(type=EventRecordType.EVENT),
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
