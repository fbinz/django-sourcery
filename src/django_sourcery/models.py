import datetime
from itertools import groupby
from typing import Iterable
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

        name = event.__class__.__qualname__
        EventRecord.objects.create(
            name=name,
            object_id=self.id,
            applied_to_version=self.version,
            state=asdict(event),
        )

        self.apply(event)

    def snapshot(self):
        name = self.__class__.__qualname__

        EventRecord.objects.create(
            type=EventRecord.Type.SNAPSHOT,
            name=name,
            object_id=self.id,
            applied_to_version=self.version,
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

        name_prefix = cls.__qualname__

        snapshots = EventRecord.objects.filter(
            object_id=id,
            name__startswith=name_prefix,
            type=EventRecord.Type.SNAPSHOT,
        ).order_by("applied_to_version")

        if version:
            snapshots = snapshots.filter(applied_to_version__lte=version)
        else:
            snapshots = snapshots.filter(timestamp__lte=timestamp)

        events = EventRecord.objects.filter(
            object_id=id,
            name__startswith=name_prefix,
            type=EventRecord.Type.EVENT,
        ).order_by("applied_to_version")

        if version:
            events = events.filter(applied_to_version__lt=version)
        else:
            events = events.filter(timestamp__lte=timestamp)

        snapshot = snapshots.first()
        if snapshot is not None:
            instance = next(deserialize("json", snapshot.state)).object
            events = events.filter(applied_to_version__gte=snapshot.applied_to_version)
        else:
            instance = cls(id=id)

        for event in events:
            event_class = getattr(cls, event.name.split(".")[-1])
            instance.apply(event_class(**event.state))

        return instance

    @classmethod
    def restore_many(cls, *, ids: Iterable[int], timestamp: datetime.datetime):
        name_prefix = cls.__qualname__

        all_events = EventRecord.objects.filter(
            object_id__in=ids,
            name__startswith=name_prefix,
            timestamp__lte=timestamp,
            type=EventRecord.Type.EVENT,
        ).order_by("object_id", "applied_to_version")

        instances = []
        for id, events in groupby(all_events, key=lambda e: e.object_id):
            instance = cls(id=id)

            for event in events:
                event_class = getattr(cls, event.name.split(".")[-1])
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
        choices=EventRecordType.choices,
        default=EventRecordType.EVENT.value,
    )

    # Name of aggregate
    name = models.CharField(max_length=50)

    # Object ID (ID of an aggregate)
    object_id = models.PositiveBigIntegerField()

    # Object version (version of an aggregate)
    applied_to_version = models.PositiveBigIntegerField()

    # The serialized state of the event
    state = models.JSONField()

    # Timestamp
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    "object_id",
                    "applied_to_version",
                ],
                condition=Q(type=EventRecordType.EVENT),
                name="record_unique_applied_to_version",
            )
        ]
