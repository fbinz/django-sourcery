from dataclasses import asdict, dataclass
import datetime
from itertools import groupby
from typing import Iterable, Self

from django.db import models
from django.db.models import Q, When
from django.utils.translation import gettext_lazy as _
from django.core.serializers import deserialize, serialize

from django_sourcery.helpers import require_transaction


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
            # TODO model might not have an 'id' field -> use primary key
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
            # TODO model might not have an 'id' field -> use primary key
            object_id=self.id,
            applied_to_version=self.version,
            state=serialize("json", [self]),
        )

    @classmethod
    def restore(
        cls,
        *,
        object_id,
        version: int | None = None,
        timestamp: datetime.datetime | None = None,
    ):
        if (version is None) == (timestamp is None):
            raise ValueError("Either version or timestamp need to be specified")

        name_prefix = cls.__qualname__

        snapshots = EventRecord.objects.filter(
            object_id=object_id,
            name__startswith=name_prefix,
            type=EventRecord.Type.SNAPSHOT,
        ).order_by("applied_to_version")

        if version:
            snapshots = snapshots.filter(applied_to_version__lte=version)
        else:
            snapshots = snapshots.filter(timestamp__lte=timestamp)

        events = EventRecord.objects.filter(
            object_id=object_id,
            name__startswith=name_prefix,
            type=EventRecord.Type.EVENT,
        ).order_by("applied_to_version")

        if version:
            events = events.filter(applied_to_version__lt=version)
        else:
            events = events.filter(timestamp__lte=timestamp)

        snapshot = snapshots.first()
        if snapshot is not None:
            instance = cls._load_snapshot(snapshot)
            events = events.filter(applied_to_version__gte=snapshot.applied_to_version)
        else:
            instance = cls(id=object_id)

        for event in events:
            event_class = getattr(cls, event.name.split(".")[-1])
            instance.apply(event_class(**event.state))

        return instance

    @classmethod
    def _load_snapshot(cls, snapshot: "EventRecord") -> Self:
        return next(deserialize("json", snapshot.state)).object

    @classmethod
    def restore_many(
        cls, *, ids: Iterable[int], timestamp: datetime.datetime
    ) -> dict[int, Self]:
        """
        Load the state of the objects with the given IDs at the given time.

        ``timestamp`` is inclusive.
        """
        if not isinstance(ids, set):
            ids = frozenset(ids)

        if not ids:
            return {}

        name_prefix = cls.__qualname__
        # For each object ID, find the most recent snapshot
        snapshots = EventRecord.objects.filter(
            object_id__in=ids,
            name__startswith=name_prefix,
            timestamp__lte=timestamp,
            type=EventRecord.Type.SNAPSHOT,
        )
        snapshot_map = {}
        timestamp_map = dict[int, datetime.datetime]()
        for snapshot in snapshots:
            if (
                snapshot.object_id not in timestamp_map
                or snapshot.timestamp > timestamp_map[snapshot.object_id]
            ):
                timestamp_map[snapshot.object_id] = snapshot.timestamp
                snapshot_map[snapshot.object_id] = snapshot
        # Build a query to get all changes that need to be applied to each snapshot
        timestamp_restriction = (
            (Q(object_id=object_id) & Q(timestamp__gt=timestamp_map[object_id]))
            if object_id in timestamp_map
            else Q(object_id=object_id)  # Must apply all changes
            for object_id in ids
        )
        sub_query = None
        for q_object in timestamp_restriction:
            if sub_query is None:
                sub_query = q_object
            else:
                sub_query |= q_object

        all_events = EventRecord.objects.filter(
            sub_query,
            name__startswith=name_prefix,
            timestamp__lte=timestamp,
            type=EventRecord.Type.EVENT,
        ).order_by("object_id", "applied_to_version")

        instances = {
            object_id: cls._load_snapshot(snapshot)
            for object_id, snapshot in snapshot_map.items()
        }
        for object_id, events in groupby(all_events, key=lambda e: e.object_id):
            instance = instances.get(object_id)
            if instance is None:
                instance = cls(id=object_id)

            for event in events:
                event_class = getattr(cls, event.name.split(".")[-1])
                instance.apply(event_class(**event.state))

            instances[object_id] = instance

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
