import datetime
from functools import partial
from django.utils import timezone
from decimal import Decimal
from core.models import Order, Payment
from freezegun import freeze_time
import pytest

from django_sourcery.models import EventRecord


@pytest.mark.django_db(transaction=True)
def test_create():
    order = Order.create(total=100)

    assert EventRecord.objects.count() == 1

    er = EventRecord.objects.first()
    assert er.topic == "Order.OrderCreated"
    assert er.originator_id == order.id
    assert er.originator_version == 1
    assert er.state == {"total": 100, "version": 1}


@pytest.mark.django_db(transaction=True)
def test_create_and_pay():
    order = Order.create(total=100)
    order.process_payment(payment=Payment(amount=50, order_version=order.version))

    assert order.total == 50

    records = list(EventRecord.objects.order_by("originator_version"))

    assert len(records) == 2

    er = records[0]
    assert er.topic == "Order.OrderCreated"
    assert er.originator_id == order.id
    assert er.originator_version == 1
    assert er.state == {"total": 100, "version": 1}

    er = records[1]
    assert er.topic == "Order.PaymentReceived"
    assert er.originator_id == order.id
    assert er.originator_version == 2
    assert er.state == {"amount": 50, "version": 2}


@pytest.mark.django_db(transaction=True)
def test_restore_by_version():
    order = Order.create(total=100)
    order_version_1 = order.version

    order.process_payment(
        payment=Payment(
            amount=50,
            order_version=order_version_1,
        )
    )
    order_version_2 = order.version

    order.process_payment(
        payment=Payment(
            amount=50,
            order_version=order_version_2,
        )
    )
    order_version_3 = order.version

    assert order.total == 0

    o = Order.restore(id=order.id, version=order_version_1)
    assert o.total == 100

    o = Order.restore(id=order.id, version=order_version_2)
    assert o.total == 50

    o = Order.restore(id=order.id, version=order_version_3)
    assert o.total == 0


@pytest.mark.django_db(transaction=True)
def test_restore_by_timestamp():
    mk_datetime = partial(
        datetime.datetime, 2025, 3, 4, tzinfo=timezone.get_default_timezone()
    )

    timestamp_1 = mk_datetime(hour=10)
    timestamp_2 = mk_datetime(hour=11)
    timestamp_3 = mk_datetime(hour=12)

    with freeze_time(timestamp_1):
        order = Order.create(total=100)

    with freeze_time(timestamp_2):
        order.process_payment(payment=Payment(amount=50, order_version=order.version))

    with freeze_time(timestamp_3):
        order.process_payment(payment=Payment(amount=50, order_version=order.version))

    assert order.total == 0

    o = Order.restore(id=order.id, timestamp=timestamp_1)
    assert o.total == 100

    o = Order.restore(id=order.id, timestamp=timestamp_2)
    assert o.total == 50

    o = Order.restore(id=order.id, timestamp=timestamp_3)
    assert o.total == 0


@pytest.mark.django_db(transaction=True)
def test_restore_many():
    mk_datetime = partial(
        datetime.datetime, 2025, 3, 4, tzinfo=timezone.get_default_timezone()
    )

    timestamp_1 = mk_datetime(hour=10)
    timestamp_2 = mk_datetime(hour=11)
    timestamp_3 = mk_datetime(hour=12)

    with freeze_time(timestamp_1):
        order_1 = Order.create(total=100)
        order_2 = Order.create(total=200)

    with freeze_time(timestamp_2):
        order_1.process_payment(
            payment=Payment(amount=50, order_version=order_1.version)
        )
        order_2.process_payment(
            payment=Payment(amount=130, order_version=order_2.version)
        )

    with freeze_time(timestamp_3):
        order_1.process_payment(
            payment=Payment(amount=50, order_version=order_1.version)
        )
        order_2.process_payment(
            payment=Payment(amount=70, order_version=order_2.version)
        )

    assert order_1.total == 0
    assert order_2.total == 0

    os = Order.restore_many(ids=[order_1.id, order_2.id], timestamp=timestamp_1)
    assert os[0].total == 100
    assert os[1].total == 200

    os = Order.restore_many(ids=[order_1.id, order_2.id], timestamp=timestamp_2)
    assert os[0].total == 50
    assert os[1].total == 70

    os = Order.restore_many(ids=[order_1.id, order_2.id], timestamp=timestamp_3)
    assert os[0].total == 0
    assert os[1].total == 0
