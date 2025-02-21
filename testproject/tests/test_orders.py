from core.models import Order, Payment
import pytest

from django_sourcery.models import EventRecord


@pytest.mark.django_db(transaction=True)
def test_create():
    order = Order.create(total=100)

    assert EventRecord.objects.count() == 1

    er = EventRecord.objects.first()
    assert er.topic == "Order.OrderCreated"
    assert er.originator_id == order.id
    assert er.originator_version == 0
    assert er.state == {"total": 100, "version": 0}


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
    assert er.originator_version == 0
    assert er.state == {"total": 100, "version": 0}

    er = records[1]
    assert er.topic == "Order.PaymentReceived"
    assert er.originator_id == order.id
    assert er.originator_version == 1
    assert er.state == {"amount": 50, "version": 1}
