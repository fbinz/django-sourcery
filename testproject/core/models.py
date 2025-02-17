from dataclasses import dataclass
from django.db import models, transaction

from django_sourcery.models import Aggregate, Event


@dataclass
class Payment:
    amount: float
    order_version: int


class Order(Aggregate):
    total = models.FloatField()

    @dataclass
    class OrderCreated(Event):
        total: float

        def apply(self, order):
            order.total = self.total

    @dataclass
    class PaymentReceived(Event):
        amount: float

        def apply(self, order):
            order.total -= self.amount

    @staticmethod
    @transaction.atomic
    def create(*, total) -> "Order":
        order = Order.objects.create(total=total)
        order.trigger_event(Order.OrderCreated(version=1, total=total))
        return order

    @transaction.atomic
    def process_payment(self, payment: Payment):
        self.trigger_event(
            Order.PaymentReceived(
                version=payment.order_version,
                amount=payment.amount,
            )
        )
        self.save()
