import pytest
from django.db import transaction

from django_sourcery.helpers import TransactionRequiredError, require_transaction


@pytest.mark.django_db(transaction=True)
def test_transaction_required_regular():
    @require_transaction
    def f():
        pass

    with transaction.atomic():
        f()


@pytest.mark.django_db(transaction=True)
def test_transaction_required_fail():
    @require_transaction
    def f():
        pass

    with pytest.raises(TransactionRequiredError):
        f()
