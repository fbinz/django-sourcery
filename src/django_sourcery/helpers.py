from functools import wraps
from django.db import (
    DEFAULT_DB_ALIAS,
    connections,
)


class TransactionRequiredError(Exception):
    pass


def inside_transaction(using=None):
    if using is None:
        using = DEFAULT_DB_ALIAS
    return connections[using].in_atomic_block


def require_transaction(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not inside_transaction():
            raise TransactionRequiredError()

        return fn(*args, **kwargs)

    return wrapper
