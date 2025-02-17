INSTALLED_APPS = [
    "django_sourcery",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

SECRET_KEY = "dummy_secret_key"
