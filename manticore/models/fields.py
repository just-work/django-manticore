import json

from django.db import models

__all__ = ['JSONField']


class JSONField(models.Field):
    def db_type(self, connection):
        return 'json'

    def get_internal_type(self):
        return "JSONField"

    def get_prep_value(self, value):
        return value

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def from_db_value(self, value, expression, connection):
        return json.loads(value)
