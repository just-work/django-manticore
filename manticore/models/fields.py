import json

from django.db import models

__all__ = ['JSONField', 'MultiField']


class JSONField(models.Field):
    def db_type(self, connection):
        return 'json'

    def get_internal_type(self):
        return "JSONField"

    def get_prep_value(self, value):
        return value

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def from_db_value(self, value, expression, connection):
        return json.loads(value)


class MultiField(models.PositiveIntegerField):
    def db_type(self, connection):
        return 'multi'

    def get_internal_type(self):
        return 'MultiField'

    def get_prep_value(self, value):
        return value

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def from_db_value(self, value, expression, connection):
        if not value:
            return []
        return list(map(int, value.split(',')))


class BigMultiField(models.BigIntegerField):
    def db_type(self, connection):
        return 'multi64'

    def get_internal_type(self):
        return 'BigMultiField'

    def get_prep_value(self, value):
        return value

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def from_db_value(self, value, expression, connection):
        value = value or ''
        return list(map(int, value.split(',')))
