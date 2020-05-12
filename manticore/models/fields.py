import json

from django.db import models

__all__ = ['BigMultiField', 'JSONField', 'MultiField', 'RTField']

from manticore.models import lookups


class RTField(models.TextField):
    """ Full-text search field (rt_field)."""

    def __init__(self, *, stored=True, **kwargs):
        """
        :param stored: store field contents in index and return in if necessary
        :param kwargs: common django text field kwargs
        """
        # null is not supported by manticore, '' is reasonable default
        kwargs['null'] = False
        kwargs['default'] = ''
        self.stored = stored
        super().__init__(**kwargs)

    def db_type(self, connection):
        sql = 'text indexed'
        if self.stored:
            sql += ' stored'
        return sql

    def get_internal_type(self):
        return "RTField"


class JSONField(models.Field):
    """ JSON field (attr_json)."""
    def db_type(self, connection):
        return 'json'

    def get_internal_type(self):
        return "JSONField"

    def get_prep_value(self, value):
        if value is None:
            return ''
        return json.dumps(value)

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def from_db_value(self, value, expression, connection):
        if value is None:
            return None
        return json.loads(value)


class MultiField(models.PositiveIntegerField):
    """ Multi-field (attr_multi). Contains a list of uint32."""
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


MultiField.register_lookup(lookups.MultiExact)


class BigMultiField(models.BigIntegerField):
    """ Multi-field for big integer (attr_multi64). Contains a list of int64."""

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


BigMultiField.register_lookup(lookups.MultiExact)
