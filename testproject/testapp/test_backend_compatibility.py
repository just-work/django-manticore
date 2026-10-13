"""Regression checks for the Django and Manticore backend interfaces."""

from datetime import datetime, timezone
from unittest.mock import patch

from django.db import connections
from django.test import SimpleTestCase

from manticore.backend.base import DatabaseWrapper


class BackendCompatibilityTests(SimpleTestCase):
    def test_manticore_column_types_extend_mysql_types(self):
        connection = DatabaseWrapper(connections['manticore'].settings_dict, 'manticore')
        with patch.dict(connection.features.__dict__, has_native_uuid_field=False):
            data_types = connection.data_types

        self.assertEqual(data_types['CharField'], 'string')
        self.assertEqual(data_types['DateTimeField'], 'timestamp')
        self.assertIn('AutoField', data_types)

    def test_table_prefix_is_not_sent_as_mysql_database(self):
        settings = connections['manticore'].settings_dict.copy()
        settings['NAME'] = 'test_'
        connection = DatabaseWrapper(settings, 'manticore')
        params = connection.get_connection_params()

        self.assertNotIn('database', params)
        self.assertNotIn('db', params)

    def test_multi_fields_have_integer_ranges(self):
        ranges = connections['manticore'].ops.integer_field_ranges

        self.assertEqual(ranges['MultiField'], ranges['PositiveIntegerField'])
        self.assertEqual(ranges['BigMultiField'], ranges['BigIntegerField'])

    def test_datetime_is_converted_to_unix_utc_timestamp(self):
        value = datetime(2024, 1, 1, tzinfo=timezone.utc)

        self.assertEqual(
            connections['manticore'].ops.adapt_datetimefield_value(value),
            1704067200,
        )
