from datetime import timedelta

from django.db import connections
from django.test import TransactionTestCase
from django.utils import timezone
from django_testing_utils.mixins import BaseTestCase

from testapp import models


class SearchIndexTestCaseBase(BaseTestCase, TransactionTestCase):
    model = models.TestModel
    databases = {'default', 'manticore'}

    def setUp(self):
        super().setUp()
        self.now = timezone.utc.normalize(timezone.now()).replace(microsecond=0)
        self.defaults = self.get_model_defaults()
        with connections['manticore'].cursor() as c:
            c.execute("SHOW TABLES LIKE %s", ('testapp_testmodel',))
            if c.rowcount > 0:
                c.execute("TRUNCATE RTINDEX testapp_testmodel")
        self.obj = self.model.objects.create(**self.defaults)

    def get_model_defaults(self):
        defaults = {
            'sphinx_field': 'hello sphinx field',
            'attr_uint': 100500,
            'attr_bool': True,
            'attr_bigint': 2 ** 33,
            'attr_float': 1.2345,
            'attr_multi': [1, 2, 3],
            'attr_multi_64': [2 ** 33, 2 ** 34],
            'attr_timestamp': self.now,
            'attr_string': 'hello sphinx attr',
            'attr_json': {'json': 'test'},
        }
        return defaults


class SearchIndexTestCase(SearchIndexTestCaseBase):

    def assert_exists(self, **kwargs):
        try:
            other = self.model.objects.get(**kwargs)
            self.assertEqual(other.pk, self.obj.pk)
        except self.model.DoesNotExist:  # pragma: no cover
            self.fail("lookup failed for %s" % kwargs)

    def assert_excluded(self, **kwargs):
        items = list(self.model.objects.exclude(**kwargs))
        self.assertFalse(items)

    def test_insert_attrs(self):
        """ Object inserted attributes are equal to retrieved from db."""
        self.assert_object_fields(self.obj, **self.defaults)

    def test_filter_by_attributes(self):
        """ Filtering over attributes works."""
        exclude = ['attr_multi', 'attr_multi_64', 'attr_json', 'sphinx_field']
        for key in self.defaults:
            if key in exclude:
                continue
            value = getattr(self.obj, key)
            self.assert_exists(**{key: value})

    def test_filter_multi_fields(self):
        """ Filtering over multi attributes works."""
        multi_lookups = dict(
            attr_multi=self.obj.attr_multi[0],
            attr_multi_64=self.obj.attr_multi_64[0],
            attr_multi__in=[self.obj.attr_multi[0], 100],
            attr_multi_64__in=[self.obj.attr_multi_64[0], 1]
        )
        for key, value in multi_lookups.items():
            self.assert_exists(**{key: value})

    def test_exclude_by_attrs(self):
        """ Exclude items by attributes works."""
        exclude = ['attr_multi', 'attr_multi_64', 'attr_json', 'sphinx_field',
                   'attr_float']
        for key in self.defaults:
            if key in exclude:
                continue
            value = getattr(self.obj, key)
            self.assert_excluded(**{key: value})

    def test_exclude_by_value_list(self):
        exclude = ['attr_multi', 'attr_multi_64', 'attr_json', 'sphinx_field',
                   'attr_float']
        for key in self.defaults.keys():
            if key in exclude:
                continue
            value = getattr(self.obj, key)
            filter_kwargs = {"%s__in" % key: [value]}
            self.assert_excluded(**filter_kwargs)

    def test_numeric_lookups(self):
        """ Numeric fields could be compared to greater/less."""
        numeric_lookups = dict(
            attr_uint__gte=0,
            attr_timestamp__gte=self.now,
            attr_multi__gte=0,
            attr_multi_64__gte=0,
            attr_float__gte=0.0,
        )

        for k, v in numeric_lookups.items():
            self.assert_exists(**{k: v})

    def test_update_attributes(self):
        """ Attributes could be updated via model save with update_fields."""
        new_values = {
            'attr_uint': 200,
            'attr_bool': False,
            'attr_bigint': 2 ** 35,
            'attr_float': 5.4321,
            'attr_multi': [6, 7, 8],
            'attr_multi_64': [2 ** 34, 2 ** 35],
            'attr_timestamp': self.now + timedelta(seconds=60),
            'attr_string': "another string",
            'attr_json': {"json": "other", 'add': 3},
        }

        for k, v in new_values.items():
            setattr(self.obj, k, v)

        # Check UPDATE mode (string attributes are not updated)
        self.obj.save(update_fields=new_values.keys())

        self.assert_object_fields(self.obj, **new_values)

    def test_update_indexed_fields(self):
        """ Indexed fields may be updated with REPLACE query."""
        new_values = {
            'sphinx_field': "another_field",
            'other_field': "another other",
        }
        for k, v in new_values.items():
            setattr(self.obj, k, v)

        self.obj.save()
