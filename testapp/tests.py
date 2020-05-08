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

    def test_insert_attrs(self):
        """ Object inserted attributes are equal to retrieved from db."""
        self.assert_object_fields(self.obj, **self.defaults)
