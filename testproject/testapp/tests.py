from datetime import timedelta

from django.db import connections
from django.test import utils
from django.utils import timezone
from django_testing_utils.mixins import BaseTestCase

from manticore.routers import ManticoreRouter, is_search_index
from manticore.sphinxql.expressions import F, T, P
from testproject.testapp import models


class SearchIndexTestCaseBase(BaseTestCase):
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

    def get_new_attr_values(self):
        new_values = {
            'attr_uint': 200,
            'attr_bool': False,
            'attr_bigint': 2 ** 35,
            'attr_float': 5.4321,
            'attr_multi': [6, 7, 8],
            'attr_multi_64': [2 ** 34, 2 ** 35],
            'attr_timestamp': self.now + timedelta(seconds=60),
            'attr_string': "another string",
        }
        return new_values

    # noinspection PyMethodMayBeStatic
    def get_new_field_values(self):
        new_values = {
            'attr_json': {"json": "other", 'add': 3},
            'sphinx_field': "another_field",
            'other_field': "another other",
        }
        return new_values


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

    def test_json_field_null(self):
        """ NULL in attr_json is not supported."""
        self.obj.attr_json = None
        self.obj.save()
        self.assert_object_fields(self.obj, attr_json=None)

    def test_update_attributes(self):
        """ Attributes could be updated via model save with update_fields."""
        new_values = self.get_new_attr_values()

        for k, v in new_values.items():
            setattr(self.obj, k, v)

        # Check UPDATE mode (string attributes are not updated)
        self.obj.save(update_fields=new_values.keys())

        self.assert_object_fields(self.obj, **new_values)

    def test_update_indexed_fields(self):
        """ Indexed fields may be updated with REPLACE query."""
        new_values = self.get_new_field_values()
        for k, v in new_values.items():
            setattr(self.obj, k, v)
        self.obj.save()
        expected = {**self.defaults, **new_values}
        self.assert_object_fields(self.obj, **expected)

    def test_update_fields_with_indexed_fields(self):
        """ REPLACE with update_fields is not supported."""
        with self.assertRaises(NotImplementedError):
            self.obj.save(update_fields=('sphinx_field',))

    def test_queryset_update_attributes(self):
        """ UPDATE for queryset is supported for attributes."""
        qs = self.model.objects.filter(attr_uint=self.defaults['attr_uint'])
        new_values = self.get_new_attr_values()

        self.assertEqual(1, qs.update(**new_values))

        expected = {**self.defaults, **new_values}
        self.assert_object_fields(self.obj, **expected)

    def test_get_or_create(self):
        """ get_or_create works correctly."""
        values = {**self.get_new_attr_values(), **self.get_new_field_values()}
        attr_uint = values.pop('attr_uint')

        obj, created = self.model.objects.get_or_create(
            values, attr_uint=attr_uint)

        self.assertTrue(created)
        self.assert_object_fields(obj, attr_uint=attr_uint, **values)

        obj, created = self.model.objects.get_or_create(
            values, attr_uint=self.obj.attr_uint)
        self.assert_object_fields(obj, **self.defaults)

    def test_update_or_create(self):
        """ update_or_create works correctly."""
        values = {**self.get_new_attr_values(), **self.get_new_field_values()}
        attr_uint = values.pop('attr_uint')

        obj, created = self.model.objects.update_or_create(
            values, attr_uint=attr_uint)

        self.assertTrue(created)
        self.assert_object_fields(obj, attr_uint=attr_uint, **values)

        obj, created = self.model.objects.update_or_create(
            values, attr_uint=self.obj.attr_uint)
        self.assert_object_fields(obj, attr_uint=self.obj.attr_uint,
                                  **values)

    def test_delete_object(self):
        """ DELETE single object works."""
        self.assertEqual(self.model.objects.count(), 1)
        self.obj.delete()
        self.assertEqual(self.model.objects.count(), 0)

    def test_bulk_delete(self):
        """ DELETE set of primary keys works."""
        self.obj.delete()
        objs = [self.model(**self.defaults) for _ in range(10)]
        self.model.objects.bulk_create(objs)
        expected = [obj.pk for obj in objs]
        delete_ids = expected[3:7]

        self.model.objects.filter(id__in=delete_ids).delete()

        qs = self.model.objects.filter(id__in=delete_ids)
        self.assertEqual(len(qs), 0)
        qs = self.model.objects.all().values_list('id', flat=True)
        self.assertListEqual(list(qs), expected[:3] + expected[7:])

    def test_bulk_create(self):
        """ bulk_creates sets primary key value."""
        objs = [self.model(**self.defaults) for _ in range(10)]
        self.model.objects.bulk_create(objs)
        for obj in objs:
            self.assertIsNotNone(obj.pk)
            self.assert_object_fields(obj, **self.defaults)

    def test_bulk_create_single_object(self):
        """ bulk_create works correctly for single object."""
        objs = [self.model(**self.defaults)]
        self.model.objects.bulk_create(objs)
        self.assertIsNotNone(objs[0].pk)

    def assert_match(self, qs, sphinxql, escape=True):
        if escape:
            escape = connections['manticore'].connection.literal
            sphinxql = escape(sphinxql).decode('utf-8')
            match_expression = f"MATCH({sphinxql})"
        else:
            match_expression = f"MATCH('{sphinxql}')"

        with utils.CaptureQueriesContext(connections['manticore']) as ctx:
            result = list(qs)
        self.assertIn(match_expression, ctx.captured_queries[-1]['sql'])
        return result

    def test_match_text(self):
        """ Full-text search by plain text is supported."""
        qs = self.model.objects.match("hello")
        objs = self.assert_match(qs, "(hello)")
        self.assertEqual(objs, [self.obj])

    def test_match_text_and_other_text(self):
        """ two subsequent match calls combined with &."""
        qs = self.model.objects.match("hello")
        qs = qs.match("sphinx")
        self.assert_match(qs, "(hello) & (sphinx)")

    def test_match_multiple_terms(self):
        """ passing space-separated text is not split to words."""
        qs = self.model.objects.match("hello sphinx")
        self.assert_match(qs, "(hello sphinx)")

    def test_single_escape_characters(self):
        """
        https://docs.manticoresearch.com/latest/html/searching/escaping_in_queries.html
        """
        chars = r'''!"$'()-/<@\^|~'''
        for c in chars:
            qs = self.model.objects.match(f'{c} hello')
            if c in ['"', "'", '\\']:
                # escaped by mysql also
                c = fr'\{c}'
            objs = self.assert_match(qs, fr"(\\{c} hello)", escape=False)
            self.assertListEqual(objs, [self.obj])

    def test_match_one_or_another(self):
        """ Operator OR (|) works with terms."""
        qs = self.model.objects.match(T("hello") | T("world"))
        objs = self.assert_match(qs, '(hello) | (world)')
        self.assertListEqual(objs, [self.obj])

    def test_match_with_filter(self):
        """ Filtering over attributes works with full-text search."""
        qs = self.model.objects.match("hello").filter(
            attr_uint=self.obj.attr_uint)
        objs = list(qs)
        self.assertListEqual(objs, [self.obj])

    def test_match_field(self):
        """ Match over index fields is supported with keyword arguments."""
        qs = self.model.objects.match(sphinx_field="hello")
        objs = self.assert_match(qs, '(@sphinx_field (hello))')
        self.assertListEqual(objs, [self.obj])

        qs = self.model.objects.match(sphinx_field='sphinx',
                                      other_field='other')
        self.assert_match(
            qs, '(@sphinx_field (sphinx)) & (@other_field (other))')

    def test_field_search_expression(self):
        """ Match with field search expression."""

        qs = self.model.objects.match(F(sphinx_field='hello'))
        self.assert_match(qs, '(@sphinx_field (hello))')

        qs = self.model.objects.match(F('sphinx_field', 'other_field', 'hello'))
        self.assert_match(qs, '(@(sphinx_field,other_field) (hello))')

        qs = self.model.objects.match(F('other_field', T('wat'), exclude=True))
        self.assert_match(qs, '(@!other_field (wat))')

        qs = self.model.objects.match(F('sphinx_field', 'other_field',
                                        T('text') & ~T('exclude'),
                                        exclude=True))
        self.assert_match(
            qs, '(@!(sphinx_field,other_field) (text) & !(exclude))')

    def test_field_search_validation(self):
        """ F object must validate invalid initialization."""
        self.assertRaises(TypeError, F, sphinx_field=F('other_field', 'value'))

        self.assertRaises(ValueError, F, sphinx_field='one', other_field='two')

        self.assertRaises(ValueError, F)

        self.assertRaises(ValueError, F, 'sphinx_field')

        self.assertRaises(ValueError, F, 'sphinx_field', 'text',
                          other_field='two')

    def test_phrase_term(self):
        qs = self.model.objects.match(F(sphinx_field=P("phrase search")))
        self.assert_match(qs, '(@sphinx_field ("phrase search"))')


class ManticoreRouterTestCase(BaseTestCase):
    databases = {'default', 'manticore'}

    def setUp(self):
        super().setUp()
        self.test_model = models.TestModel.objects.create()
        self.django_model = models.DjangoModel.objects.create(title='title')
        self.router = ManticoreRouter()

    def test_is_search_index(self):
        for obj in self.test_model, models.TestModel:
            self.assertTrue(is_search_index(obj))

        for obj in self.django_model, models.DjangoModel:
            self.assertFalse(is_search_index(obj))

    def test_db_for_read(self):
        self.assertEqual(self.router.db_for_read(self.test_model), 'manticore')
        self.assertIsNone(self.router.db_for_read(self.django_model))

    def test_db_for_write(self):
        self.assertEqual(self.router.db_for_write(self.test_model), 'manticore')
        self.assertIsNone(self.router.db_for_write(self.django_model))

    def test_allow_relation(self):
        self.assertIsNone(self.router.allow_relation(self.django_model,
                                                     self.django_model))
        cases = [
            (self.test_model, self.test_model),
            (self.test_model, self.django_model),
            (self.django_model, self.test_model),
        ]
        for obj1, obj2 in cases:
            self.assertFalse(self.router.allow_relation(obj1, obj2))

    def test_allow_migrate(self):
        self.assertIsNone(self.router.allow_migrate('default', 'testapp'))
        self.assertIsNone(self.router.allow_migrate('manticore', 'testapp'))

        self.assertFalse(self.router.allow_migrate(
            'default', 'testapp', 'TestModel'))
        self.assertTrue(self.router.allow_migrate(
            'manticore', 'testapp', 'TestModel'))

        self.assertIsNone(self.router.allow_migrate(
            'default', 'testapp', 'DjangoModel'))
        self.assertFalse(self.router.allow_migrate(
            'manticore', 'testapp', 'DjangoModel'))


class NonTransactionalTestCase(BaseTestCase):
    databases = {'default', 'manticore'}

    def setUp(self):
        super().setUp()
        self.obj = models.TestModel.objects.create(attr_uint=123)

    def test_1(self):
        obj = models.TestModel.objects.create(attr_uint=321)
        self.assert_object_fields(obj, attr_uint=321)

    def test_2(self):
        # object created in test_1 is removed in tearDown
        self.assertEqual(
            models.TestModel.objects.filter(attr_uint=321).count(), 0)
