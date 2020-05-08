from django.db import models

from manticore.models import SearchIndex


class FieldMixin(SearchIndex):
    class Meta:
        abstract = True
    sphinx_field = models.TextField(default='')
    other_field = models.TextField(default='')
    # attr_uint = spx_models.SphinxIntegerField(default=0, db_column='attr_uint_')
    # attr_bigint = spx_models.SphinxBigIntegerField(default=0)
    attr_float = models.FloatField(default=0.0)
    # attr_timestamp = spx_models.SphinxDateTimeField(default=datetime.now)
    attr_string = models.CharField(max_length=32, default='')
    # attr_multi = spx_models.SphinxMultiField(default=[])
    # attr_multi_64 = spx_models.SphinxMulti64Field(default=[])
    # attr_json = spx_models.SphinxJSONField(default={})
    attr_bool = models.BooleanField(default=False)


class TestModel(FieldMixin, SearchIndex):
    pass
