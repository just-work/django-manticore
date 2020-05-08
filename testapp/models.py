from django.db import models
from django.utils import timezone

from manticore.models import SearchIndex, JSONField
from manticore.models.fields import MultiField, BigMultiField


class FieldMixin(models.Model):
    class Meta:
        abstract = True
    sphinx_field = models.TextField(default='')
    other_field = models.TextField(default='')

    attr_bigint = models.BigIntegerField(default=0)
    attr_bool = models.BooleanField(default=False)
    attr_float = models.FloatField(default=0.0)
    attr_json = JSONField(default={})
    attr_multi_64 = BigMultiField(default=[])
    attr_multi = MultiField(default=[])
    attr_string = models.CharField(max_length=32, default='')
    attr_timestamp = models.DateTimeField(default=timezone.now)
    attr_uint = models.IntegerField(default=0)


class TestModel(FieldMixin, SearchIndex):
    pass
