from django.db import models
from django.utils import timezone

from manticore.models import fields, SearchIndex


class FieldMixin(models.Model):
    class Meta:
        abstract = True
    sphinx_field = fields.RTField()
    other_field = fields.RTField()

    attr_bigint = models.BigIntegerField(default=0)
    attr_bool = models.BooleanField(default=False)
    attr_float = models.FloatField(default=0.0)
    attr_json = fields.JSONField(default={})
    attr_multi_64 = fields.BigMultiField(default=[])
    attr_multi = fields.MultiField(default=[])
    attr_string = models.CharField(max_length=32, default='')
    attr_timestamp = models.DateTimeField(default=timezone.now)
    attr_uint = models.IntegerField(default=0)


class TestModel(FieldMixin, SearchIndex):
    pass


class DjangoModel(models.Model):
    title = models.CharField(max_length=32)
