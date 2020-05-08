from django.db.models import base


class SearchIndexBase(base.ModelBase):
    pass


class SearchIndex(base.Model, metaclass=SearchIndexBase):
    class Meta:
        abstract = True
