from django.db.models import base

from manticore.models.manager import SearchManager


__all__ = ['SearchIndex']


class SearchIndexBase(base.ModelBase):
    pass


class SearchIndex(base.Model, metaclass=SearchIndexBase):
    class Meta:
        abstract = True

    objects = SearchManager()
