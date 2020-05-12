from django.db.models import base

from manticore.models.manager import SearchManager


__all__ = ['SearchIndex']


class SearchIndexBase(base.ModelBase):
    """ Search index metaclass used in ManticoreRouter."""


class SearchIndex(base.Model, metaclass=SearchIndexBase):
    """ Search index base model."""
    class Meta:
        abstract = True

    objects = SearchManager()
