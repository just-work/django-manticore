from django.db.models import base, options

from manticore.models.manager import SearchManager


__all__ = ['SearchIndex']


INDEX_OPTIONS = (
    'min_prefix_len',
    'regexp_filter',
    'blend_chars',
    'charset_table',
)

options.DEFAULT_NAMES += INDEX_OPTIONS


class SearchIndexBase(base.ModelBase):
    """ Search index metaclass used in ManticoreRouter."""


class SearchIndex(base.Model, metaclass=SearchIndexBase):
    """ Search index base model."""
    class Meta:
        abstract = True

    objects = SearchManager()
