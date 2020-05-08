from django.db.models import manager

from manticore.models.query import SearchQuerySet


class SearchManager(manager.BaseManager.from_queryset(SearchQuerySet)):
    pass
