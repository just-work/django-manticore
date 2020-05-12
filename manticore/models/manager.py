from django.db.models import manager

from manticore.models.query import SearchQuerySet


class SearchManager(manager.BaseManager.from_queryset(SearchQuerySet)):
    """ Manager for search indices."""
    # Copies SearchQuerySet.match() method to Manager
