from django.apps import apps
from django.conf import settings

from manticore.models import base


def is_search_index(model_or_obj):
    if type(model_or_obj) is not base.SearchIndexBase:
        model = model_or_obj.__class__
    else:
        model = model_or_obj
    return (issubclass(model, base.SearchIndex) or
            type(model) is base.SearchIndexBase)


# noinspection PyUnusedLocal
class ManticoreRouter:
    db_name = getattr(settings, 'MANTICORE_DATABASE_NAME', 'manticore')

    def db_for_read(self, model, **hints):
        if is_search_index(model):
            return self.db_name

    def db_for_write(self, model, **hints):
        if is_search_index(model):
            return self.db_name

    @staticmethod
    def allow_relation(obj1, obj2, **hints):
        # Joins between search index and anything else are not allowed
        if is_search_index(obj1) or is_search_index(obj2):
            return False
        # No decisions are made for unrelated models
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if not model_name:
            # we can't tell anything
            return None
        model = apps.get_model(app_label, model_name)
        if is_search_index(model):
            # indices are migrated only for manticore db
            return db == self.db_name
        if db == self.db_name:
            # in manticore only indices are migrated
            return False
        return None
