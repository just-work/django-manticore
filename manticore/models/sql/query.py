from django.db.models import sql

from manticore.models.sql.where import ManticoreWhereNode


class SearchQuery(sql.Query):
    def __init__(self, model, where=ManticoreWhereNode, alias_cols=True):
        super().__init__(model, alias_cols=alias_cols)
        self.where = where()
        self.where_class = where
        self.options = {}

    def clone(self):
        query = super().clone()
        query.options = self.options.copy()
        return query
