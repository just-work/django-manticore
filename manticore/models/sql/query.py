from django.db.models import sql

from manticore.models.sql.where import ManticoreWhereNode


class SearchQuery(sql.Query):
    def __init__(self, model, where=ManticoreWhereNode, alias_cols=True):
        super().__init__(model, where, alias_cols)
        self.options = {}

    def clone(self):
        query = super().clone()
        query.options = self.options.copy()
        return query
