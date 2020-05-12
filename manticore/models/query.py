from django.db.models import query
from django.db.models.sql import AND

from manticore.models.sql.sphinxql import Match, T


class SearchQuerySet(query.QuerySet):
    def match(self, *args, **kwargs):
        """ Very large usage description here."""
        qs: SearchQuerySet = self._clone()
        expression = self._build_match_expression(*args, **kwargs)
        qs._match_expression().add(expression)
        return qs

    @staticmethod
    def _build_match_expression(term):
        if isinstance(term, str):
            return T(term)
        return term

    def _match_expression(self) -> Match:
        where = self.query.where
        if where.connector != AND:
            raise ValueError(f"MATCH can't be used with {where.connector}")

        for node in where.children:
            if isinstance(node, Match):
                match = node
                break
        else:
            match = Match()
            where.add(match, AND)

        return match
