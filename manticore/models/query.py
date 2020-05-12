import operator
from functools import reduce

from django.db.models import query
from django.db.models.sql import AND

from manticore.models.sql.sphinxql import *


class SearchQuerySet(query.QuerySet):
    def match(self, *args, **kwargs):
        """ Very large usage description here."""
        qs: SearchQuerySet = self._clone()
        expression = self._build_match_expression(*args, **kwargs)
        qs._match_expression().add(expression)
        return qs

    @staticmethod
    def _build_match_expression(*args):
        """ Transforms *args, **kwargs to SphinxQL DSL."""
        terms = []
        for term in args:
            if isinstance(term, str):
                terms.append(T(term))
            elif isinstance(term, (SphinxQLCombinable, SphinxQLNode)):
                terms.append(term)
            else:
                raise ValueError(term)
        return reduce(operator.and_, terms)

    def _match_expression(self) -> Match:
        """ Gets or adds Match expression to where clause."""
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
