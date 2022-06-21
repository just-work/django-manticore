import operator
from functools import reduce

from django.db.models.query import QuerySet
from django.db.models.sql import AND
from django.db.models.expressions import RawSQL

from manticore.models import sql
from manticore.sphinxql.expressions import T, Match, F
from manticore.sphinxql.base import SphinxQLCombinable, SphinxQLNode


class SearchQuerySet(QuerySet):

    def __init__(self, model=None, query=None, using=None, hints=None):
        query = query or sql.SearchQuery(model)
        super().__init__(model, query, using, hints)

    def match(self, *args, **kwargs):
        """ Very large usage description here."""
        qs: SearchQuerySet = self._clone()
        expression = self._build_match_expression(*args, **kwargs)
        qs._match_expression().add(expression)
        return qs

    def options(self, **kwargs):
        """ Adds OPTIONS clause to search query."""
        qs: SearchQuerySet = self._clone()
        for item in kwargs:
            if not isinstance(kwargs[item], dict):
                continue
            i = ', '.join(f'`{k}`={v}' for k, v in kwargs[item].items())
            kwargs[item] = RawSQL(i, [])
        qs.query.options.update(kwargs)
        return qs

    @staticmethod
    def _build_match_expression(*args, **kwargs):
        """ Transforms *args, **kwargs to SphinxQL DSL."""
        terms = []
        for term in args:
            if isinstance(term, str):
                terms.append(T(term))
            elif isinstance(term, (SphinxQLCombinable, SphinxQLNode)):
                terms.append(term)
            else:
                raise TypeError(term)
        terms.extend(map(lambda pair: F(*pair), kwargs.items()))
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
