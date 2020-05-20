from manticore.sphinxql.base import SphinxQLCombinable, SphinxQLNode, escape

__all__ = [
    'Match',
    'T',
]


class T(SphinxQLCombinable):
    """ SphinxQL text term."""

    def __init__(self, term: str):
        self.term = term

    def as_sphinxql(self):
        return '(%s)', [self.term]


class Match(SphinxQLNode):
    """ Root match node that renders MATCH(...) """
    # duck typing for Django ORM
    contains_aggregate = False

    # noinspection PyUnusedLocal
    def as_sql(self, compiler, connection):
        sphinxql, params = self.as_sphinxql()
        expression = sphinxql % tuple(map(escape, params))
        return f"MATCH(%s)", [expression]
