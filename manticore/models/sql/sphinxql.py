from django.db.models.expressions import Combinable

ESCAPE = str.maketrans({k: rf"\{k}" for k in r'''!"$'()-/<@^|~'''})


def escape(s: str):
    s = s.replace('\\', '\\\\')
    s = s.translate(ESCAPE)
    return s


class SphinxQLCombinable(Combinable):

    def __and__(self, other):
        if isinstance(other, SphinxQLCombinable):
            return SphinxQLNode(self) & SphinxQLNode(other)

    def __rand__(self, other):
        return NotImplemented

    def __or__(self, other):
        if isinstance(other, SphinxQLCombinable):
            return SphinxQLNode(self) | SphinxQLNode(other)

    def __ror__(self, other):
        return NotImplemented


class SphinxQLNode:
    connector = '&'

    def __init__(self, *expressions, connector='&'):
        self.expressions = list(expressions)
        self.connector = connector

    def add(self, expression):
        self.expressions.append(expression)

    def as_sphinxql(self):
        sphinxql = []
        params = []
        for expr in self.expressions:
            s, p = expr.as_sphinxql()
            sphinxql.append(s)
            params.extend(p)
        return self.connector.join(sphinxql), params

    def _combine(self, other, connector):
        if not (isinstance(other, SphinxQLNode) and
                self.connector == other.connector):
            return SphinxQLNode(*self.expressions, other, connector=connector)
        return SphinxQLNode(*self.expressions, *other.expressions,
                            connector=connector)

    def __and__(self, other):
        return self._combine(other, '&')

    def __or__(self, other):
        return self._combine(other, '|')


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

    def as_sql(self, compiler, connection):
        sphinxql, params = self.as_sphinxql()
        expression = sphinxql % tuple(map(escape, params))
        return f"MATCH(%s)", [expression]

    def __rand__(self, other):
        return NotImplemented

    def __ror__(self, other):
        return NotImplemented
