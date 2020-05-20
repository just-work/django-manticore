from django.db.models.expressions import Combinable


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
        connector = f' {self.connector} '
        return connector.join(sphinxql), params

    def _combine(self, other, connector):
        if not (isinstance(other, self.__class__) and
                self.connector == other.connector):
            return self.__class__(*self.expressions, other, connector=connector)
        return self.__class__(*self.expressions, *other.expressions,
                              connector=connector)

    def __and__(self, other):
        return self._combine(other, '&')

    def __or__(self, other):
        return self._combine(other, '|')

    def __rand__(self, other):
        return NotImplemented

    def __ror__(self, other):
        return NotImplemented


class SphinxQLCombinable(Combinable):
    node_class = SphinxQLNode

    def __and__(self, other):
        if isinstance(other, self.__class__):
            return self.node_class(self, other)
        return NotImplemented

    def __rand__(self, other):
        return NotImplemented

    def __or__(self, other):
        if isinstance(other, self.__class__):
            return self.node_class(self, other, connector='|')
        return NotImplemented

    def __ror__(self, other):
        return NotImplemented


ESCAPE = str.maketrans({k: rf"\{k}" for k in r'''!"$'()-/<@^|~'''})


def escape(s: str):
    s = s.replace('\\', '\\\\')
    s = s.translate(ESCAPE)
    return s
