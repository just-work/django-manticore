from typing import Tuple, Iterable, Any

from django.db.models.expressions import Combinable


class SphinxQLNode:
    connector = '&'

    def __init__(self, *expressions, connector='&'):
        self.expressions = list(expressions)
        self.connector = connector

    def as_sphinxql(self):
        sphinxql = []
        params = []
        for expr in self.expressions:
            s, p = expr.as_sphinxql()
            sphinxql.append(s)
            params.extend(p)
        connector = f' {self.connector} '
        if len(self.expressions) > 1:
            return f'({connector.join(sphinxql)})', params
        return connector.join(sphinxql), params

    def _combine(self, other, connector):
        if self.connector == connector:
            if isinstance(other, SphinxQLNode):
                self.expressions.extend(other.expressions)
            else:
                self.expressions.append(other)
            return self
        return self.__class__(self, other, connector=connector)

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
        if isinstance(other, SphinxQLCombinable):
            return self.node_class(self, other)
        return NotImplemented

    def __rand__(self, other):
        return NotImplemented

    def __or__(self, other):
        if isinstance(other, SphinxQLCombinable):
            return self.node_class(self, other, connector='|')
        return NotImplemented

    def __ror__(self, other):
        return NotImplemented

    def as_sphinxql(self) -> Tuple[str, Iterable[Any]]:
        raise NotImplementedError

    def __repr__(self):
        sql, params = self.as_sphinxql()
        return f'{self.__class__.__name__}: {sql % tuple(params)}'


ESCAPE = str.maketrans({k: rf"\{k}" for k in r'''!"$'()-/<@^|~'''})


def escape(s: str):
    s = s.replace('\\', '\\\\')
    s = s.translate(ESCAPE)
    return s
