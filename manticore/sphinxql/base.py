"""
This module contains base classes for SphinxQL domain language.
"""

from typing import Tuple, Iterable, Any


class SphinxQLNode:
    """
    SphinxQL non-leaf node.

    SphinxQLNode implements a node that combines child nodes with same connector.
    """
    AND = "&"
    OR = "|"
    MAYBE = "MAYBE"  # Lazy OR

    connector = AND

    def __init__(self, *expressions, connector=AND):
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

    def _combine(self, other, connector, reverse):
        if not hasattr(other, 'as_sphinxql'):
            # Graph can contain only nodes that render SphinxQL expressions.
            raise TypeError(other)

        if self.connector == connector == other.connector:
            # With same connector we can use same predescence level.

            # Unpacking "other hand" expressions to list from SphinxQLNode
            expressions = list(getattr(other, 'expressions', [other]))
            if reverse:
                expressions.extend(self.expressions)
            else:
                expressions = self.expressions + expressions
            return self.__class__(*expressions, connector=connector)
        # Connector differs, add new level of predescence to graph.
        if reverse:
            return self.__class__(other, self, connector=connector)
        return self.__class__(self, other, connector=connector)

    def __and__(self, other):
        return self._combine(other, self.AND, False)

    def __or__(self, other):
        return self._combine(other, self.OR, False)

    def __rand__(self, other):
        return self._combine(other, self.AND, True)

    def __ror__(self, other):
        return self._combine(other, self.OR, True)


class SphinxQLCombinable:
    """
    Leaf node class for SphinxQL graph.

    SphinxQLCombinable is a base class for terms and unary operators in SphinxQL.

    It does not provide __init__ signature to allow any signature for child classes.
    """
    node_class = SphinxQLNode
    connector = node_class.AND  # For compatibility with SphinxQLNode

    def __and__(self, other):
        return self._connect(other, self.node_class.AND, False)

    def __rand__(self, other):
        return self._connect(other, self.node_class.AND, True)

    def __or__(self, other):
        return self._connect(other, self.node_class.OR, False)

    def __ror__(self, other):
        return self._connect(other, self.node_class.OR, True)

    def _connect(self, other, connector, reverse):
        parts = [other, self] if reverse else [self, other]
        return self.node_class(*parts, connector=connector)

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
