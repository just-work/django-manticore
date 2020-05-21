from typing import Optional, Union

from manticore.sphinxql.base import SphinxQLCombinable, SphinxQLNode, escape

__all__ = [
    'F',
    'Match',
    'P',
    'T',
]


class TextNode(SphinxQLNode):
    """ Node allowing operations with text terms."""


class T(SphinxQLCombinable):
    """ SphinxQL text term."""
    node_class = TextNode

    def __init__(self, term: str, negate=False, exact=False):
        """
        Initializes search term node:

        >>> T("any of word matches")
        T: (any of word matches)
        >>> T("negated", negate=True)
        T: !(negated)


        """
        if not isinstance(term, str):
            raise TypeError("term is not string")
        self.term = term
        self.negate = negate
        self.exact = exact

    def __invert__(self):
        return self.__class__(self.term, negate=not self.negate,
                              exact=self.exact)

    def as_sphinxql(self):
        e = '=' if self.exact else ''
        sql = f'!({e}%s)' if self.negate else f'({e}%s)'
        return sql, [self.term]


class P(T):
    """
    SphinxQL quoted text node (phrase).
    >>> P("phrase search")
    P: ("phrase search")
    >>> ~P("exclude phrase")
    P: !("exclude phrase")
    >>> P("2 of m words match", quorum=2)
    P: ("2 of m words match"/2)
    >>> P("~30% of words match", quorum=0.3)
    P: ("~30% of words match"/0.3)
    >>> P("distance between words", proximity=4)
    P: ("distance between words"~4)
    >>> ~P("exact word forms", exact=True)
    P: !(="exact word forms")
    """

    def __init__(self, term: str, negate=False, exact=False,
                 proximity: Optional[int] = None,
                 quorum: Union[None, int, float] = None):
        super().__init__(term, negate, exact)
        if proximity is not None:
            if not isinstance(proximity, int):
                raise TypeError("proximity must be int")
        self.proximity = proximity
        if quorum is not None:
            if not isinstance(quorum, (int, float)):
                raise TypeError("quorum must be int or float")
        self.quorum = quorum

    def __invert__(self):
        return self.__class__(self.term, negate=not self.negate,
                              exact=self.exact, proximity=self.proximity,
                              quorum=self.quorum)

    def as_sphinxql(self):
        if isinstance(self.quorum, int):
            m = f'/{self.quorum}'
        elif isinstance(self.quorum, float):
            m = f'/{self.quorum:0.7f}'.rstrip('0')
        elif self.proximity:
            m = f'~{self.proximity}'
        else:
            m = ''
        p = '=' if self.exact else ''
        sql = f'!({p}"%s"{m})' if self.negate else f'({p}"%s"{m})'
        return sql, [self.term]


class F(SphinxQLCombinable):
    """
    Field search operator

    Supports:
    - @field text -

    """

    def __init__(self, *args, exclude=False, **kwargs):
        """
        Constructs field search expression
        :param args: field names and search expression (last args item)
        :param exclude: exclude fields from search
        :param kwargs: shortcut for single field name -> search expression value

        >>> F(first_field="text")
        F: (@first_field (text))
        >>> F('first_field', 'other_field', T("term"))
        F: (@(first_field,other_field) (term))
        >>> F(other_field=(T("text") & ~T("other")), exclude=True)
        F: (@!other_field (text) & (other))
        """
        if args and kwargs:
            raise ValueError("Don't pass args and kwargs simultaneously")

        if args:
            if len(args) < 2:
                raise ValueError(
                    "pass at least one field name and single search expression")
            *field_names, expression = args
        elif kwargs:
            if len(kwargs) > 1:
                raise ValueError("Only single value in kwargs is supported")
            tup = kwargs.items().__iter__().__next__()
            field_names = (tup[0],)
            expression = tup[1]
        else:
            raise ValueError("Pass args or kwargs")

        self.fields = field_names
        if isinstance(expression, str):
            self.expression = T(expression)
        elif isinstance(expression, T):
            self.expression = expression
        elif isinstance(expression, TextNode):
            self.expression = expression
        else:
            raise TypeError("unsupported expression for F")
        self.exclude = exclude

    def as_sphinxql(self):
        if len(self.fields) == 1:
            fields = self.fields[0]
        else:
            fields = f"({','.join(self.fields)})"
        prefix = '@!' if self.exclude else '@'

        expr, params = self.expression.as_sphinxql()
        return f'({prefix}{fields} {expr})', params


class Match(SphinxQLNode):
    """ Root match node that renders MATCH(...) """
    # duck typing for Django ORM
    contains_aggregate = False

    # noinspection PyUnusedLocal
    def as_sql(self, compiler, connection):
        sphinxql, params = self.as_sphinxql()
        expression = sphinxql % tuple(map(escape, params))
        return f"MATCH(%s)", [expression]
