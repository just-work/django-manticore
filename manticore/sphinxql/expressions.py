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

    def __init__(self, term: str, negate=False):
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

    def __invert__(self):
        return self.__class__(self.term, negate=not self.negate)

    def as_sphinxql(self):
        sql = '!(%s)' if self.negate else '(%s)'
        return sql, [self.term]


class P(T):
    """
    SphinxQL quoted text node (phrase).
    >>> P("phrase search")
    P: ("phrase search")
    >>> ~P("exclude phrase")
    P: !("exclude phrase")
    """

    def as_sphinxql(self):
        sql = '!("%s")' if self.negate else '("%s")'
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
