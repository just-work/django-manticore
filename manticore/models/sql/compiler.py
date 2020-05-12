from django.db import models
from django.db.backends.mysql import compiler
from django.db.models import expressions, lookups
from django.db.models.query import EmptyResultSet
from django.db.models.sql import constants
from django.db.models.sql.where import WhereNode, ExtraWhere, AND

from manticore.models.lookups import InFunction
from manticore.models.sql.sphinxql import Match
from manticore.models.sql.where import ManticoreWhereNode


class SphinxQLCompiler(compiler.SQLCompiler):

    def compile(self, node):
        if isinstance(node, expressions.Col):
            # `table_name`.`column_name` is not supported
            return self.__compile_col(node)
        if isinstance(node, lookups.In):
            # col IN (values list) not supported, transforming to function call
            return self._compile_in(node)
        return super().compile(node)

    def execute_sql(self, result_type=constants.MULTI, chunked_fetch=False,
                    chunk_size=constants.GET_ITERATOR_CHUNK_SIZE):
        return super().execute_sql(result_type, chunked_fetch, chunk_size)

    def as_sql(self, with_limits=True, with_col_aliases=False):
        self.__maybe_set_limits(with_limits)
        try:
            self.__maybe_move_where()
        except EmptyResultSet:
            # Where node has compiled to always false condition, but we need
            # to call super().as_sql again for some django internal side effects
            pass

        return super().as_sql(with_limits, with_col_aliases)

    def __compile_col(self, node: expressions.Col):
        qn = self.quote_name_unless_alias
        return qn(node.target.column), ()

    def __maybe_move_where(self):
        where = self.query.where.clone()
        match = None
        for node in where.children:
            if isinstance(node, Match):
                match = node
                break
        if match:
            where.children.remove(match)
        if not where:
            return
        sql, params = self.compile(where)
        extra_select = expressions.RawSQL(sql, params, models.BooleanField())
        extra_where = ExtraWhere(['__where__ = %s'], (True,))
        where = ManticoreWhereNode()
        where.add(extra_where, AND)
        if match:
            where.add(match, AND)

        # All filtering conditions are now evaluated as __where__ in select
        # clause, so we need to check only that it is true
        self.query.add_annotation(extra_select, '__where__')
        self.query.where = where

    def __maybe_set_limits(self, with_limits):
        if with_limits and not self.query.low_mark:
            # by default 20 items are returned, setting to max value
            self.query.set_limits(high=2 ** 31 - 1)

    def _compile_in(self, node: lookups.In):
        lookup = InFunction(node.lhs, node.rhs)
        return lookup.as_sql(self, self.connection)
