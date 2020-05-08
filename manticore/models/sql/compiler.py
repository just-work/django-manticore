from django.db.backends.mysql import compiler
from django.db.models import expressions
from django.db.models.sql import constants


class SphinxQLCompiler(compiler.SQLCompiler):

    def compile(self, node):
        if isinstance(node, expressions.Col):
            # `table_name`.`column_name` is not supported
            return self.__compile_col(node)
        return super().compile(node)

    def __compile_col(self, node: expressions.Col):
        qn = self.quote_name_unless_alias
        return qn(node.target.column), ()

    def execute_sql(self, result_type=constants.MULTI, chunked_fetch=False,
                    chunk_size=constants.GET_ITERATOR_CHUNK_SIZE):
        return super().execute_sql(result_type, chunked_fetch, chunk_size)

    def as_sql(self, with_limits=True, with_col_aliases=False):
        if with_limits and not self.query.low_mark:
            # by default 20 items are returned, setting to max value
            self.query.set_limits(high=2 ** 31 - 1)
        return super().as_sql(with_limits, with_col_aliases)
