from django.core.exceptions import EmptyResultSet
from django.db.models import lookups
from django.utils.datastructures import OrderedSet


class MultiExact(lookups.Exact):

    def as_sql(self, compiler, connection):
        return InFunction(self.lhs, [self.rhs]).as_sql(compiler, connection)


class InFunction(lookups.In):

    def as_sql(self, compiler, connection):
        lhs_sql, params = self.process_lhs(compiler, connection)
        rhs_sql, rhs_params = self.process_rhs(compiler, connection)
        params.extend(rhs_params)
        return 'IN(%s, %s)' % (lhs_sql, rhs_sql), params

    def process_rhs(self, compiler, connection):
        if self.rhs_is_direct_value():
            try:
                rhs = OrderedSet(self.rhs)
            except TypeError:  # Unhashable items in self.rhs
                rhs = self.rhs

            if not rhs:
                raise EmptyResultSet

            # rhs should be an iterable; use batch_process_rhs() to
            # prepare/transform those values.
            sqls, sqls_params = self.batch_process_rhs(
                compiler, connection, rhs)
            placeholder = ', '.join(sqls)
            return placeholder, sqls_params
        else:
            # subqueries not supported
            raise NotImplementedError()
