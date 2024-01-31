from django.db.models.sql.where import WhereNode


class ManticoreWhereNode(WhereNode):
    # Manticore does not allow parentheses on root where clause

    contains_aggregate = False
    contains_over_clause = False

    def as_sql(self, compiler, connection):
        sql, params = super().as_sql(compiler, connection)
        # remove one parentheses level
        if sql and sql[0] == '(' and sql[-1] == ')':
            sql = sql[1:-1]
        return sql, params
