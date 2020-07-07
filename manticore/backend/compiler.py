from django.db.backends.mysql import compiler
from django.db.models import sql, lookups

from manticore.models import RTField, JSONField
from manticore.models.sql.compiler import SphinxQLCompiler


class SQLCompiler(SphinxQLCompiler):
    """ interface for DatabaseOperations.compiler_module """
    pass


class SQLInsertCompiler(compiler.SQLInsertCompiler, SphinxQLCompiler):

    def execute_sql(self, returning_fields=None):
        """
        Marks table name for database name prefix addition.
        """
        # noinspection PyProtectedMember
        opts = self.query.model._meta
        # marking db_table attribute to add database name prefix in quote_name
        opts.db_table = self.connection.ops.mark_table_name(opts.db_table)
        return super().execute_sql(returning_fields)


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SphinxQLCompiler):

    def as_sql(self):
        """
        Performs REPLACE query instead of UPDATE in case of
        rt_field and attr_json updates.
        """
        if self.__is_index_field_update():
            pk = self.__get_primary_key_value()
            if pk is None:
                raise NotImplementedError(
                    "Updating indexed fields is supported only by pk value")
            # rt fields and attr_json can't be updated with UPDATE syntax;
            # if we are performing SearchIndex.save(), we can perform REPLACE
            return self.__as_replace(pk)

        return super().as_sql()

    def __get_primary_key_value(self):
        """
        :returns: pk value from WHERE clause which looks like "WHERE id = %s"
        """
        if not self.query.where:
            return
        if len(self.query.where.children) != 1:
            return
        node = self.query.where.children[0]
        if node.lookup_name != 'exact':
            return
        if not node.lhs.field.primary_key:
            return
        return node.rhs

    def __as_replace(self, pk):
        """
        Executes REPLACE query to update rt_field and attr_json fields by
        given pk value.
        """
        query = sql.InsertQuery(self.query.model)
        # noinspection PyProtectedMember
        opts = self.query.model._meta
        fields = [opts.pk]
        obj = self.query.model()
        setattr(obj, 'pk', pk)
        for field, _, value in self.query.values:
            fields.append(field)
            setattr(obj, field.attname, value)
        if set(fields) ^ set(opts.local_fields):
            # If you call REPLACE omitting some attributes, manticore will
            # store default empty values (0, '', [] etc...)
            raise NotImplementedError(
                "REPLACE query from save() with update_fields not supported")
        query.insert_values(fields, [obj], raw=False)
        insert_compiler = query.get_compiler(self.using, self.connection)
        sqls = insert_compiler.as_sql()
        insert_sql, params = sqls[0]

        # INSERT -> REPLACE
        if not insert_sql.startswith('INSERT '):
            raise ValueError(insert_sql)
        insert_sql = 'REPLACE' + insert_sql[6:]

        return insert_sql, params

    def __is_index_field_update(self):
        """ Checks whether update query contains indexed fields updates."""
        for field, _, _ in self.query.values:
            if isinstance(field, (RTField, JSONField)):
                return True
        return False


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SphinxQLCompiler):
    def _compile_in(self, node: lookups.In):
        """
        Formats "a IN values" expression instead of widely used in manticore
        "IN(a, values)" function
        """
        # DELETE supports WHERE id IN (values_list) instead of IN-function
        return node.as_sql(self, self.connection)
