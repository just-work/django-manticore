from django.db.backends.mysql import compiler
from django.db.models import sql, lookups

from manticore.models import RTField, JSONField
from manticore.models.sql.compiler import SphinxQLCompiler


class SQLCompiler(SphinxQLCompiler):
    pass


class SQLInsertCompiler(compiler.SQLInsertCompiler, SphinxQLCompiler):

    def execute_sql(self, returning_fields=None):
        # noinspection PyProtectedMember
        opts = self.query.model._meta
        # marking db_table attribute to add database name prefix in quote_name
        opts.db_table = self.connection.ops.mark_table_name(opts.db_table)
        if returning_fields:
            # when performing bulk_create, it is useful to fill primary keys
            # for new inserted objects.
            if (len(returning_fields) != 1 or
                    not returning_fields[0].primary_key):
                raise NotImplementedError("returning_fields not supported")
            super().execute_sql(returning_fields=False)
            with self.connection.cursor() as c:
                c.execute("SELECT LAST_INSERT_ID()")
                row = c.fetchone()
            result = []
            pks = list(map(int, row[0].split(',')))
            if len(self.query.objs) > 1:
                # bulk_create needs something like [[1], [2], [3]],
                # but only if there are more than one objects inserted
                for pk in pks:
                    result.append([pk])
            else:
                # simple save(force_insert) needs not-a-list, see
                # QuerySet._batched_insert
                result = (pks[0],)
            return result
        return super().execute_sql(returning_fields)

    def pre_save_val(self, field, obj):
        return super().pre_save_val(field, obj)


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SphinxQLCompiler):

    def as_sql(self):
        if self.__is_index_field_update():
            pk = self.__get_primary_key_value()
            if pk is not None:
                # rt fields cant be updated with UPDATE syntax, and if we are
                # performing SearchIndex.save(), we can perform REPLACE
                return self.__as_replace(pk)
            else:
                raise NotImplementedError(
                    "Updating indexed fields is supported only by pk value"
                )
        return super().as_sql()

    def __get_primary_key_value(self):
        """
        Checks whether where clause is pk=value
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
        insert_sql = 'REPLACE' + insert_sql[6:]
        return insert_sql, params

    def __is_index_field_update(self):
        for field, _, _ in self.query.values:
            if isinstance(field, (RTField, JSONField)):
                return True
        return False


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SphinxQLCompiler):
    def _compile_in(self, node: lookups.In):
        # DELETE supports WHERE id IN (values_list)
        return node.as_sql(self, self.connection)
