from django.db import ProgrammingError
from django.db.backends.mysql import schema
from django.db.models.fields import NOT_PROVIDED
from django.db.models.options import Options

from manticore.models import fields, base


class DatabaseSchemaEditor(schema.DatabaseSchemaEditor):

    def prepare_default(self, value):
        raise NotImplementedError()

    def create_model(self, model):
        """
        Marks table name for database prefix addition, adds stub rt_field for
        django internal tables,
        """
        # noinspection PyProtectedMember
        opts: Options = model._meta
        # mark table name to set database name prefix for created tables
        opts.db_table = self.connection.ops.mark_table_name(opts.db_table)
        opts.db_table.skip_cluster = True
        # manticore search does not allow creating tables without rt fields,
        # adding a stub field for it
        has_rt_index = False
        for f in opts.local_fields:
            if isinstance(f, fields.RTField):
                has_rt_index = True
                break
        if not has_rt_index:
            stub = fields.IndexedField(db_column='__stub__')
            try:
                stub.contribute_to_class(model, '_stub')
                super().create_model(model)
            finally:
                # removing stub to prevent fetching non-stored field
                opts.local_fields.remove(stub)
                opts.__dict__.pop('fields', None)
        else:
            super().create_model(model)
        cluster = self.connection.settings_dict.get('CLUSTER', '')
        if cluster:
            with self.connection._nodb_cursor() as c:
                cluster = self.connection.ops.quote_name(cluster)
                db_table = self.connection.ops.quote_name(opts.db_table)
                try:
                    c.execute(f'CREATE CLUSTER {cluster}')
                except ProgrammingError:
                    pass
                c.execute(f'ALTER CLUSTER {cluster} ADD {db_table}')
        opts.db_table.skip_cluster = False

    def skip_default(self, field):
        # manticore does not support defaults at all
        return True

    def add_field(self, model, field):
        """
        Adds new column to table and performs update of default value
        """
        # calling BaseDatabaseSchemaEditor.add_field to skip mysql
        # implementation
        # noinspection PyUnresolvedReferences
        super(schema.DatabaseSchemaEditor, self).add_field(model, field)

        if (self.skip_default(field) and
                field.default not in (None, NOT_PROVIDED)):
            effective_default = self.effective_default(field)

            if isinstance(field, fields.RTField):
                if effective_default != '':
                    # RTField update is not supported by manticore, and there
                    # is no data to preform REPLACE query (INSERT basically).
                    raise ValueError("RTField default must be ''")
                else:
                    # empty string is default by default, no update is necessary
                    return

            # UPDATE needs WHERE clause
            # noinspection SqlNoDataSourceInspection,PyProtectedMember
            self.execute(
                'UPDATE %(table)s SET %(column)s = %%s WHERE `id` > %%s' % {
                    'table': self.quote_name(model._meta.db_table),
                    'column': self.quote_name(field.column),
                }, [effective_default, 0])

    def column_sql(self, model, field, include_default=False):
        if field.primary_key:
            # FIXME: create manual primary keys
            return None, ''
        # without null flag column sql is created with NOT NULL annotation,
        # which is not supported by manticore
        null, field.null = field.null, True
        try:
            return super().column_sql(model, field, include_default=False)
        finally:
            field.null = null

    def alter_unique_together(self, model, old_unique_together,
                              new_unique_together):
        # unique indexes not supported
        pass

    def table_sql(self, model):
        sql, params = super().table_sql(model)
        # noinspection PyProtectedMember
        opts = model._meta

        index_sql, index_params = [], []
        for k in base.INDEX_OPTIONS:
            try:
                v = getattr(opts, k)
            except AttributeError:
                continue
            index_sql.append(f'{k} = %s')
            index_params.append(str(v))
        if index_sql:
            sql += ' '.join(index_sql)
            params += index_params

        return sql, params
