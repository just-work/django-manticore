from django.db.backends.mysql import schema
from django.db.models.fields import NOT_PROVIDED


class DatabaseSchemaEditor(schema.DatabaseSchemaEditor):

    def prepare_default(self, value):
        raise NotImplementedError()

    def create_model(self, model):
        super().create_model(model)

    def skip_default(self, field):
        # manticore does not support defaults at all
        return True

    def add_field(self, model, field):
        base_add_field = super(schema.DatabaseSchemaEditor, self).add_field
        base_add_field(model, field)

        if (self.skip_default(field) and
                field.default not in (None, NOT_PROVIDED)):
            effective_default = self.effective_default(field)
            # UPDATE needs WHERE clause
            # noinspection SqlResolve,SqlNoDataSourceInspection,PyProtectedMember
            self.execute(
                'UPDATE %(table)s SET %(column)s = %%s WHERE `id` > %%s' % {
                    'table': self.quote_name(model._meta.db_table),
                    'column': self.quote_name(field.column),
                }, [effective_default, 0])

    def column_sql(self, model, field, include_default=False):
        if field.primary_key:
            # FIXME: созданные вручную первичные ключи нужно все-таки создавать
            return None, ''
        # Без этого создается описание колонки с NULL/NOT NULL в конце
        field.null = True
        return super().column_sql(model, field, include_default=False)

    def alter_unique_together(self, model, old_unique_together,
                              new_unique_together):
        # unique indexes not supported
        pass