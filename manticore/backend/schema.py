from django.db.backends.mysql import schema


class DatabaseSchemaEditor(schema.DatabaseSchemaEditor):

    def prepare_default(self, value):
        raise NotImplementedError()

    def create_model(self, model):
        super().create_model(model)

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




