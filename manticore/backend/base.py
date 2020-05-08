from datetime import datetime

from django.db.backends.base.introspection import TableInfo
from django.db.backends.mysql import base
from django.utils import timezone
from django.utils.functional import cached_property

from manticore.backend.schema import DatabaseSchemaEditor


class DatabaseWrapper(base.DatabaseWrapper):
    data_types = {
        **base.DatabaseWrapper.data_types,
        'AutoField': 'integer',
        # CharField is stored_only_field (needed for django_migrations table)
        'CharField': 'text stored',
        # TextField is stored indexed field
        'TextField': 'text indexed stored',
        'DateTimeField': 'timestamp',
        'FloatField': 'float',
    }

    SchemaEditorClass = DatabaseSchemaEditor

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.features = ManticoreFeatures(self)
        self.introspection = ManticoreIntrospection(self)
        self.validation = ManticoreValidation(self)
        self.ops = ManticoreOperations(self)

    @cached_property
    def mysql_server_info(self):
        # mysql uses 'SELECT VERSION()', not supported
        with self.temporary_connection():
            return self.connection.get_server_info()


class ManticoreFeatures(base.DatabaseFeatures):
    # mysql detects this querying SELECT @@SQL_AUTO_IS_NULL, not supported
    is_sql_auto_is_null_enabled = False
    implied_column_null = True


class ManticoreIntrospection(base.DatabaseIntrospection):

    def get_table_list(self, cursor):
        """Return a list of table and view names in the current database."""
        # mysql uses 'SHOW FULL TABLES', not supported
        cursor.execute("SHOW TABLES")
        # FIXME: вернуть здесь и дисковые индексы тоже
        # django migrations uses tables, matching 'rt' indexes
        return [TableInfo(row[0], {'rt': 't'}.get(row[1]))
                for row in cursor.fetchall()]

    def get_storage_engine(self, cursor, table_name):
        # 'SELECT * FROM information_schema.tables', not supported
        # noinspection PyProtectedMember
        return 'RT'

    def table_names(self, cursor=None, include_views=False):
        return super().table_names(cursor, include_views)


class ManticoreValidation(base.DatabaseValidation):
    def _check_sql_mode(self, **kwargs):
        # mysql uses 'SELECT @@sql_mode', not supported
        return []


class ManticoreOperations(base.DatabaseOperations):
    compiler_module = 'manticore.backend.compiler'

    def convert_datetimefield_value(self, value, expression, connection):
        # DateTime is
        if value is not None:
            if isinstance(value, int):
                value = datetime.utcfromtimestamp(value)
        return super().convert_datetimefield_value(value, expression, connection)
