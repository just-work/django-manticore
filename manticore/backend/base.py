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
        'CharField': 'string',
        'TextField': 'string',
        'DateTimeField': 'timestamp',
        'FloatField': 'float',
    }

    SchemaEditorClass = DatabaseSchemaEditor

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.creation = ManticoreCreation(self)
        self.features = ManticoreFeatures(self)
        self.introspection = ManticoreIntrospection(self)
        self.ops = ManticoreOperations(self)
        self.validation = ManticoreValidation(self)

    @cached_property
    def mysql_server_info(self):
        # mysql uses 'SELECT VERSION()', not supported
        with self.temporary_connection():
            return self.connection.get_server_info()


class ManticoreFeatures(base.DatabaseFeatures):
    # mysql detects this querying SELECT @@SQL_AUTO_IS_NULL, not supported
    is_sql_auto_is_null_enabled = False
    # column definition NULL is not supported
    implied_column_null = True
    # FIXME
    supports_transactions = False
    # django tries to check foreign key constraints in tests
    can_rollback_ddl = False
    # select for update not supported
    has_select_for_update = False
    # savepoints not supported
    uses_savepoints = False


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

    def adapt_datetimefield_value(self, value):
        if isinstance(value, datetime):
            # closest manticore datetime equivalent is attr_timestamp
            return int(timezone.utc.normalize(value).timestamp())
        return super().adapt_datetimefield_value(value)

    def convert_datetimefield_value(self, value, expression, connection):
        if isinstance(value, int):
            # closest manticore datetime equivalent is attr_timestamp, which
            # stores datetime as unix utc timestamp
            value = datetime.utcfromtimestamp(value)
        return super().convert_datetimefield_value(value, expression, connection)

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        if tables:
            sql = []
            for table in tables:
                sql.append('%s %s;' % (
                    style.SQL_KEYWORD('TRUNCATE RTINDEX'),
                    style.SQL_FIELD(self.quote_name(table)),
                ))
            sql.extend(self.sequence_reset_by_name_sql(style, sequences))
            return sql
        else:
            return []


class ManticoreCreation(base.DatabaseCreation):

    def create_test_db(self, *args, **kwargs):
        # NOOP, test using regular manticore database.
        if self.connection.settings_dict.get('TEST_NAME'):
            # initialize connection database name
            test_name = self.connection.settings_dict['TEST_NAME']
            self.connection.close()
            self.connection.settings_dict['NAME'] = test_name
            self.connection.cursor()
            return test_name
        return self.connection.settings_dict['NAME']

    def destroy_test_db(self, *args, **kwargs):
        # NOOP, we created nothing, nothing to destroy.
        return
