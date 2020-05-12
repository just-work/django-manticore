from datetime import datetime

from django.db.backends.base.introspection import TableInfo
from django.db.backends.mysql import base
from django.utils import timezone
from django.utils.functional import cached_property

from manticore.backend.schema import DatabaseSchemaEditor


class TableName(str):
    # table name marker used in quote_name for adding database name prefix
    # to table names
    is_table_name = True


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
    # used to return primary keys from LAST_INSERT_ID() function to bulk_create
    can_return_rows_from_bulk_insert = True


class ManticoreIntrospection(base.DatabaseIntrospection):

    def get_table_list(self, cursor):
        """Return a list of table and view names in the current database."""
        # mysql uses 'SHOW FULL TABLES', not supported
        cursor.execute("SHOW TABLES")
        result = []
        database_prefix = f'{self.connection.settings_dict["NAME"]}__'
        # django migrations uses tables, matching 'rt' indexes
        table_types = {
            'rt': 't'
        }
        for row in cursor.fetchall():
            name = row[0]
            table_type = row[1]
            if name.startswith(database_prefix):
                # removing database prefix for tables
                name = name[len(database_prefix):]
            else:
                # We don't need tables not from current db
                continue
            result.append(TableInfo(name, table_types.get(table_type)))
        # FIXME: вернуть здесь и дисковые индексы тоже
        return result

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

    @property
    def db_name(self):
        return self.connection.settings_dict.get('NAME', '')

    def quote_name(self, name):
        """ Table names are prefixed with database name."""
        if getattr(name, 'is_table_name', False):
            if self.db_name:
                name = f'{self.db_name}__{name}'
        return super().quote_name(name)

    @staticmethod
    def mark_table_name(name):
        """
        Marks table name with is_table_name flag for correct addition of
        database name prefix
        """
        return TableName(name)

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
    #
    # def create_test_db(self, *args, **kwargs):
    #     # NOOP, test using regular manticore database.
    #     if self.connection.settings_dict.get('TEST_NAME'):
    #         # initialize connection database name
    #         test_name = self.connection.settings_dict['TEST_NAME']
    #         self.connection.close()
    #         self.connection.settings_dict['NAME'] = test_name
    #         self.connection.cursor()
    #         return test_name
    #     return self.connection.settings_dict['NAME']
    #
    # def destroy_test_db(self, *args, **kwargs):
    #     # NOOP, we created nothing, nothing to destroy.
    #     return

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        # manticore does not support multiple databases
        pass

    def _destroy_test_db(self, test_database_name, verbosity):
        # manticore does not support destroying test databases, instead we
        # drop every table with corresponding prefix
        introspection: ManticoreIntrospection = self.connection.introspection
        # noinspection PyProtectedMember
        with self.connection._nodb_connection.cursor() as c:
            c.execute("SHOW TABLES")
            for row in c.fetchall():
                table_name = row[0]
                if table_name.startswith(f'{test_database_name}__'):
                    c.execute(f"DROP TABLE {table_name}")
