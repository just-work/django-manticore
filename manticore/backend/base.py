from datetime import datetime

from django.db.backends.base.introspection import TableInfo
from django.db.backends.mysql import base
from django.utils import timezone
from django.utils.functional import cached_property

from manticore.backend.schema import DatabaseSchemaEditor


class TableName(str):
    """
    Table name marker for proper database name prefix addition.
    """
    # table name marker used in quote_name for adding database name prefix
    # to table names
    is_table_name = True


class DatabaseWrapper(base.DatabaseWrapper):
    data_types = {
        **base.DatabaseWrapper.data_types,
        # overriding model fields mapping to manticore attributes
        'CharField': 'string',  # attr_string instead of varchar
        'TextField': 'string',  # attr_string instead of text, which is rt_field
        # closest equivalent for datetime field in manticore is attr_timestamp
        'DateTimeField': 'timestamp',
        'FloatField': 'float',  # attr_float instead of double
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
    # FIXME add fake transaction support
    # manticore supports over-isolated transaction, which means that updates
    # are not visible in same transaction. Transaction support is disabled
    # mostly for running tests.
    supports_transactions = False
    # django tries to check foreign key constraints in tests
    can_rollback_ddl = False
    # select for update not supported
    has_select_for_update = False
    # savepoints not supported
    uses_savepoints = False
    # enables returning primary keys from LAST_INSERT_ID() in bulk_create
    can_return_rows_from_bulk_insert = True


class ManticoreIntrospection(base.DatabaseIntrospection):

    def get_table_list(self, cursor):
        """Return a list of table and view names in the current database."""
        # mysql uses 'SHOW FULL TABLES', not supported
        cursor.execute("SHOW TABLES")
        result = []
        database_prefix = f'{self.connection.settings_dict["NAME"]}__'
        # manticore annotates real-time indices with "rt" type
        table_types = {
            'rt': 't'
        }
        for row in cursor.fetchall():
            name = row[0]
            table_type = row[1]
            # multi-database support is implemented with table prefixes,
            # that should be removed when collecting back table information
            if name.startswith(database_prefix):
                # removing database prefix for tables
                name = name[len(database_prefix):]
            else:
                # We don't need tables not from current db
                continue
            result.append(TableInfo(name, table_types.get(table_type)))
        # FIXME: support on-disk indices as views
        return result

    def get_storage_engine(self, cursor, table_name):
        # 'SELECT * FROM information_schema.tables', not supported
        # noinspection PyProtectedMember
        return 'RT'

    def table_names(self, cursor=None, include_views=False):
        # FIXME: debug hook
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
        # Multi-database support is implemented with database name prefixes
        # for tables, but this method is used also for quoting other identifiers
        # like database name or column names. To distinguish table names
        # `mark_table_name` method is used to add table name mark for `name`
        # argument.
        if getattr(name, 'is_table_name', False) and self.db_name:
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
        """ Converts datetime value to unix timestamp."""
        if isinstance(value, datetime):
            # closest manticore datetime equivalent is attr_timestamp
            return int(timezone.utc.normalize(value).timestamp())
        return super().adapt_datetimefield_value(value)

    def convert_datetimefield_value(self, value, expression, connection):
        """ Convert unix timestamp values to datetime."""
        if isinstance(value, int):
            # closest manticore datetime equivalent is attr_timestamp, which
            # stores datetime as unix utc timestamp
            value = datetime.utcfromtimestamp(value)
        return super().convert_datetimefield_value(
            value, expression, connection)

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        """ Implement flushing manticore database as truncating all tables."""
        if tables:
            sql = []
            for table in tables:
                table = self.mark_table_name(table)
                sql.append('%s %s;' % (
                    style.SQL_KEYWORD('TRUNCATE RTINDEX'),
                    style.SQL_FIELD(self.quote_name(table)),
                ))
            sql.extend(self.sequence_reset_by_name_sql(style, sequences))
            return sql
        else:
            return []


class ManticoreCreation(base.DatabaseCreation):

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        # manticore does not support multiple databases, skipping
        # CREATE DATABASE command
        pass

    def _destroy_test_db(self, test_database_name, verbosity):
        # manticore does not support multiple databases, skipping
        # DROP DATABASE command.
        # noinspection PyProtectedMember
        with self.connection._nodb_connection.cursor() as c:
            # manticore does not support destroying databases, instead we
            # drop every table with corresponding prefix
            c.execute("SHOW TABLES")
            for row in c.fetchall():
                table_name = row[0]
                if table_name.startswith(f'{test_database_name}__'):
                    c.execute(f"DROP TABLE {table_name}")
