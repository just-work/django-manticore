from datetime import datetime

from django.db import models
from django.db.backends.mysql import compiler
from django.utils import timezone

from manticore.models.sql.compiler import SphinxQLCompiler


class SQLCompiler(SphinxQLCompiler):
    pass


class SQLInsertCompiler(compiler.SQLInsertCompiler, SphinxQLCompiler):

    def execute_sql(self, returning_fields=None):
        return super().execute_sql(returning_fields)

    def pre_save_val(self, field, obj):
        return super().pre_save_val(field, obj)


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SphinxQLCompiler):
    pass
