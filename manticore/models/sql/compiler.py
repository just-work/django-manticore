from django.core.exceptions import EmptyResultSet
from django.db import models
from django.db.backends.mysql import compiler
from django.db.models import expressions, lookups
from django.db.models.sql.datastructures import BaseTable
from django.db.models.sql.where import ExtraWhere, AND

from manticore.models.fields import RTField
from manticore.models.lookups import InFunction
from manticore.models.sql.where import ManticoreWhereNode
from manticore.sphinxql.expressions import Match


class SphinxQLCompiler(compiler.SQLCompiler):

    def compile(self, node):
        if isinstance(node, expressions.Col):
            # `table_name`.`column_name` is not supported
            return self.__compile_col(node)
        if isinstance(node, lookups.In):
            # col IN (values list) not supported, transforming to function call
            return self._compile_in(node)
        if isinstance(node, BaseTable):
            # prefix table name with database name
            return self.__compile_table(node)
        return super().compile(node)

    def as_sql(self, with_limits=True, with_col_aliases=False):
        self.__maybe_set_limits(with_limits)
        try:
            self.__maybe_move_where()
        except EmptyResultSet:
            # Where node has compiled to always false condition, but we need
            # to call super().as_sql again for some django internal side effects
            pass

        sql, params = super().as_sql(with_limits, with_col_aliases)
        if getattr(self.query, 'options', False):
            options, options_params = [], []
            for k, v in self.query.options.items():
                if hasattr(v, 'as_sql'):
                    v_sql, v_params = v.as_sql(self, self.connection)
                    options.append(f'{k} = {v_sql}')
                    options_params.extend(v_params)
                elif isinstance(v, dict) and k == 'field_weights':
                    dict_option, dict_params = self._compile_dict(v)
                    options.append(f'{k} = ({dict_option})')
                    options_params.extend(dict_params)
                else:
                    options.append(f'{k} = %s')
                    options_params.append(v)
            options_sql = ', '.join(options)
            sql = f'{sql} OPTION {options_sql}'
            params += tuple(options_params)
        return sql, params

    def __compile_col(self, node: expressions.Col):
        qn = self.quote_name_unless_alias
        return qn(node.target.column), ()

    def __compile_table(self, node: BaseTable):
        qn = self.connection.ops.quote_name
        table_name = self.connection.ops.mark_table_name(node.table_name)
        return qn(table_name), ()

    def __maybe_move_where(self):
        where = self.query.where.clone()
        match = None
        for node in where.children:
            if isinstance(node, Match):
                match = node
                break
        if match:
            where.children.remove(match)
        if not where:
            return
        sql, params = self.compile(where)
        extra_select = expressions.RawSQL(sql, params, models.BooleanField())
        extra_where = ExtraWhere(['__where__ = %s'], (True,))
        where = ManticoreWhereNode()
        where.add(extra_where, AND)
        if match:
            where.add(match, AND)

        # All filtering conditions are now evaluated as __where__ in select
        # clause, so we need to check only that it is true
        self.query.add_annotation(extra_select, '__where__')
        self.query.where = where

    def __maybe_set_limits(self, with_limits):
        if with_limits and not self.query.low_mark:
            # by default 20 items are returned, setting to max value
            self.query.set_limits(high=2 ** 31 - 1)

    def _compile_in(self, node: lookups.In):
        lookup = InFunction(node.lhs, node.rhs)
        return lookup.as_sql(self, self.connection)

    def _compile_dict(self, param_dict: dict):
        self._check_field_models(param_dict.keys())
        qn = self.connection.ops.quote_name
        params_str = ', '.join([f'{qn(k)}=%s' for k in param_dict])
        params_list = param_dict.values()
        return params_str, params_list

    def _check_field_models(self, fields):
        """ Сhecks that the field is in the model """
        index_fields = self.query.model._meta.get_fields()
        index_field_dict = {f.name: f for f in index_fields}
        for field in fields:
            if field not in index_field_dict:
                raise ValueError(f'Model not consist field: [{field}]')

            if not isinstance(index_field_dict[field], RTField):
                raise ValueError(
                    f'Fields specified in field_weights '
                    f'option not found : [{field}]'
                )
