import json
import re

try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest
import django
from django.db.models.sql import compiler
from django.db.models.fields.json import KeyTransform, KeyTransformExact, KeyTransformIsNull
from django.db.models.fields.json import HasAnyKeys, HasKey, HasKeys, DataContains, ContainedBy
from django.db.models.expressions import Exists
from django.db.models.lookups import Exact
from django.db.models.fields.json import compile_json_path

from django.core.exceptions import EmptyResultSet, FieldError
from django.db import DatabaseError, NotSupportedError
from django.db.models.expressions import F, OrderBy, RawSQL, Ref, Value
if django.VERSION>=(3,2):
    from django.db.models.functions.math import Random
if django.VERSION<(3,2):
    from django.db.models.expressions import Random
from django.db.models.functions import Cast

from django.db.models.sql.constants import ORDER_DIR
from django.db.models.sql.query import get_order_dir
from django.utils.hashable import make_hashable

class SQLCompiler(compiler.SQLCompiler):
    def compile(self, node, select_format=False):
        vendor_impl = getattr(node, 'as_' + self.connection.vendor, None)
        
        if vendor_impl:
            sql, params = vendor_impl(self, self.connection)
        elif isinstance(node, KeyTransform):
            sql, params = self.as_cast_type(node, self.connection)
        elif isinstance(node, KeyTransformExact):
            sql, params = node.as_sql(self, self.connection)

            # params传出可能为中文转成的unicode编码，由于数据库不默认转换，此处额外转换
            temp_params = []
            for param in params:
                if type(param) is str and re.compile(r'[\\u4e00-\\u9fa5]').search(param):
                    try:
                        temp_params.append("\"" + json.loads(param).replace("\"", "\\\"") + "\"")
                    except Exception:
                        temp_params.append(param)
                else:
                    temp_params.append(param)

            params = temp_params
        elif isinstance(node, KeyTransformIsNull):
            sql, params = HasKey(
                node.lhs.lhs,
                node.lhs.key_name,
            ).as_sql(self, self.connection, template='JSON_QUERY(%s, %%s WITH WRAPPER) IS NOT NULL')
            if not node.rhs:
                return sql, params
            lhs, lhs_params, _ = node.lhs.preprocess_lhs(self, self.connection)
            return '(NOT %s OR %s IS NULL)' % (sql, lhs), tuple(params) + tuple(lhs_params)            
        elif isinstance(node, HasAnyKeys):
            sql, params = node.as_sql(self, self.connection, template='JSON_VALUE(%s, %%s) IS NOT NULL')
        elif isinstance(node, HasKey) or isinstance(node, HasKeys):
            sql, params = node.as_sql(self, self.connection, template='JSON_QUERY(%s, %%s WITH WRAPPER) IS NOT NULL')
        elif isinstance(node, OrderBy):
            sql, params = node.as_oracle(self, self.connection)
        elif isinstance(node, Exact) and isinstance(node.lhs, Exists) and isinstance(node.rhs, Exists):
            sql, params = self.as_sql_for_Exact(node)
        elif isinstance(node, DataContains):
            sql, params = self.as_containt(node)
        else:
            sql, params = node.as_sql(self, self.connection)
            
        if select_format and not self.query.subquery:
            return node.output_field.select_format(self, sql, params)

        return sql, params

    def as_cast_type(self, node, connection):
        lhs, params, key_transforms = node.preprocess_lhs(self, connection)
        json_path = compile_json_path(key_transforms)
        return (
            "CAST(JSON_EXTRACT(%s, '%s') AS VARCHAR(32767))"
            % (lhs, json_path)
        ), tuple(params)

    def as_containt(self, node):
        lhs, lhs_params = node.process_lhs(self, self.connection)
        rhs, rhs_params = node.process_rhs(self, self.connection)
        params = tuple(lhs_params) + tuple(rhs_params)
        return 'JSON_CONTAINS(%s, %s)' % (lhs, rhs), params
    
    def as_sql_for_Exact(self, node):
        lhs_sql, params = node.process_lhs(self, self.connection)
        rhs_sql, rhs_params = node.process_rhs(self, self.connection)
        params.extend(rhs_params)
        rhs_sql = 'AND %s' % rhs_sql
        return '%s %s' % (lhs_sql, rhs_sql), params

class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def __init__(self, *args, **kwargs):
        self.return_id = False
        super(SQLInsertCompiler, self).__init__(*args, **kwargs)

    def fix_auto(self, sql, opts, fields, qn):
        if opts.auto_field is not None:
            auto_field_column = opts.auto_field.db_column or opts.auto_field.column
            columns = [f.column for f in fields]

            if auto_field_column in columns and fields or not fields and auto_field_column:
                table = qn(opts.db_table)
                sql_format = 'SET IDENTITY_INSERT %s ON WITH REPLACE NULL; %s; SET IDENTITY_INSERT %s OFF;'
                sql = sql_format % (table, sql, table)

        return sql
    
    def as_sql(self):
        result = super().as_sql()
        for sql, params in result:
            opts = self.query.get_meta()

            qn = self.connection.ops.quote_name

            sql = self.fix_auto(sql, opts, self.query.fields, qn)

        return [(sql, params),]

    def field_as_sql(self, field, val):
        """
        Take a field and a value intended to be saved on that field, and
        return placeholder SQL and accompanying params. Checks for raw values,
        expressions and fields with get_placeholder() defined in that order.

        When field is None, the value is considered raw and is used as the
        placeholder, with no corresponding parameters returned.
        """
        if field is None:
            # A field value of None means the value is raw.
            sql, params = val, []
        elif hasattr(val, 'as_sql'):
            # This is an expression, let's compile it.
            sql, params = self.compile(val)
        elif hasattr(field, 'get_placeholder'):
            # Some fields (e.g. geo fields) need special munging before
            # they can be inserted.
            sql, params = field.get_placeholder(val, self, self.connection), [val]
        else:
            # Return the common case for the placeholder
            sql, params = '?', [val]

        params = self.connection.ops.modify_insert_params(sql, params)

        return sql, params

class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass

class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass

class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
