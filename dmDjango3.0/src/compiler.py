import json
import re

try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest
import django
from django.db.models.sql import compiler
from django.db.models.fields.json import KeyTransform, KeyTransformExact, KeyTransformIsNull, JSONField
from django.db.models.fields.json import HasAnyKeys, HasKey, HasKeys, DataContains, ContainedBy
from django.db.models.expressions import Exists
from django.db.models.lookups import Exact

from django.core.exceptions import EmptyResultSet, FieldError
from django.db import DatabaseError, NotSupportedError
from django.db.models.expressions import F, OrderBy, RawSQL, Ref, Value
if django.VERSION >= (3, 2):
    from django.db.models.functions.math import Random
if django.VERSION < (3, 2):
    from django.db.models.expressions import Random
from django.db.models.functions import Cast

from django.db.models.sql.constants import ORDER_DIR
from django.db.models.sql.query import get_order_dir
from django.utils.hashable import make_hashable
from .extension import ADAPT_JSON_VALUE

def compile_json_path(key_transforms, include_root=True):
    path = ['$'] if include_root else []
    for key_transform in key_transforms:
        try:
            num = int(key_transform)
        except ValueError:  # non-integer
            path.append('.')
            path.append(json.dumps(key_transform.replace("'", "''")))
        else:
            path.append('[%s]' % num)
    return ''.join(path)

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
            if django.VERSION > (5, 1, 3):
                template = 'JSON_QUERY(%s, %s WITH WRAPPER) IS NOT NULL'
            else:
                template = 'JSON_QUERY(%s, %%s WITH WRAPPER) IS NOT NULL'
            sql, params = HasKey(
                node.lhs.lhs,
                node.lhs.key_name,
            ).as_sql(self, self.connection, template=template)
            if not node.rhs:
                return sql, params
            lhs, lhs_params, _ = node.lhs.preprocess_lhs(self, self.connection)
            return '(NOT %s OR %s IS NULL)' % (sql, lhs), tuple(params) + tuple(lhs_params)            
        elif isinstance(node, HasAnyKeys):
            if django.VERSION > (5, 1, 3):
                template = 'JSON_VALUE(%s, %s) IS NOT NULL'
            else:
                template = 'JSON_VALUE(%s, %%s) IS NOT NULL'
            sql, params = node.as_sql(self, self.connection, template=template)
        elif isinstance(node, HasKey) or isinstance(node, HasKeys):
            if django.VERSION > (5, 1, 3):
                template = 'JSON_QUERY(%s, %s WITH WRAPPER) IS NOT NULL'
            else:
                template = 'JSON_QUERY(%s, %%s WITH WRAPPER) IS NOT NULL'
            sql, params = node.as_sql(self, self.connection, template=template)
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
        params.extend(params)
        sql = ('CASE WHEN %s AND %s THEN TRUE WHEN NOT %s AND NOT %s THEN TRUE ELSE FALSE END' %
               (lhs_sql, rhs_sql, lhs_sql, rhs_sql))
        return sql, params

if not ADAPT_JSON_VALUE:
    class DMJSONField(JSONField):

        def get_prep_value(self, value):
            if value is None:
                return value
            return json.dumps(value, ensure_ascii=False, cls=self.encoder)

class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def __init__(self, *args, **kwargs):
        self.return_id = False
        super(SQLInsertCompiler, self).__init__(*args, **kwargs)
    
    def as_sql(self):
        from django.db.models.sql.subqueries import InsertQuery
        if isinstance(self.query, InsertQuery):
            if not ADAPT_JSON_VALUE:
                opts = self.query.get_meta()
                fields = self.query.fields or [opts.pk]
                for i, field in enumerate(fields):
                    if isinstance(field, JSONField):
                        temp_field = DMJSONField()
                        temp_field.__dict__.update(field.__dict__)
                        self.query.fields[i] = temp_field
            result = super().as_sql()
            return self.connection.features.parse_module.adjust_insert_sql(self, result)
        else:
            result = super().as_sql()
            return result

class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass

class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass

class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
