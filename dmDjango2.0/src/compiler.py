import django

try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest

from django.db.models.sql import compiler
from django.core.exceptions import FieldError
from django.db.models.fields import BinaryField

class SQLCompiler(compiler.SQLCompiler):
    pass

class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def __init__(self, *args, **kwargs):
        self.return_id = False
        super(SQLInsertCompiler, self).__init__(*args, **kwargs)

    def fix_auto(self, sql, opts, fields, qn):
        if opts.auto_field is not None and fields or not fields:
            auto_field_column = opts.auto_field.db_column or opts.auto_field.column
            columns = [f.column for f in fields]

            if auto_field_column in columns and fields or not fields and auto_field_column:

                table = qn(opts.db_table)
                sql_format = 'SET IDENTITY_INSERT %s ON WITH REPLACE NULL; %s; SET IDENTITY_INSERT %s OFF;'
                id_insert_sql = sql_format % (table, sql, table)

                sql = id_insert_sql

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
