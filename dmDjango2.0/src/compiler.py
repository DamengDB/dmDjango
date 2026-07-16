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

    def as_sql(self):
        result = super(SQLInsertCompiler, self).as_sql()
        from django.db.models.sql.subqueries import InsertQuery
        if isinstance(self.query, InsertQuery):
            return self.connection.features.parse_module.adjust_insert_sql(self, result)
        else:
            return result

class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass

class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass

class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
