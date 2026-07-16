import datetime
import sys
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.models.deletion import CASCADE, SET_DEFAULT, SET_NULL

class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):

    sql_alter_column_type = "MODIFY %(column)s %(type)s"
    sql_alter_column_null = "MODIFY %(column)s NULL"
    sql_alter_column_not_null = "MODIFY %(column)s NOT NULL"
    sql_alter_column_default = "ALTER COLUMN %(column)s SET DEFAULT %(default)s"

    def quote_value(self, value):
        if sys.version_info.major == 2 and isinstance(value, unicode):
                value_return = value.encode("utf-8")
                return "'%s'" % value_return.replace("'", "''").replace("%", "%%")
        if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
            return "'%s'" % value
        elif isinstance(value, str):
            return "'%s'" % value.replace("'", "''").replace('%', '%%')
        elif isinstance(value, (bytes, bytearray, memoryview)):
            return "'%s'" % value.hex()
        elif isinstance(value, bool):
            return "1" if value else "0"
        else:
            return str(value) 
            
    def prepare_default(self, value):
        return self.quote_value(value)
