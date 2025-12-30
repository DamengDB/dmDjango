import datetime

from django.utils.encoding import force_bytes

from .base import Database

convert_unicode = force_bytes

class InsertIdVar(object):
    """
    A late-binding cursor variable that can be passed to Cursor.execute
    as a parameter, in order to receive the id of the row created by an
    insert statement.
    """

    def bind_parameter(self, cursor):
        param = cursor.cursor.var(Database.NUMBER)
        cursor._insert_id_var = param
        return param

class InsertVar:
    types = {
        'AutoField': int,
        'BigAutoField': int,
        'SmallAutoField': int,
        'IntegerField': int,
        'BigIntegerField': int,
        'SmallIntegerField': int,
        'PositiveBigIntegerField': int,
        'PositiveSmallIntegerField': int,
        'PositiveIntegerField': int,
        'FloatField': Database.DOUBLE,
        'DateTimeField': Database.TIMESTAMP,
        'DateField': Database.DATE,
        'DecimalField': Database.NUMBER,
    }

    def __init__(self, field):
        internal_type = getattr(field, 'target_field', field).get_internal_type()
        self.db_type = self.types.get(internal_type, str)
        self.bound_param = None

    def bind_parameter(self, cursor):
        self.bound_param = cursor.cursor.var(self.db_type)
        return self.bound_param

    def get_value(self):
        return self.bound_param.getvalue()