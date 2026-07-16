from __future__ import unicode_literals

import datetime
import re
import uuid
import json
from functools import lru_cache
from .extension import VERSION
from django.conf import settings
from django.utils import timezone
from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils.functional import cached_property
from django.utils.encoding import force_bytes, force_str

from .base import Database
from .utils import InsertIdVar, convert_unicode, InsertVar

from django.db.models import AutoField, Exists, ExpressionWrapper
from django.db.backends.utils import strip_quotes, truncate_name

from django.db.models.sql.where import WhereNode
from django.db.models.expressions import RawSQL
from decimal import Decimal


class DatabaseOperations(BaseDatabaseOperations):
    """
    This class encapsulates all backend-specific differences, such as the way
    a backend performs ordering or calculates the ID of a recently-inserted
    row.
    """
    
    compiler_module = "dmDjango.compiler"
    
    # CharField data type if the max_length argument isn't provided.
    cast_char_field_without_max_length = 'varchar'
    
    # Prefix for EXPLAIN queries, or None EXPLAIN isn't supported.
    explain_prefix = 'EXPLAIN'
    
    # Mapping of Field.get_internal_type() (typically the model field's class
    # name) to the data type to use for the Cast() function, if different from
    # DatabaseWrapper.data_types.    
    cast_data_types = {
        'AutoField': 'integer',
        'SmallAutoField': 'integer',
        'BigAutoField': 'bigint',
        'TimeField': 'TIME(6)',
    }
    
    # DAMENG stores positive fields as UNSIGNED ints.
    integer_field_ranges = dict(BaseDatabaseOperations.integer_field_ranges,)
    
    _sequence_reset_sql = """
DECLARE
    table_value integer;
    seq_value integer;
    seq_name user_tab_identity_cols.sequence_name%%TYPE;
BEGIN
    BEGIN
        SELECT sequence_name INTO seq_name FROM user_tab_identity_cols
        WHERE  table_name = '%(table_name)s' AND
               column_name = '%(column_name)s';
        EXCEPTION WHEN NO_DATA_FOUND THEN
            seq_name := '%(no_autofield_sequence_name)s';
    END;

    SELECT NVL(MAX(%(column)s), 0) INTO table_value FROM %(table)s;
    SELECT NVL(last_number - cache_size, 0) INTO seq_value FROM user_sequences
           WHERE sequence_name = seq_name;
    WHILE table_value > seq_value LOOP
        EXECUTE IMMEDIATE 'SELECT "'||seq_name||'".nextval FROM DUAL'
        INTO seq_value;
    END LOOP;
END;
"""    

    def cache_key_culling_sql(self):
        """
        Return an SQL query that retrieves the first cache key greater than the
        n smallest.

        This is used by the 'db' cache backend to determine where to start
        culling.
        """
        return 'SELECT ' + self.quote_name("cache_key") + ' FROM %s ORDER BY ' + self.quote_name("cache_key") + ' LIMIT 1 OFFSET %%s'

    if VERSION >= (4, 1):
        from django.utils.regex_helper import _lazy_re_compile

        _extract_format_re = _lazy_re_compile(r"[A-Z_]+")

        def date_extract_sql(self, lookup_type, sql, params):
            extract_sql = f"TO_CHAR({sql}, %s)"
            extract_param = None
            if lookup_type == "week_day":
                extract_param = "D"
            elif lookup_type == "iso_week_day":
                extract_sql = f"TO_CHAR({sql} - 1, %s)"
                extract_param = "D"
            elif lookup_type == "week":
                extract_sql = f"CAST(TO_CHAR({sql}, %s) AS INT)"
                extract_param = "IW"
            elif lookup_type == "quarter":
                extract_param = "Q"
            elif lookup_type == "iso_year":
                extract_param = "IYYY"
            else:
                lookup_type = lookup_type.upper()
                if not self._extract_format_re.fullmatch(lookup_type):
                    raise ValueError(f"Invalid loookup type: {lookup_type!r}")
                return f"EXTRACT({lookup_type} FROM {sql})", params
            return extract_sql, (*params, extract_param)

    if VERSION < (4, 1):
        def date_extract_sql(self, lookup_type, field_name):
            """
            Given a lookup_type of 'year', 'month' or 'day', returns the SQL that
            extracts a value from the given date field field_name.
            """
            if lookup_type == 'week_day':
                return "TO_CHAR(%s, 'D')" % field_name
            elif lookup_type == 'iso_week_day':
                return "TO_CHAR(%s - 1, 'D')" % field_name
            elif lookup_type == 'week':
                return "CAST(TO_CHAR(%s, 'IW') AS INT)" % field_name
            elif lookup_type == 'quarter':
                return "TO_CHAR(%s, 'Q')" % field_name
            elif lookup_type == 'iso_year':
                return "TO_CHAR(%s, 'IYYY')" % field_name
            else:
                return "EXTRACT(%s FROM %s)" % (lookup_type.upper(), field_name)

    def date_interval_sql(self, timedelta):
        return self.connection.features.parse_module.date_interval_sql(timedelta)

    def return_insert_columns(self, fields):
        return self.connection.features.parse_module.return_insert_columns(self, fields)

    def returning_columns(self, fields):
        return self.connection.features.parse_module.returning_columns(self, fields)

    def fetch_returned_rows(self, cursor, returning_params):
        if hasattr(cursor.cursor, 'has_returning') and cursor.cursor.has_returning is True:
            pos_tup = cursor.cursor.pos_tup
            returning_tup = cursor.cursor.returning_tup
            result_lst = []
            for i in pos_tup:
                result_lst.append(returning_tup[i])

            return [list(result_lst)]
        else:
            return [list(cursor.fetchone())]

    def fetch_returned_insert_columns(self, cursor, returning_params):

        if hasattr(cursor.cursor, 'has_returning') and cursor.cursor.has_returning is True:
            pos_tup = cursor.cursor.pos_tup
            returning_tup = cursor.cursor.returning_tup
            result_lst = []
            for i in pos_tup:
                result_lst.append(returning_tup[i])

            return tuple(result_lst)
        else:
            return cursor.fetchone()

    if VERSION >= (4, 1):
        def date_trunc_sql(self, lookup_type, sql, params, tzname=None):
            sql, params = self._convert_sql_to_tz(sql, params, tzname)
            trunc_param = None
            if lookup_type in ("year", "month"):
                trunc_param = lookup_type.upper()
            elif lookup_type == "quarter":
                trunc_param = "Q"
            elif lookup_type == "week":
                trunc_param = "IW"
            else:
                return f"TRUNC({sql})", params
            return f"TRUNC({sql}, %s)", (*params, trunc_param)

    if VERSION < (4, 1):
        def date_trunc_sql(self, lookup_type, field_name):
            """
            Given a lookup_type of 'year', 'month' or 'day', returns the SQL that
            truncates the given date field field_name to a date object with only
            the given specificity.
            """
            if lookup_type in ('year', 'month'):
                return "TRUNC(%s, '%s')" % (field_name, lookup_type.upper())
            elif lookup_type == 'quarter':
                return "TRUNC(%s, 'Q')" % field_name
            elif lookup_type == 'week':
                return "TRUNC(%s, 'IW')" % field_name
            else:
                return "TRUNC(%s)" % field_name
        
    _tzname_re = re.compile(r'^[\w/:+-]+$')
    
    def _prepare_tzname_delta(self, tzname):
        if '+' in tzname:
            return tzname[tzname.find('+'):]
        elif '-' in tzname:
            return tzname[tzname.find('-'):]
        return tzname    

    def _convert_field_to_tz(self, field_name, tzname):
        return self.connection.features.parse_module._convert_field_to_tz(field_name, tzname)

    if VERSION >= (4, 1):
        def datetime_cast_date_sql(self, sql, params, tzname):
            sql, params = self._convert_sql_to_tz(sql, params, tzname)
            return f"TRUNC({sql})", params

    if VERSION < (4, 1):
        def datetime_cast_date_sql(self, field_name, tzname):
            """
            Returns the SQL necessary to cast a datetime value to date value.
            """
            field_name = self._convert_field_to_tz(field_name, tzname)
            sql = 'TRUNC(%s)' % field_name
            return sql

    if VERSION >= (4, 1):
        def datetime_cast_time_sql(self, sql, params, tzname):
            # Since `TimeField` values are stored as TIMESTAMP change to the
            # default date and convert the field to the specified timezone.
            sql, params = self._convert_sql_to_tz(sql, params, tzname)
            convert_datetime_sql = (
                f"TO_TIMESTAMP(CONCAT('1900-01-01 ', TO_CHAR({sql}, 'HH24:MI:SS.FF')), "
                f"'YYYY-MM-DD HH24:MI:SS.FF')"
            )
            return (
                f"CASE WHEN {sql} IS NOT NULL THEN {convert_datetime_sql} ELSE NULL END",
                (*params, *params),
            )

    if VERSION < (4, 1):
        def datetime_cast_time_sql(self, field_name, tzname):
            # Since `TimeField` values are stored as TIMESTAMP where only the date
            # part is ignored, convert the field to the specified timezone.
            return self._convert_field_to_tz(field_name, tzname)

    if VERSION >= (4, 1):
        def datetime_extract_sql(self, lookup_type, sql, params, tzname):
            sql, params = self._convert_sql_to_tz(sql, params, tzname)
            return self.date_extract_sql(lookup_type, sql, params)

    if VERSION < (4, 1):
        def datetime_extract_sql(self, lookup_type, field_name, tzname):
            """
            Given a lookup_type of 'year', 'month', 'day', 'hour', 'minute' or
            'second', returns the SQL that extracts a value from the given
            datetime field field_name, and a tuple of parameters.
            """
            field_name = self._convert_field_to_tz(field_name, tzname)
            sql = self.date_extract_sql(lookup_type, field_name)
            return sql

    if VERSION >= (4, 1):
        def datetime_trunc_sql(self, lookup_type,sql, params, tzname):
            sql, params = self._convert_sql_to_tz(sql, params, tzname)
            trunc_param = None
            if lookup_type in ("year", "month"):
                trunc_param = lookup_type.upper()
            elif lookup_type == "quarter":
                trunc_param = "Q"
            elif lookup_type == "week":
                trunc_param = "IW"
            elif lookup_type == "hour":
                trunc_param = "HH24"
            elif lookup_type == "minute":
                trunc_param = "MI"
            elif lookup_type == "day":
                return f"TRUNC({sql})", params
            else:
                return f"CAST({sql} AS DATETIME)", params
            return f"TRUNC({sql}, %s)", (*params, trunc_param)

    if VERSION >= (4, 1):
        def time_trunc_sql(self, lookup_type, sql, params, tzname=None):
            # The implementation is similar to `datetime_trunc_sql` as both
            # `DateTimeField` and `TimeField` are stored as TIMESTAMP where
            # the date part of the later is ignored.
            sql, params = self._convert_sql_to_tz(sql, params, tzname)
            trunc_param = None
            if lookup_type == "hour":
                trunc_param = "HH24"
            elif lookup_type == "minute":
                trunc_param = "MI"
            elif lookup_type == "second":
                return f"CAST({sql} AS DATETIME)", params
            return f"TRUNC({sql}, %s)", (*params, trunc_param)

    if VERSION >= (4, 1):
        def _convert_sql_to_tz(self, sql, params, tzname):
            if not (settings.USE_TZ and tzname):
                return sql, params
            if not self._tzname_re.match(tzname):
                raise ValueError("Invalid time zone name: %s" % tzname)
            # Convert from connection timezone to the local time, returning
            # TIMESTAMP WITH TIME ZONE and cast it back to TIMESTAMP to strip the
            # TIME ZONE details.
            if self.connection.timezone_name != tzname:
                from_timezone_name = self.connection.timezone_name
                to_timezone_name = self._prepare_tzname_delta(tzname)
                return (
                    f"CAST((FROM_TZ({sql}, '{from_timezone_name}') AT TIME ZONE "
                    f"'{to_timezone_name}') AS TIMESTAMP)",
                    params,
                )
            return sql, params

    if VERSION < (4, 1):
        def datetime_trunc_sql(self, lookup_type, field_name, tzname):
            field_name = self._convert_field_to_tz(field_name, tzname)
            if lookup_type in ('year', 'month'):
                sql = "TRUNC(%s, '%s')" % (field_name, lookup_type.upper())
            elif lookup_type == 'week':
                sql = "TRUNC(%s, 'IW')" % field_name
            elif lookup_type == 'day':
                sql = "TRUNC(%s)" % field_name
            elif lookup_type == 'hour':
                sql = "TRUNC(%s, 'HH24')" % field_name
            elif lookup_type == 'minute':
                sql = "TRUNC(%s, 'MI')" % field_name
            else:
                sql = "CAST(%s AS DATETIME)" % field_name
            return sql

    if VERSION < (4, 1):
        def time_trunc_sql(self, lookup_type, field_name):
            if lookup_type == 'hour':
                sql = "TRUNC(%s, 'HH24')" % field_name
            elif lookup_type == 'minute':
                sql = "TRUNC(%s, 'MI')" % field_name
            elif lookup_type == 'second':
                sql = "CAST(%s AS TIME)" % field_name  # Cast to TIME removes sub-second precision.
            return sql

    def adapt_json_value(self, value, encoder):
        return json.dumps(value, ensure_ascii=False, cls=encoder)

    def deferrable_sql(self):
        """
        Returns the SQL necessary to make a constraint "initially deferred"
        during a CREATE TABLE statement.
        """
        return " DEFERRABLE INITIALLY DEFERRED"

    def drop_sequence_sql(self, table):
        """
        Returns any SQL necessary to drop the sequence for the given table.
        Returns None if no SQL is necessary.
        """
        return "DROP SEQUENCE %s;" % self.quote_name(self._get_sequence_name(table))

    def fetch_returned_insert_id(self, cursor):
        """
        Given a cursor object that has just performed an INSERT...RETURNING
        statement into a table that has an auto-incrementing ID, returns the
        newly created ID.
        """
        return int(cursor._insert_id_var.getvalue())

    def field_cast_sql(self, db_type, internal_type):
        """
        Given a column type (e.g. 'BLOB', 'VARCHAR'), and an internal type
        (e.g. 'GenericIPAddressField'), returns the SQL necessary to cast it
        before using it in a WHERE statement. Note that the resulting string
        should contain a '%s' placeholder for the column being searched against.
        """
        if db_type and db_type.endswith('LOB') and internal_type != 'JSONField':
            return "DBMS_LOB.SUBSTR(%s)"
        else:
            return "%s"

    def last_executed_query(self, cursor, sql, params):
        """
        Returns a string of the query last executed by the given cursor, with
        placeholders replaced with actual values.

        `sql` is the raw query containing placeholders, and `params` is the
        sequence of parameters. These are used by default, but this method
        exists for database backends to provide a better implementation
        according to their own quoting schemes.
        """
        # The DB API definition does not define this attribute.
        statement = cursor.statement

        return super(DatabaseOperations, self).last_executed_query(cursor, statement, params)

    def last_insert_id(self, cursor, table_name, pk_name):
        """
        Given a cursor object that has just performed an INSERT statement into
        a table that has an auto-incrementing ID, returns the newly created ID.

        This method also receives the table name and the name of the primary-key
        column.
        """
        if cursor.lastrowid is not None:
            query = 'select %s from %s where rowid = ?' % (self.quote_name(pk_name), self.quote_name(table_name),)
            cursor.execute(query, (cursor.lastrowid,))
        else:
            cursor.execute("SELECT IDENT_CURRENT(?)", (table_name.upper(),))
            
        value = cursor.fetchone()[0]
        return value

    def lookup_cast(self, lookup_type, internal_type=None):
        """
        Returns the string to use in a query when performing lookups
        ("contains", "like", etc.). The resulting string should contain a '%s'
        placeholder for the column being searched against.
        """
        if lookup_type in ('iexact', 'icontains', 'iregex', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        if internal_type == 'JSONField' and lookup_type == 'exact':
            return 'TO_CHAR(%s)'
        if internal_type == "TextField" and (lookup_type == 'exact' or lookup_type == 'in'):
            return "TO_CHAR(%s)"
        return "%s"

    def max_in_list_size(self):
        """
        Returns the maximum number of items that can be passed in a single 'IN'
        list condition, or None if the backend does not impose a limit.
        """
        return 1000

    def max_name_length(self):
        """
        Returns the maximum length of table and column names, or None if there
        is no limit.
        """
        return 128

    def no_limit_value(self):
        """
        Returns the value to use for the LIMIT when we are wanting "LIMIT
        infinity". Returns None if the limit clause can be omitted in this case.
        """
        return 9223372036854775807
    
    def limit_offset_sql(self, low_mark, high_mark):
        return self.connection.features.parse_module.limit_offset_sql(self, low_mark, high_mark)

    def pk_default_value(self):
        """
        Returns the value to use during an INSERT statement to specify that
        the field should use its default value.
        """
        return 'NULL'

    def process_clob(self, value):
        """
        Returns the value of a CLOB column, for backends that return a locator
        object that requires additional processing.
        """
        if value is None:
            return ''
        
        if isinstance(value, Database.LOB):
            value = force_str(value.read())
        
        return value

    def return_insert_id(self):
        """
        For backends that support returning the last insert ID as part
        of an insert query, this method returns the SQL and params to
        append to the INSERT query. The returned fragment should
        contain a format string to hold the appropriate column.
        """
        return "RETURNING %s INTO ?", (InsertIdVar(),)

    def quote_name(self, name):
        return self.connection.features.parse_module.quote_name(name)
    
    def dm_quote_name(self, name):
        if not (name.startswith('"') and name.endswith('"')):
            name = name.replace('"', '""')
            name = '"%s"' % truncate_name(name.upper(), 128)
        return name

    def random_function_sql(self):
        """
        Returns an SQL expression that returns a random value.
        """
        return "DBMS_RANDOM.RANDOM"

    def regex_lookup(self, lookup_type):
        """
        Returns the string to use in a query when performing regular expression
        lookups (using "regex" or "iregex"). The resulting string should
        contain a '%s' placeholder for the column being searched against.

        If the feature is not supported (or part of it is not supported), a
        NotImplementedError exception can be raised.
        """
        if lookup_type == 'regex':
            match_option = "'c'"
        else:
            match_option = "'i'"
        return 'REGEXP_LIKE(%%s, %%s, %s)' % match_option

    def savepoint_create_sql(self, sid):
        return convert_unicode("SAVEPOINT " + self.quote_name(sid))    

    def savepoint_rollback_sql(self, sid):
        return convert_unicode("ROLLBACK TO SAVEPOINT " + self.quote_name(sid))
    
    def savepoint_commit_sql(self, sid):
        return convert_unicode("RELEASE SAVEPOINT " + self.quote_name(sid))

    def format_for_duration_arithmetic(self, sql):
        return "NUMTODSINTERVAL(%s / 1000000, 'SECOND')" % sql

    def _get_django_constraints(self, tables):
        
        if not isinstance(tables, list) :
            return None
        
        size = len(tables)
        
        in_expr = ''
        for table in tables:
            if table == tables[size - 1]:
                in_expr = in_expr + "'%s'" % (table.upper())
            else:
                in_expr = in_expr + "'%s'," % (table.upper())
        
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    cons.table_name, cons.constraint_name
                FROM
                    user_constraints cons
                WHERE
                    cons.constraint_type = 'R'
                    AND cons.table_name in (%s)
            """ % (in_expr))
                   
            return cursor.fetchall()    
    
    def __foreign_key_constraints(self, table_name, recursive):
        with self.connection.cursor() as cursor:
            if recursive:
                cursor.execute("""
                    with cons_view as
                    (select cons.table_name p_tab_name, cons.constraint_name p_cons_name, r_cons.constraint_name r_cons_name, r_cons.table_name r_tab_name
                    from
                    user_constraints cons
                    join
                    user_constraints r_cons
                    on
                    cons.constraint_name = r_cons.r_constraint_name and cons.constraint_type = any('P','U') and r_cons.constraint_type = 'R')
                    select r_tab_name, r_cons_name from cons_view where p_tab_name = ?;""",
                   (table_name.upper(),)
                )
            else:
                cursor.execute("""
                    SELECT
                        cons.table_name, cons.constraint_name
                    FROM
                        user_constraints cons
                    WHERE
                        cons.constraint_type = 'R'
                        AND cons.table_name = ?
                """, (table_name.upper(),))
            return cursor.fetchall()

    @cached_property
    def _foreign_key_constraints(self):
        # 512 is large enough to fit the ~330 tables (as of this writing) in
        # Django's test suite.
        return lru_cache(maxsize=512)(self.__foreign_key_constraints)    

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        return self.connection.features.parse_module.sql_flush(db_op=self, style=style, tables=tables, reset_sequences=reset_sequences, allow_cascade=allow_cascade)

    def _get_no_autofield_sequence_name(self, table):

        name_length = self.max_name_length() - 3
        return '%s_SQ' % truncate_name(strip_quotes(table), name_length).upper()    

    def sequence_reset_by_name_sql(self, style, sequences):
        sql = []
        for sequence_info in sequences:
            no_autofield_sequence_name = self._get_no_autofield_sequence_name(sequence_info['table'])
            table = self.quote_name(sequence_info['table'])
            column = self.quote_name(sequence_info['column'] or 'id')
            query = self._sequence_reset_sql % {
                'no_autofield_sequence_name': no_autofield_sequence_name,
                'table': table,
                'column': column,
                'table_name': strip_quotes(table),
                'column_name': strip_quotes(column),
            }
            sql.append(query)
        return sql    

    def sequence_reset_sql(self, style, model_list):
        """
        Returns a list of the SQL statements required to reset sequences for
        the given models.

        The `style` argument is a Style object as returned by either
        color_style() or no_style() in django.core.management.color.
        """
        return []
    
        output = []
        query = self._sequence_reset_sql
        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, AutoField):
                    no_autofield_sequence_name = self._get_no_autofield_sequence_name(model._meta.db_table)
                    table = self.quote_name(model._meta.db_table)
                    column = self.quote_name(f.column)
                    output.append(query % {
                        'no_autofield_sequence_name': no_autofield_sequence_name,
                        'table': table,
                        'column': column,
                        'table_name': strip_quotes(table),
                        'column_name': strip_quotes(column),
                    })
                    # Only one AutoField is allowed per model, so don't
                    # continue to loop
                    break
            for f in model._meta.many_to_many:
                if not f.remote_field.through:
                    no_autofield_sequence_name = self._get_no_autofield_sequence_name(f.m2m_db_table())
                    table = self.quote_name(f.m2m_db_table())
                    column = self.quote_name('id')
                    output.append(query % {
                        'no_autofield_sequence_name': no_autofield_sequence_name,
                        'table': table,
                        'column': column,
                        'table_name': strip_quotes(table),
                        'column_name': 'ID',
                    })
        return output        
    
    def start_transaction_sql(self):
        """
        Returns the SQL statement required to start a transaction.
        """
        return ''

    def tablespace_sql(self, tablespace, inline=False):
        """
        Returns the SQL that will be used in a query to define the tablespace.

        Returns '' if the backend doesn't support tablespaces.

        If inline is True, the SQL is appended to a row; otherwise it's appended
        to the entire CREATE TABLE or CREATE INDEX statement.
        """
        if inline:
            return "USING INDEX TABLESPACE %s" % self.quote_name(tablespace)
        else:
            return "TABLESPACE %s" % self.quote_name(tablespace)

    def prep_for_iexact_query(self, x):
        return x    

    def adapt_datetimefield_value(self, value):
        """
        Transforms a datetime value to an object compatible with what is expected
        by the backend driver for datetime columns.
        """
        if value is None:
            return None
        
        # Expression values are adapted by the database.
        if hasattr(value, 'resolve_expression'):
            return value        
        
        # DAMENG doesn't support tz-aware datetimes
        if timezone.is_aware(value):
            if settings.USE_TZ:
                value = timezone.make_naive(value, self.connection.timezone)
            else:
                raise ValueError("Dameng backend does not support timezone-aware datetimes when USE_TZ is False.")

        return value

    def adapt_timefield_value(self, value):
        """
        Transforms a time value to an object compatible with what is expected
        by the backend driver for time columns.
        """
        if value is None:
            return None
        
        # Expression values are adapted by the database.
        if hasattr(value, 'resolve_expression'):
            return value        

        # Dameng doesn't support tz-aware times
        if timezone.is_aware(value):
            raise ValueError("Dameng backend does not support timezone-aware times.")

        return value

    def get_db_converters(self, expression):
        """
        Get a list of functions needed to convert field data.

        Some field types on some backends do not provide data in the correct
        format, this is the hook for converter functions.
        """
        converters = super(DatabaseOperations, self).get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        if internal_type in ['JSONField', 'TextField']:
            converters.append(self.convert_textfield_value)
        elif internal_type == 'BinaryField':
            converters.append(self.convert_binaryfield_value)
        elif internal_type in ['BooleanField', 'NullBooleanField']:
            converters.append(self.convert_booleanfield_value)
        elif internal_type == 'DateTimeField':
            converters.append(self.convert_datetimefield_value)
        elif internal_type == 'DateField':
            converters.append(self.convert_datefield_value)
        elif internal_type == 'TimeField':
            converters.append(self.convert_timefield_value)
        elif internal_type == 'UUIDField':
            converters.append(self.convert_uuidfield_value)
        elif internal_type == 'DecimalField':
            converters.append(self.convert_decimalfield_value)

        if self.connection.features.compatible_mode == 2 and expression.field.empty_strings_allowed:
            converters.append(
                self.convert_empty_bytes
                if internal_type == 'BinaryField' else
                self.convert_empty_string
            )
        return converters 
    
    # convert function
    def convert_decimalfield_value(self, value, expression, connection):
        if isinstance(value, str):
            return Decimal(value)
        if isinstance(value, int) or isinstance(value, float):
            return Decimal('%s' % (value))
        return value
    
    def convert_textfield_value(self, value, expression, connection):
        if isinstance(value, Database.LOB):
            value = force_str(value.read())
        return value

    def convert_binaryfield_value(self, value, expression, connection):
        if isinstance(value, Database.LOB):
            value = force_bytes(value.read())
        return value

    def convert_booleanfield_value(self, value, expression, connection):
        if value in (0, 1):
            value = bool(value)
        if value in ('0', '1'):
            value = bool(int(value))
        return value

    def convert_datetimefield_value(self, value, expression, connection):
        if value is not None:
            if settings.USE_TZ:
                value = timezone.make_aware(value, self.connection.timezone)
        return value

    def convert_datefield_value(self, value, expression, connection):
        if isinstance(value, Database.Timestamp) or isinstance(value, datetime.datetime):
            value = value.date()
        return value

    def convert_timefield_value(self, value, expression, connection):
        if isinstance(value, Database.Timestamp) or isinstance(value, datetime.datetime):
            value = value.time()
        return value

    def convert_uuidfield_value(self, value, expression, connection):
        if value is not None:
            value = uuid.UUID(value)
        return value

    @staticmethod
    def convert_empty_string(value, expression, connection):
        return '' if value is None else value

    @staticmethod
    def convert_empty_bytes(value, expression, connection):
        return b'' if value is None else value

    def combine_expression(self, connector, sub_expressions):

        if connector == '%%':
            return 'MOD(%s)' % ','.join(sub_expressions)
        elif connector == '&':
            return 'BITAND(%s)' % ','.join(sub_expressions)        
        elif connector == '^':
            return 'POWER(%s)' % ','.join(sub_expressions)
        return super(DatabaseOperations, self).combine_expression(connector, sub_expressions)    
    
    def _get_sequence_name(self, table):
        name_length = self.max_name_length() - 3
        return '%s_SQ' % truncate_name(table, name_length).upper()

    def _get_trigger_name(self, table):
        name_length = self.max_name_length() - 3
        return '%s_TR' % truncate_name(table, name_length).upper()            
    
    
    def bulk_insert_sql(self, fields, placeholder_rows):
        return " UNION ALL ".join(
            "SELECT %s FROM DUAL" % ", ".join(row)
            for row in placeholder_rows
        )
    
    def binary_placeholder_sql(self, value):
        """
        Some backends require special syntax to insert binary content (MySQL
        for example uses '_binary %s').
        """
        return '?'
    
    def conditional_expression_supported_in_where_clause(self, expression):
        """
        DM supports only EXISTS(...) or filters in the WHERE clause, others
        must be compared with True.
        """
        if isinstance(expression, (Exists, WhereNode)):
            return True
        if isinstance(expression, ExpressionWrapper) and expression.conditional:
            return self.conditional_expression_supported_in_where_clause(expression.expression)
        if isinstance(expression, RawSQL) and expression.conditional:
            return True
        return False
    
    def validate_autopk_value(self, value):
        #zero in AUTO_INCREMENT field does not work.
        if value == 0:
            raise ValueError('The database backend does not accept 0 as a '
                             'value for AutoField.')
        return value        


    
