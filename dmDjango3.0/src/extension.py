import re
import sys
import django
from django.conf import settings
from django.db.backends.utils import truncate_name

VERSION = django.VERSION
if len(VERSION) == 5 and VERSION[0] == 4 and VERSION[1] == 2 and VERSION[2] == 0 and VERSION[3] == 'alpha':
    ADAPT_JSON_VALUE = False
else:
    if VERSION > (4, 2):
        ADAPT_JSON_VALUE = True
    else:
        ADAPT_JSON_VALUE = False

class DMDialect_Adapter:

    def __init__(self):
        self.set_parse_sql = ""
        self.revert_parse_sql = ""
        self.sql_create_fk = ("ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s)"
                              " REFERENCES %(to_table)s (%(to_column)s) DEFERRABLE INITIALLY DEFERRED")
        self.test_create_user_statement = [("CREATE USER %(user)s IDENTIFIED BY %(password)s DEFAULT TABLESPACE "
                                            "%(tablespace)s TEMPORARY TABLESPACE %(tablespace_temp)s QUOTA UNLIMITED "
                                            "ON %(tablespace)s;"),
                                           ("GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE PROCEDURE, "
                                            "CREATE TRIGGER, CREATE INDEX, CREATE VIEW, CREATE MATERIALIZED VIEW "
                                            "TO %(user)s;")]
        self.test_create_db_statement = ["CREATE TABLESPACE {} DATAFILE {} SIZE 128 AUTOEXTEND ON NEXT 10"]
        self.test_drop_db_statement = ["DROP TABLESPACE IF EXISTS %(tablespace)s;"]
        self.test_drop_user_statement = ["DROP USER IF EXISTS %(user)s CASCADE;"]

        self.operators = {
            'exact': '= %s',
            'iexact': '= UPPER(%s)',
            'contains': "LIKE %s ESCAPE '\\'",
            'icontains': "LIKE UPPER(%s) ESCAPE '\\'",
            'gt': '> %s',
            'gte': '>= %s',
            'lt': '< %s',
            'lte': '<= %s',
            'startswith': "LIKE %s ESCAPE '\\'",
            'endswith': "LIKE %s ESCAPE '\\'",
            'istartswith': "LIKE UPPER(%s) ESCAPE '\\'",
            'iendswith': "LIKE UPPER(%s) ESCAPE '\\'",
        }

        self.pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%', '\%'), '_', '\_')"
        self.pattern_ops = {
            'contains': r"LIKE '%' || {} || '%' ESCAPE '\'",
            'icontains': r"LIKE '%' || UPPER({}) || '%' ESCAPE '\'",
            'startswith': r"LIKE {} || '%' ESCAPE '\'",
            'istartswith': r"LIKE UPPER({}) || '%%' ESCAPE '\'",
            'endswith': r"LIKE '%' || {} ESCAPE '\'",
            'iendswith': r"LIKE '%' || UPPER({}) ESCAPE '\'",
        }


    def quote_name(self, name):
        if not (name.startswith('"') and name.endswith('"')):
            name = name.replace('"', '""')
            name = '"%s"' % truncate_name(name.upper(), 128)
        return name

    def test_quote_name(self, name):
        if not (name.startswith('"') and name.endswith('"')):
            name = name.replace('"', '""')
            name = '"%s"' % truncate_name(name.upper(), 128)
        return name

    def add_quote_name(self, name):
        return "'" + name.replace("'", "''") + "'"

    def test_add_quote_name(self, name):
        return "'" + name.replace("'", "''") + "'"

    def add_dquote_name(self, name):
        return '"' + name.replace('"', '""') + '"'

    def test_add_dquote_name(self, name):
        return '"' + name.replace('"', '""') + '"'

    def add_quote_password(self, password):
        return '"' + password.replace('"', '""') + '"'

    def test_add_quote_password(self, password):
        return '"' + password.replace('"', '""') + '"'

    def create_test_db(self, db_creation, cursor, parameters, verbosity, autoclobber):
        confirm = None
        if db_creation._test_database_create():
            try:
                db_creation._execute_test_db_creation(cursor, parameters, verbosity)
            except Exception as e:
                sys.stderr.write("Got an error creating the test database: %s\n" % e)
                if not autoclobber:
                    confirm = input("It appears the test database, %s, already exists. Type 'yes' to delete it, or 'no' to cancel: " % parameters["dbname"])
                if autoclobber or confirm == "yes":
                    try:
                        if verbosity >= 1:
                            print("Destroying old test database '%s'..." % db_creation.connection.alias)
                        db_creation._destroy_test_user(cursor, parameters, verbosity)
                        db_creation._execute_test_db_destruction(cursor, parameters, verbosity)
                        db_creation._execute_test_db_creation(cursor, parameters, verbosity)
                    except Exception as e:
                        sys.stderr.write("Got an error recreating the test database: %s\n" % e)
                        sys.exit(2)
                else:
                    print("Tests cancelled.")
                    sys.exit(1)

        confirm = None
        if db_creation._test_user_create():
            if verbosity >= 1:
                print("Creating test user...")
            try:
                db_creation._create_test_user(cursor, parameters, verbosity)
            except Exception as e:
                sys.stderr.write("Got an error creating the test user: %s\n" % e)
                if not autoclobber:
                    confirm = input("It appears the test user, %s, already exists. Type 'yes' to delete it, or 'no' to cancel: " % parameters["user"])
                if autoclobber or confirm == "yes":
                    try:
                        if verbosity >= 1:
                            print("Destroying old test user...")
                        db_creation._destroy_test_user(cursor, parameters, verbosity)
                        if verbosity >= 1:
                            print("Creating test user...")
                        db_creation._create_test_user(cursor, parameters, verbosity)
                    except Exception as e:
                        sys.stderr.write("Got an error recreating the test user: %s\n" % e)
                        sys.exit(2)
                else:
                    print("Tests cancelled.")
                    sys.exit(1)

    def deferrable_sql(self):
        return " DEFERRABLE INITIALLY DEFERRED"
    
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

    def adjust_insert_sql(self, compiler, result):
        for sql, params in result:
            opts = compiler.query.get_meta()
            qn = compiler.connection.ops.quote_name
            sql = self.fix_auto(sql, opts, compiler.query.fields, qn)

        return [(sql, params)]

    def add_element(self, lst):
        return lst

    def sql_flush(self, db_op, style, tables, reset_sequences=False, allow_cascade=False):
        if not tables:
            return []

        truncated_tables = {table.upper() for table in tables}
        constraints = set()

        for table in tables:
            for foreign_table, constraint in db_op._foreign_key_constraints(table, recursive=allow_cascade):
                if allow_cascade:
                    truncated_tables.add(foreign_table)
                constraints.add((foreign_table, constraint))
        sql = [
                  '%s %s %s %s %s %s;' % (
                      style.SQL_KEYWORD('ALTER'),
                      style.SQL_KEYWORD('TABLE'),
                      style.SQL_FIELD(db_op.quote_name(table)),
                      style.SQL_KEYWORD('DISABLE'),
                      style.SQL_KEYWORD('CONSTRAINT'),
                      style.SQL_FIELD(db_op.quote_name(constraint)),
                  ) for table, constraint in constraints
              ] + [
                  '%s %s %s;' % (
                      style.SQL_KEYWORD('TRUNCATE'),
                      style.SQL_KEYWORD('TABLE'),
                      style.SQL_FIELD(db_op.quote_name(table)),
                  ) for table in truncated_tables
              ] + [
                  '%s %s %s %s %s %s;' % (
                      style.SQL_KEYWORD('ALTER'),
                      style.SQL_KEYWORD('TABLE'),
                      style.SQL_FIELD(db_op.quote_name(table)),
                      style.SQL_KEYWORD('ENABLE'),
                      style.SQL_KEYWORD('CONSTRAINT'),
                      style.SQL_FIELD(db_op.quote_name(constraint)),
                  ) for table, constraint in constraints
              ]

        if reset_sequences:
            sequences = [
                sequence
                for sequence in db_op.connection.introspection.sequence_list()
                if sequence['table'].upper() in truncated_tables
            ]
        return sql

    def date_interval_sql(self, timedelta):
        minutes, seconds = divmod(timedelta.seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days = str(timedelta.days)
        day_precision = len(days)
        fmt = "INTERVAL '%s %02d:%02d:%02d.%06d' DAY(%d) TO SECOND(6)"
        return fmt % (days, hours, minutes, seconds, timedelta.microseconds,
                day_precision), []

    def _convert_field_to_tz(self, field_name, tzname):
        if not settings.USE_TZ:
            return field_name
        _tzname_re = re.compile(r'^[\w/:+-]+$')
        if not _tzname_re.match(tzname):
            raise ValueError("Invalid time zone name: %s" % tzname)
        result = "(FROM_TZ(%s, '0:00') AT TIME ZONE '%s')" % (field_name, tzname)
        result = "TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS')" % result
        result = "TO_DATE(%s, 'YYYY-MM-DD HH24:MI:SS')" % result
        return "CAST(%s AS TIMESTAMP)" % result

    def sql_flush(self, db_op, style, tables, reset_sequences=False, allow_cascade=False):
        if not tables:
            return []

        truncated_tables = {table.upper() for table in tables}
        constraints = set()

        for table in tables:
            for foreign_table, constraint in db_op._foreign_key_constraints(table, recursive=allow_cascade):
                if allow_cascade:
                    truncated_tables.add(foreign_table)
                constraints.add((foreign_table, constraint))
        sql = [
                  '%s %s %s %s %s %s;' % (
                      style.SQL_KEYWORD('ALTER'),
                      style.SQL_KEYWORD('TABLE'),
                      style.SQL_FIELD(db_op.quote_name(table)),
                      style.SQL_KEYWORD('DISABLE'),
                      style.SQL_KEYWORD('CONSTRAINT'),
                      style.SQL_FIELD(db_op.quote_name(constraint)),
                  ) for table, constraint in constraints
              ] + [
                  '%s %s %s;' % (
                      style.SQL_KEYWORD('TRUNCATE'),
                      style.SQL_KEYWORD('TABLE'),
                      style.SQL_FIELD(db_op.quote_name(table)),
                  ) for table in truncated_tables
              ] + [
                  '%s %s %s %s %s %s;' % (
                      style.SQL_KEYWORD('ALTER'),
                      style.SQL_KEYWORD('TABLE'),
                      style.SQL_FIELD(db_op.quote_name(table)),
                      style.SQL_KEYWORD('ENABLE'),
                      style.SQL_KEYWORD('CONSTRAINT'),
                      style.SQL_FIELD(db_op.quote_name(constraint)),
                  ) for table, constraint in constraints
              ]

        if reset_sequences:
            sequences = [
                sequence
                for sequence in db_op.connection.introspection.sequence_list()
                if sequence['table'].upper() in truncated_tables
            ]
        return sql

    def return_insert_columns(self, db_op, fields):
        if not fields:
            return '', ()
        from .utils import InsertVar
        field_names = []
        params = []
        for field in fields:
            field_names.append('%s.%s' % (
                db_op.quote_name(field.model._meta.db_table),
                db_op.quote_name(field.column),
            ))
            params.append(InsertVar(field))
        temp_str = 'RETURNING %s INTO %s' % (
            ', '.join(field_names),
            ', '.join(['%s'] * len(params)),
        )
        temp_str = temp_str.replace('%s', '?')
        return temp_str, tuple(params)

    def returning_columns(self, db_op, fields):
        if not fields:
            return "", ()
        from .utils import InsertVar
        field_names = []
        params = []
        for field in fields:
            field_names.append(
                "%s.%s"
                % (
                    db_op.quote_name(field.model._meta.db_table),
                    db_op.quote_name(field.column),
                )
            )
            params.append(InsertVar(field))
        return "RETURNING %s INTO %s" % (
            ", ".join(field_names),
            ", ".join(["%s"] * len(params)),
        ), tuple(params)

    def do_execute(self, cursor_wp, query, args):
        args_temp = []
        pos_list = []
        has_returning = False
        from .utils import InsertVar
        if type(args) is tuple or type(args) is list:
            for i in range(len(args)):
                if isinstance(args[i], InsertVar):
                    args_temp.append(None)
                    has_returning = True
                    pos_list.append(i)
                else:
                    args_temp.append(args[i])
            args = tuple(args_temp)
            query = cursor_wp.convert_query(query)
        elif type(args) is dict:
            query = cursor_wp.convert_query_dict_args(query)
        else:
            query = cursor_wp.convert_query(query)

        result = cursor_wp.cursor.execute(query, args)
        if has_returning:
            cursor_wp.returning_tup = tuple(result)
            cursor_wp.has_returning = has_returning
            cursor_wp.pos_tup = tuple(pos_list)
        return result

    def limit_offset_sql(self, db_op, low_mark, high_mark):
        fetch, offset = db_op._get_limit_offset_params(low_mark, high_mark)
        return ' '.join(sql for sql in (
            ('OFFSET %d ROWS' % offset) if offset else None,
            ('FETCH FIRST %d ROWS ONLY' % fetch) if fetch else None,
        ) if sql)

class DMMySQLDialect_Adapter(DMDialect_Adapter):

    def __init__(self):
        super().__init__()
        self.set_parse_sql = "SP_SET_SESSION_PARSE_TYPE('DM');"
        self.revert_parse_sql = "SP_SET_SESSION_PARSE_TYPE('MySQL');"
        self.sql_create_fk = ("ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s)"
                              " REFERENCES %(to_table)s (%(to_column)s)")
        self.test_create_db_statement = self.add_element(self.test_create_db_statement)
        self.test_drop_db_statement = self.add_element(self.test_drop_db_statement)
        self.test_drop_user_statement = self.add_element(self.test_drop_user_statement)
        self.test_create_user_statement = self.add_element(self.test_create_user_statement)

        self.operators = {
            'exact': '= %s',
            'iexact': 'LIKE UPPER(%s)',
            'contains': 'LIKE %s',
            'icontains': 'LIKE UPPER(%s)',
            'gt': '> %s',
            'gte': '>= %s',
            'lt': '< %s',
            'lte': '<= %s',
            'startswith': 'LIKE %s',
            'endswith': 'LIKE %s',
            'istartswith': 'LIKE UPPER(%s)',
            'iendswith': 'LIKE UPPER(%s)',
        }

        self.pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\\', '\\\\'), '%%', '\%%'), '_', '\_')"
        self.pattern_ops = {
            'contains': "LIKE CONCAT('%%', {}, '%%')",
            'icontains': "LIKE CONCAT('%%', UPPER({}), '%%')",
            'startswith': "LIKE CONCAT({}, '%%')",
            'istartswith': "LIKE CONCAT(UPPER({}), '%%')",
            'endswith': "LIKE CONCAT('%%', {})",
            'iendswith': "LIKE CONCAT('%%', UPPER({}))",
        }

    def add_element(self, lst):
        lst.insert(0, self.set_parse_sql)
        lst.append(self.revert_parse_sql)
        return lst

    def quote_name(self, name):
        if not (name.startswith("`") and name.endswith("`")):
            name = name.replace("`", "``")
            name = "`%s`" % truncate_name(name.upper(), 128)
        return name

    def add_dquote_name(self, name):
        return "`" + name.replace("`", "``") + "`"

    def add_quote_password(self, password):
        return "'" + password.replace("'", "''") + "'"

    def deferrable_sql(self):
        return ""

    def adjust_insert_sql(self, compiler, result):
        return result

    def date_interval_sql(self, timedelta):
        minutes, seconds = divmod(timedelta.seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days = str(timedelta.days)
        day_precision = len(days)
        fmt = "INTERVAL '%s %02d:%02d:%02d.%06d' DAY_MICROSECOND"
        return fmt % (days, hours, minutes, seconds, timedelta.microseconds)
    
    def _convert_field_to_tz(self, field_name, tzname):
        if settings.USE_TZ:
            raise NotImplementedError('Vonversion of timezone is not supported in MySQL syntax mode')
        return field_name

    def sql_flush(self, db_op, style, tables, reset_sequences=False, allow_cascade=False):
        if tables:
            sql = ['%s %s %s;' % (
                    style.SQL_KEYWORD('DELETE'),
                    style.SQL_KEYWORD('FROM'),
                    style.SQL_FIELD(self.quote_name(table_name)),
                ) for table_name in tables]
            return sql
        else:
            return []

    def return_insert_columns(self, db_op, fields):
        return '', ()

    def returning_columns(self, db_op, fields):
        return '', ()

    def do_execute(self, cursor_wp, query, args):
        try:
            query = cursor_wp.convert_query(query)
            result = cursor_wp.cursor.execute(query, args)
        except Exception as e:
            raise
        return result

    def limit_offset_sql(self, db_op, low_mark, high_mark):
        limit, offset = db_op._get_limit_offset_params(low_mark, high_mark)
        return ' '.join(sql for sql in (
            ('LIMIT %d' % limit) if limit else None,
            ('OFFSET %d' % offset) if offset else None,
        ) if sql)

class DMTSQLDialect_Adapter(DMDialect_Adapter):
    pass