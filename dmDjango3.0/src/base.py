"""
Dameng database backend for Django.
"""
from __future__ import unicode_literals

from .extension import DMTSQLDialect_Adapter, DMMySQLDialect_Adapter
from django.db.backends.base.base import BaseDatabaseWrapper
from django.utils.functional import cached_property
from django.utils.asyncio import async_unsafe
from django.utils.regex_helper import _lazy_re_compile

try:
    import dmPython as Database
    Database.Binary = bytes
except ImportError as e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading dmPython module: %s" % e)

# Some of these import dmPython, so import them after checking if it's installed.
from .client import DatabaseClient                      # isort:skip
from .creation import DatabaseCreation                  # isort:skip
from .features import DatabaseFeatures                  # isort:skip
from .introspection import DatabaseIntrospection        # isort:skip
from .operations import DatabaseOperations              # isort:skip
from .schema import DatabaseSchemaEditor                # isort:skip
from .validation import DatabaseValidation              # isort:skip

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError   

class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'dameng'
    display_name = 'DM'
    
    data_types = {
        'SmallAutoField': 'INTEGER IDENTITY(1,1)',
        'AutoField': 'INTEGER IDENTITY(1,1)',
        'BigAutoField': 'BIGINT IDENTITY(1,1)',
        'BinaryField': 'BLOB',
        'BooleanField': 'TINYINT',
        'CharField': 'NVARCHAR2(%(max_length)s)',
        'CommaSeparatedIntegerField': 'VARCHAR(%(max_length)s)',
        'DateField': 'DATE',
        'DateTimeField': 'TIMESTAMP(6)',
        'DecimalField': 'NUMBER(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'INTERVAL DAY(9) TO SECOND(6)',
        'FileField': 'NVARCHAR2(%(max_length)s)',
        'FilePathField': 'NVARCHAR2(%(max_length)s)',
        'FloatField': 'DOUBLE PRECISION',
        'IntegerField': 'INTEGER',
        'BigIntegerField': 'BIGINT',
        'IPAddressField': 'VARCHAR(15)',
        'GenericIPAddressField': 'VARCHAR(39)',
        'NullBooleanField': 'TINYINT',
        'OneToOneField': 'INTEGER',
        'PositiveIntegerField': 'INTEGER',
        'PositiveBigIntegerField': 'BIGINT',
        'PositiveSmallIntegerField': 'SMALLINT',
        'SlugField': 'NVARCHAR2(%(max_length)s)',
        'SmallIntegerField': 'SMALLINT',
        'TextField': 'TEXT',
        'TimeField': 'TIMESTAMP(6)',
        'URLField': 'VARCHAR(%(max_length)s)',
        'UUIDField': 'VARCHAR(32)',
        'JSONField': 'JSON',
        'VectorField': 'VECTOR',
    }
    
    data_type_check_constraints = {
        'BooleanField': '%(qn_column)s IN (0,1)',
        'NullBooleanField': '(%(qn_column)s IN (0,1)) OR (%(qn_column)s IS NULL)',
        'PositiveIntegerField': '%(qn_column)s >= 0',
        'PositiveSmallIntegerField': '%(qn_column)s >= 0',
        'PositiveBigIntegerField': '%(qn_column)s >= 0',
        'JSONField': '%(qn_column)s IS JSON',
    }
    
    # DM doesn't support a database index on these columns.
    _limited_data_types = ('clob', 'nclob', 'blob', 'text')

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    validation_class = DatabaseValidation    

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self.features = DatabaseFeatures(self)        
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = DatabaseValidation(self)    
        
    def get_connection_params(self):        
        conn_params = self.settings_dict['OPTIONS'].copy()        
        return conn_params

    def _get_dict_value(self, settings_dict, key_name, default_value):
        dict_name = settings_dict[key_name]
        if dict_name != '' and dict_name is not None:
            return dict_name.strip()
        else:
            return default_value

    def _connect_string(self):
        settings_dict = self.settings_dict            
        user = self._get_dict_value(settings_dict, 'USER', 'SYSDBA')
        passwd = self._get_dict_value(settings_dict, 'PASSWORD', '')
        host = self._get_dict_value(settings_dict, 'HOST', 'localhost')
        port = self._get_dict_value(settings_dict, 'PORT', 5236)
        mpp_type = settings_dict['OPTIONS'].get('mpp_type', {}).get('mpp_type')
        ssl_path = settings_dict['OPTIONS'].get('ssl_path', {}).get('ssl_path')
        ssl_pwd = settings_dict['OPTIONS'].get('ssl_pwd', {}).get('ssl_pwd')
        
        if mpp_type:
            if port is None or port == "":
                conn_string = '%s/%s*%s@%s' % (user, passwd, mpp_type, host)    #use dm service name in dm_svc.conf to connect to db
            else:
                conn_string = '%s/%s*%s@%s:%s' % (user, passwd, mpp_type, host, port)
        else:
            if port is None or port == "":
                conn_string = '%s/%s@%s' % (user, passwd, host)     #use dm service name in dm_svc.conf to connect to db
            else:
                conn_string = '%s/%s@%s:%s' % (user, passwd, host, port)
                
        if ssl_path:
            conn_string += '#%s' % (ssl_path)
                
        if ssl_pwd:
            conn_string += '@%s' % (ssl_pwd) 
        
        return conn_string

    def _connect_params(self):
        settings_dict = self.settings_dict
        user = self._get_dict_value(settings_dict, 'USER', 'SYSDBA')
        passwd = self._get_dict_value(settings_dict, 'PASSWORD', '')
        host = self._get_dict_value(settings_dict, 'HOST', 'localhost')
        port = self._get_dict_value(settings_dict, 'PORT', 5236)
        mpp_type = settings_dict['OPTIONS'].get('mpp_type', {}).get('mpp_type')
        ssl_path = settings_dict['OPTIONS'].get('ssl_path', {}).get('ssl_path')
        ssl_pwd = settings_dict['OPTIONS'].get('ssl_pwd', {}).get('ssl_pwd')
        
        conn_param = {}
        conn_param['user'] = user
        conn_param['password'] = passwd
        conn_param['host'] = host
        conn_param['port'] = int(port)
        conn_param['mpp_login'] = False
        conn_param['ssl_path'] = ''
        conn_param['ssl_pwd'] = ''
        
        if ssl_path:
            conn_param['ssl_path'] = ssl_path
            
        if ssl_pwd:
            conn_param['ssl_pwd'] = ssl_pwd
            
        if mpp_type:
            conn_param['mpp_login'] = mpp_type

        return conn_param    
    
    @async_unsafe
    def get_new_connection(self, conn_params):
        params = self._connect_params()
        if 'empty_string_as_null' in conn_params:
            if type(conn_params['empty_string_as_null']) is bool:
                if conn_params['empty_string_as_null'] is True:
                    self.features.interprets_empty_strings_as_nulls = True
                del conn_params['empty_string_as_null']
            else:
                raise ValueError("The empty_string_as_null must be of bool type")

        if 'compatible_mode' in conn_params:
            if type(conn_params['compatible_mode']) is int:
                self.features.compatible_mode = conn_params['compatible_mode']
                del conn_params['compatible_mode']
            else:
                raise ValueError("The compatible_mode must be of int type and corresponds to "
                                 "the following compatibility modes:\n"
                                 "            0:none, 1:SQL92, 2:Oracle, 3:MS SQL Server, "
                                 "4:MySQL, 5:DM6, 6:Teradata, 7:PG, 8:DB2")

        if 'parse_type' in conn_params:
            if type(conn_params['parse_type']) is str:
                parse_type = conn_params['parse_type'].upper()
                if parse_type in ['DM', 'TSQL', 'MYSQL']:
                    if parse_type == 'TSQL':
                        self.features.parse_module = DMTSQLDialect_Adapter()
                    elif parse_type == 'MYSQL':
                        self.data_types['SmallAutoField'] = 'INTEGER AUTO_INCREMENT'
                        self.data_types['AutoField'] = 'INTEGER AUTO_INCREMENT'
                        self.data_types['BigAutoField'] = 'BIGINT AUTO_INCREMENT'
                        self.data_types['DurationField'] = 'BIGINT'
                        self.data_types['DateTimeField'] = 'DATETIME(6)'
                        self.data_types['TimeField'] = 'DATETIME(6)'
                        if 'JSONField' in self.data_type_check_constraints:
                            del self.data_type_check_constraints['JSONField']
                        self.SchemaEditorClass.sql_create_fk = self.features.parse_module.sql_create_fk
                        self.features.can_return_columns_from_insert = False
                        self.features.parse_module = DMMySQLDialect_Adapter()
                        self.features.has_native_duration_field = False
                        self.features.allows_auto_pk_0 = False
                        self.features.supports_order_by_nulls_modifier = False
                        self.features.has_select_for_update = False
                        self.features.has_select_for_update_of = False
                else:
                    raise ValueError("The parameter parse_type can only be set to one of DM, TSQL or MySQL")
            else:
                raise ValueError("The parameter parse_type can only be set to string type")

        self.operators = self.features.parse_module.operators
        self.pattern_esc = self.features.parse_module.pattern_esc
        self.pattern_ops = self.features.parse_module.pattern_ops

        try:
            return Database.connect(user=params['user'],
                                password=params['password'],
                                host=params['host'],
                                port=params['port'],
                                mpp_login=params['mpp_login'],
                                ssl_path=params['ssl_path'],
                                ssl_pwd=params['ssl_pwd'],
                                **conn_params
                                )
        except Database.DatabaseError as e:
            raise DatabaseError
        except Exception as e:
            raise
    
    def init_connection_state(self):    
        #do nothing
        pass
        
    def create_cursor(self, name=None):
        cursor = self.connection.cursor()
        return CursorWrapper(self.features.parse_module,cursor)
    
    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autoCommit = autocommit
    
    def disable_constraint_checking(self):

        tables = self.introspection.django_table_names(only_existing=True, include_views=False)
        
        if not tables:
            return False
        
        constraints = set()
        
        for foreign_table, constraint in self.ops._get_django_constraints(tables):
            constraints.add((foreign_table, constraint)) 

        sqls = [
            'ALTER TABLE /*+ALTER_TAB_COMMIT(0)*/ %s DISABLE CONSTRAINT %s;' % (
                self.ops.quote_name(table),
                self.ops.quote_name(constraint),
            ) for table, constraint in constraints
        ]

        with self.cursor() as cursor:
            for sql in sqls:
                cursor.execute(sql)
        
        return False

    def enable_constraint_checking(self):
        """
        Re-enable foreign key checks after they have been disabled.
        """
        
        self.needs_rollback, needs_rollback = False, self.needs_rollback
        
        tables = self.introspection.django_table_names(only_existing=True, include_views=False)
    
        if not tables:
            self.needs_rollback = needs_rollback
            return
    
        constraints = set()
    
        for foreign_table, constraint in self.ops._get_django_constraints(tables):
            constraints.add((foreign_table, constraint)) 
        
        sqls = [
                'ALTER TABLE /*+ALTER_TAB_COMMIT(0)*/ %s ENABLE CONSTRAINT %s;' % (
                    self.ops.quote_name(table),
                    self.ops.quote_name(constraint),
                    ) for table, constraint in constraints
            ]
        
        try:
            with self.cursor() as cursor:
                for sql in sqls:
                    cursor.execute(sql)
        finally:
            self.needs_rollback = needs_rollback
            
    def check_constraints(self, table_names=None):
        """
        Backends can override this method if they can apply constraint
        checking (e.g. via "SET CONSTRAINTS ALL IMMEDIATE"). Should raise an
        IntegrityError if any invalid foreign key references are encountered.
        """
        self.enable_constraint_checking()

    def is_usable(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute('select 1 from dual')
        except Database.Error:
            return False
        else:
            return True
        
    @cached_property
    def dameng_full_version(self):
        with self.temporary_connection():
            return self.connection.server_version

    @cached_property
    def dameng_version(self):
        try:
            return int(self.dameng_full_version.split('.')[0])
        except ValueError:
            return None        


FORMAT_QMARK_REGEX = _lazy_re_compile(r'(?<!%)%s')
PYFORMAT_QMARK_REGEX = _lazy_re_compile(r'%\((\w+)\)s')

class CursorWrapper(object):
        
    codes_for_integrityerror = (1048,)

    def __init__(self, parse_module, cursor):
        self.parse_module = parse_module
        self.cursor = cursor
    
    def convert_query(self, query):
        return FORMAT_QMARK_REGEX.sub('?', query).replace('%%', '%')

    def convert_query_dict_args(self, query):
        return PYFORMAT_QMARK_REGEX.sub(r':\1', query).replace('%%', '%')

    def execute(self, query, args=None):
        try:
            # args is None means no string interpolation
            try:
                if args is None:
                    return self.cursor.execute(query, args)
                else:
                    return self.parse_module.do_execute(self, query, args)
            except Database.DatabaseError as e:
                if hasattr(e.args[0], "code") == False:
                    raise
                
                if e.args[0].code == -6407 or e.args[0].code == -7116:
                    return self.cursor.execute(query, args)
                elif e.args[0].code == -6105:
                    raise
                else:
                    raise
            except Database.OperationalError as e:
                raise
        except Database.OperationalError as e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            if e.args[0] in self.codes_for_integrityerror:
                raise IntegrityError(*tuple(e.args))
            raise
        except Database.DatabaseError as e:
            if hasattr(e.args[0], "code") == False:
                tmpstr = str(e)
                if tmpstr.find("Not Open") != -1:
                    raise Database.InterfaceError
            
            if isinstance(e, Database.IntegrityError):
                raise IntegrityError(*tuple(e.args))
            
            raise

    def executemany(self, query, args):
        if not args:
            # No params given, nothing to do
            return None

        try:
            query = self.convert_query(query)
            query = self.convert_query_dict_args(query)
            return self.cursor.executemany(query, args)
        except Database.OperationalError as e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            if e.args[0] in self.codes_for_integrityerror:
                raise IntegrityError(*tuple(e.args))
            raise

    def __getattr__(self, attr):
        if attr == 'rowcount':
            pass
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)
        return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):        
        self.close()
