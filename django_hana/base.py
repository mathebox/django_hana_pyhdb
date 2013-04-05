"""
SAP HANA database backend for Django.
"""
import logging
import sys

from django.db import utils
from django.db.backends import *
from django.db.backends.signals import connection_created
from django.db.backends.hana.operations import DatabaseOperations
from django.db.backends.hana.client import DatabaseClient
from django.db.backends.hana.creation import DatabaseCreation
from django.db.backends.hana.introspection import DatabaseIntrospection
from django.utils.safestring import SafeText, SafeBytes
from django.utils import six
from django.utils.timezone import utc
from time import time

try:
    from hdbcli import dbapi as Database
except ImportError as e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading SAP HANA Python driver: %s" % e)

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError

logger = logging.getLogger('django.db.backends')

class DatabaseFeatures(BaseDatabaseFeatures):
    needs_datetime_string_cast = True
    can_return_id_from_insert = False
    requires_rollback_on_dirty_transaction = True
    has_real_datatype = True
    can_defer_constraint_checks = True
    has_select_for_update = True
    has_select_for_update_nowait = True
    has_bulk_insert = False
    supports_tablespaces = False
    supports_transactions = True
    can_distinct_on_fields = False
    uses_autocommit = True
    uses_savepoints = False
    can_introspect_foreign_keys = False
    supports_timezones = False


class CursorWrapper(object):
    """
        Hana doesn't support %s placeholders
        Wrapper to convert all %s placeholders to qmark(?) placeholders
    """

    def __init__(self, cursor, db):
        self.cursor = cursor
        self.db = db

    def set_dirty(self):
        if self.db.is_managed():
            self.db.set_dirty()

    def __getattr__(self, attr):
        self.set_dirty()
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)


    def execute(self, sql, params=()):
        """
            execute with replaced placeholders
        """
        self.cursor.execute(self._replace_params(sql,len(params) if params else 0),params)

    def executemany(self, sql, param_list):
        self.cursor.executemany(self._replace_params(sql,len(param_list[0]) if param_list and len(param_list)>0 else 0),param_list)

    def _replace_params(self,sql,params_count):
        """
            converts %s style placeholders to ?
        """
        str_placeholders='?' * params_count

        return sql % tuple([p for p in str_placeholders]);


class CursorDebugWrapper(CursorWrapper):

    def execute(self, sql, params=()):
        self.set_dirty()
        start = time()
        try:
            return CursorWrapper.execute(self,sql, params)
        finally:
            stop = time()
            duration = stop - start
            sql = self.db.ops.last_executed_query(self.cursor, sql, params)
            self.db.queries.append({
                'sql': sql,
                'time': "%.3f" % duration,
            })
            logger.debug('(%.3f) %s; args=%s' % (duration, sql, params),
                extra={'duration': duration, 'sql': sql, 'params': params}
            )

    def executemany(self, sql, param_list):
        self.set_dirty()
        start = time()
        try:
            return CursorWrapper.executemany(self,sql, param_list)
        finally:
            stop = time()
            duration = stop - start
            try:
                times = len(param_list)
            except TypeError:           # param_list could be an iterator
                times = '?'
            self.db.queries.append({
                'sql': '%s times: %s' % (times, sql),
                'time': "%.3f" % duration,
            })
            logger.debug('(%.3f) %s; args=%s' % (duration, sql, param_list),
                extra={'duration': duration, 'sql': sql, 'params': param_list}
            )


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'HANA'
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'regex': '~ %s',
        'iregex': '~* %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self.features = DatabaseFeatures(self)
        autocommit = self.settings_dict["OPTIONS"].get('autocommit', False)
        self.features.uses_autocommit = autocommit

        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation(self)

    def close(self):
        self.validate_thread_sharing()
        if self.connection is None:
            return
        try:
            self.connection.close()
            self.connection = None
        except Database.Error:
            # In some cases (database restart, network connection lost etc...)
            # the connection to the database is lost without giving Django a
            # notification. If we don't set self.connection to None, the error
            # will occur a every request.
            self.connection = None
            logger.warning('saphana error while closing the connection.',
                exc_info=sys.exc_info()
            )
            raise

    def _cursor(self):
        settings_dict = self.settings_dict
        if self.connection is None:
            if not settings_dict['NAME']:
                from django.core.exceptions import ImproperlyConfigured
                raise ImproperlyConfigured(
                    "settings.DATABASES is improperly configured. "
                    "Please supply the NAME value.")
            conn_params = {
                'database': settings_dict['NAME'],
            }
            conn_params.update(settings_dict['OPTIONS'])
            if 'autocommit' in conn_params:
                del conn_params['autocommit']
            if settings_dict['USER']:
                conn_params['user'] = settings_dict['USER']
            if settings_dict['PASSWORD']:
                conn_params['password'] = settings_dict['PASSWORD']
            if settings_dict['HOST']:
                conn_params['host'] = settings_dict['HOST']
            if settings_dict['PORT']:
                conn_params['port'] = settings_dict['PORT']
            self.connection = Database.connect(address=conn_params['host'],port=int(conn_params['port']),user=conn_params['user'],password=conn_params['password'])
            self.connection.setautocommit(auto=True)
            self.default_schema=settings_dict['NAME']
            # make it upper case
            self.default_schema=self.default_schema.upper()
            logger.info('DB Connected')
        cursor = self.connection.cursor()
        self.create_or_set_default_schema(cursor)
        logger.info('Cursor created')
        return cursor

    def cursor(self):
        self.validate_thread_sharing()
        if (self.use_debug_cursor or
            (self.use_debug_cursor is None and settings.DEBUG)):
            cursor = self.make_debug_cursor(self._cursor())
        else:
            cursor = CursorWrapper(self._cursor(), self)
        return cursor

    def make_debug_cursor(self, cursor):
        return CursorDebugWrapper(cursor, self)


    def create_or_set_default_schema(self,cursor):
        """
            create if doesn't exist and then make it default
        """
        cursor.execute("select (1) as a from schemas where schema_name='%s'" % self.default_schema)
        res=cursor.fetchone()
        if not res:
            cursor.execute("create schema %s" % self.default_schema)
        cursor.execute("set schema "+self.default_schema)
    
    def _enter_transaction_management(self, managed):
        """
        Switch the isolation level when needing transaction support, so that
        the same transaction is visible across all the queries.
        """
        if self.features.uses_autocommit and managed:
            self.connection.setautocommit(auto=False);

    def _leave_transaction_management(self, managed):
        """
        If the normal operating mode is "autocommit", switch back to that when
        leaving transaction management.
        """
        if self.features.uses_autocommit and not managed:
            self.connection.setautocommit(auto=True);

    def _commit(self):
        if self.connection is not None:
            try:
                return self.connection.commit()
            except Database.IntegrityError as e:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*tuple(e.args)), sys.exc_info()[2])
