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
from django.db.backends.hana.version import get_version
from django.db.backends.hana.introspection import DatabaseIntrospection
#from django.utils.encoding import force_str
from django.utils.safestring import SafeText, SafeBytes
from django.utils import six
from django.db.backends.util import CursorWrapper
from django.utils.timezone import utc

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

class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'saphana'
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
	    #create/set default schema
            logger.info('DB Connected')
        cursor = self.connection.cursor()
	self.create_or_set_default_schema(cursor)
        logger.info('Cursor created')
        return cursor

    def create_or_set_default_schema(self,cursor):
#	cursor.execute("create schema "+self.default_schema)
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
