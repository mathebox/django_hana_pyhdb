import os
import sys

from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = 'hdbsql'

    def runshell(self):
        settings_dict = self.connection.settings_dict
        args = [self.executable_name]
        if settings_dict['USER']:
            args += ['-u', settings_dict['USER']]
        if settings_dict['HOST']:
            args.extend(['-n', settings_dict['HOST'] + ':' + settings_dict['PORT']])
        if settings_dict['PASSWORD']:
            args.extend(['-p', str(settings_dict['PASSWORD'])])
        args.extend(['-S',  settings_dict['NAME']])
        if os.name == 'nt':
            sys.exit(os.system(' '.join(args)))
        else:
            os.execvp(self.executable_name, args)
