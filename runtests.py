import os
import sys

import django

# from django.conf import settings
# from django.test.utils import get_runner


import unittest


def runtests():
    os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.test_settings'
    django.setup()
    # test_runner = get_runner(settings)()
    # failures = test_runner.run_tests(["tests"])
    # sys.exit(bool(failures))
    setup_file = sys.modules['__main__'].__file__
    setup_dir = os.path.abspath(os.path.dirname(setup_file))
    return unittest.defaultTestLoader.discover(setup_dir)


if __name__ == '__main__':
    runtests()
