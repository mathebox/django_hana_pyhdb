from __future__ import unicode_literals

import django


def createPlaceholder(compiler, field, val):
    if django.VERSION >= (1, 9):
        return compiler.field_as_sql(field, val)[0]

    return compiler.placeholder(field, val)
