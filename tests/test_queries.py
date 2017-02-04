import unittest

from django.core.management.color import no_style
from django.db import connection

from .models import TestModel


class DatabaseSQLMixin(object):
    def remove_whitespace(self, statement):
        return ' '.join(str(statement).replace('\n', '').split())

    def assertQueryEqual(self, query1, query2):
        self.assertEqual(self.remove_whitespace(query1), self.remove_whitespace(query2))


class TestCreation(DatabaseSQLMixin, unittest.TestCase):
    style = no_style()

    def test_create_model(self):
        expected_statements = (
            'CREATE COLUMN TABLE "TEST_DHP_TESTMODEL" ( '
            '"ID" integer NOT NULL PRIMARY KEY, "FIELD" nvarchar(100) NOT NULL)',
            'CREATE SEQUENCE "TEST_DHP_TESTMODEL_ID_SEQ" '
            'RESET BY SELECT IFNULL(MAX("ID"),0) + 1 FROM "TEST_DHP_TESTMODEL"',
        )

        create_statements, _ = connection.creation.sql_create_model(TestModel, self.style, [])
        self.assertEqual(len(create_statements), len(expected_statements))
        for statement, expected in zip(create_statements, expected_statements):
            self.assertQueryEqual(statement, expected)


class TestSelection(DatabaseSQLMixin, unittest.TestCase):

    def test_create_model(self):
        expected = (
            'SELECT "TEST_DHP_TESTMODEL"."ID", "TEST_DHP_TESTMODEL"."FIELD" '
            'FROM "TEST_DHP_TESTMODEL"'
        )

        qs = TestModel.objects.all()
        self.assertQueryEqual(qs.query, expected)
