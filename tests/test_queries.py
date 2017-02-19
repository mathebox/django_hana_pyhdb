import unittest

from django.db import connection, models
from mock import call

from django_hana.base import Database

from .mock_db import mock_hana, patch_db_execute, patch_db_executemany, patch_db_fetchmany, patch_db_fetchone
from .models import ComplexModel, SimpleModel


class DatabaseConnectionMixin(object):
    maxDiff = None

    @mock_hana
    @patch_db_execute
    @patch_db_fetchone
    def setUp(self, mock_fetchone, mock_execute):
        connection.ensure_connection()


class TestSetup(DatabaseConnectionMixin, unittest.TestCase):
    @mock_hana
    @patch_db_execute
    def test_create_table(self, mock_execute):
        expected_statements = [
            call(
                'CREATE COLUMN TABLE "TEST_DHP_COMPLEXMODEL" '
                '("ID" integer NOT NULL PRIMARY KEY, '
                '"CHAR_FIELD" nvarchar(100) NOT NULL, '
                '"TEXT_FIELD" nclob NOT NULL'
                ')',
                None
            ),
            call(
                'CREATE SEQUENCE "TEST_DHP_COMPLEXMODEL_ID_SEQ" '
                'RESET BY SELECT IFNULL(MAX("ID"),0) + 1 FROM "TEST_DHP_COMPLEXMODEL"',
                []
            ),
        ]

        with connection.schema_editor() as editor:
            editor.create_model(ComplexModel)
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_execute
    def test_add_column_default_value(self, mock_execute):
        expected_statements = [
            call(
                'ALTER TABLE "TEST_DHP_SIMPLEMODEL" '
                'ADD ("NEW_CHAR_FIELD" nvarchar(50) NOT NULL)',
                []
            ),
        ]

        with connection.schema_editor() as editor:
            field = models.CharField(max_length=50, default='default_value')
            field.set_attributes_from_name('new_char_field')
            editor.add_field(SimpleModel, field)
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)


class TestCreation(DatabaseConnectionMixin, unittest.TestCase):
    @mock_hana
    @patch_db_execute
    @patch_db_fetchone
    def test_insert_object(self, mock_fetchone, mock_execute):
        expected_field_names = [
            '"CHAR_FIELD"',
            '"TEXT_FIELD"',
        ]
        expected_statements = [
            call(
                'INSERT INTO "TEST_DHP_COMPLEXMODEL" (id,%(field_names)s) '
                'VALUES (test_dhp_complexmodel_id_seq.nextval, %(param_placeholders)s)' % {
                    'field_names': ', '.join(expected_field_names),
                    'param_placeholders': ', '.join('?' * len(expected_field_names)),
                },
                [
                    'foobar',
                    'some long text',
                ]
            ),
            call('select test_dhp_complexmodel_id_seq.currval from dummy', ()),
        ]
        mock_fetchone.side_effect = [[1]]

        ComplexModel.objects.create(
            char_field='foobar',
            text_field='some long text',
        )
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_executemany
    @patch_db_fetchone
    def test_insert_objects(self, mock_fetchone, mock_execute):
        expected_statements = [
            call(
                'INSERT INTO "TEST_DHP_SIMPLEMODEL" (id,"CHAR_FIELD") '
                'VALUES (test_dhp_simplemodel_id_seq.nextval, ?)',
                (['foobar'], ['barbaz'])
            ),
        ]
        mock_fetchone.side_effect = [[1]]

        SimpleModel.objects.bulk_create([
            SimpleModel(char_field='foobar'),
            SimpleModel(char_field='barbaz'),
        ])

        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)


class TestSelection(DatabaseConnectionMixin, unittest.TestCase):
    @mock_hana
    @patch_db_execute
    @patch_db_fetchmany
    def test_select_model(self, mock_fetchmany, mock_execute):
        expected_field_names = [
            '"TEST_DHP_COMPLEXMODEL"."CHAR_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."TEXT_FIELD"',
        ]
        expected_statements = [
            call(
                'SELECT "TEST_DHP_COMPLEXMODEL"."ID", %(field_names)s FROM "TEST_DHP_COMPLEXMODEL"' % {
                    'field_names': ', '.join(expected_field_names),
                }, ()),
        ]
        mock_fetchmany.side_effect = [
            [
                (
                    1234,
                    'foobar',
                    Database.NClob('bazbaz'),
                ),
            ],
        ]

        objects = list(ComplexModel.objects.all())  # trigger database query with list()
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].id, 1234)
        self.assertEqual(objects[0].char_field, 'foobar')
        self.assertEqual(objects[0].text_field, 'bazbaz')
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)


class TestAggregation(DatabaseConnectionMixin, unittest.TestCase):
    @mock_hana
    @patch_db_execute
    @patch_db_fetchone
    def test_aggregate(self, mock_fetchone, mock_execute):
        expected_statements = [
            call(
                'SELECT COUNT("TEST_DHP_SIMPLEMODEL"."CHAR_FIELD") AS "CHAR_FIELD__COUNT" '
                'FROM "TEST_DHP_SIMPLEMODEL"',
                ()
            )
        ]
        field_count = 1
        mock_fetchone.side_effect = [[field_count]]

        data = SimpleModel.objects.all().aggregate(models.Count('char_field'))

        self.assertIn('char_field__count', data)
        self.assertEqual(data['char_field__count'], field_count)
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_execute
    @patch_db_fetchmany
    def test_annotate(self, mock_fetchmany, mock_execute):
        expected_statements = [
            call(
                'SELECT "TEST_DHP_SIMPLEMODEL"."ID", "TEST_DHP_SIMPLEMODEL"."CHAR_FIELD", '
                'COUNT("TEST_DHP_SIMPLEMODEL"."CHAR_FIELD") AS "NUM_FIELDS" '
                'FROM "TEST_DHP_SIMPLEMODEL" '
                'GROUP BY "TEST_DHP_SIMPLEMODEL"."ID", "TEST_DHP_SIMPLEMODEL"."CHAR_FIELD"',
                ()
            ),
        ]
        mock_fetchmany.side_effect = [[]]  # return empty list

        qs = SimpleModel.objects.annotate(num_fields=models.Count('char_field'))
        list(qs)  # trigger database query with list()
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_execute
    @patch_db_fetchone
    def test_annotate_aggreate(self, mock_fetchone, mock_execute):
        expected_statements = [
            call(
                'SELECT SUM("NUM_FIELDS") '
                'FROM (SELECT "TEST_DHP_SIMPLEMODEL"."ID" AS Col1, '
                'COUNT("TEST_DHP_SIMPLEMODEL"."CHAR_FIELD") AS "NUM_FIELDS" '
                'FROM "TEST_DHP_SIMPLEMODEL" '
                'GROUP BY "TEST_DHP_SIMPLEMODEL"."ID") subquery',
                ()
            ),
        ]
        num_fields = 1
        mock_fetchone.side_effect = [[num_fields]]

        data = SimpleModel.objects.annotate(num_fields=models.Count('char_field')).aggregate(models.Sum('num_fields'))

        self.assertIn('num_fields__sum', data)
        self.assertEqual(data['num_fields__sum'], num_fields)
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)
