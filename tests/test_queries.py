import datetime
import decimal
import unittest
import uuid

from django.db import connection, models
from django.db.models.fields.files import FieldFile
from mock import call

from django_hana.base import Database

from .mock_db import mock_hana, patch_db_execute, patch_db_executemany, patch_db_fetchmany, patch_db_fetchone
from .models import ComplexModel, SimpleColumnModel, SimpleModel, SimpleRowModel, SpatialModel


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
                '("ID" INTEGER NOT NULL PRIMARY KEY, '
                '"BIG_INTEGER_FIELD" BIGINT NOT NULL, '
                '"BINARY_FIELD" BLOB NOT NULL, '
                '"BOOLEAN_FIELD" TINYINT NOT NULL, '
                '"CHAR_FIELD" NVARCHAR(100) NOT NULL, '
                '"DATE_FIELD" DATE NOT NULL, '
                '"DATE_TIME_FIELD" TIMESTAMP NOT NULL, '
                '"DECIMAL_FIELD" DECIMAL(5, 2) NOT NULL, '
                '"DURATION_FIELD" BIGINT NOT NULL, '
                '"EMAIL_FIELD" NVARCHAR(254) NOT NULL, '
                '"FILE_FIELD" NVARCHAR(100) NOT NULL, '
                '"FILE_PATH_FIELD" NVARCHAR(100) NOT NULL, '
                '"FLOAT_FIELD" FLOAT NOT NULL, '
                '"IMAGE_FIELD" NVARCHAR(100) NOT NULL, '
                '"INTEGER_FIELD" INTEGER NOT NULL, '
                '"GENERIC_IP_ADDRESS_FIELD" NVARCHAR(39) NOT NULL, '
                '"NULL_BOOLEAN_FIELD" TINYINT NULL, '
                '"POSITIVE_INTEGER_FIELD" INTEGER NOT NULL, '
                '"POSITIVE_SMALL_INTEGER_FIELD" SMALLINT NOT NULL, '
                '"SLUG_FIELD" NVARCHAR(50) NOT NULL, '
                '"SMALL_INTEGER_FIELD" SMALLINT NOT NULL, '
                '"TEXT_FIELD" NCLOB NOT NULL, '
                '"TIME_FIELD" TIME NOT NULL, '
                '"URL_FIELD" NVARCHAR(200) NOT NULL, '
                '"UUID_FIELD" NVARCHAR(32) NOT NULL'
                ')',
                None
            ),
            call(
                'CREATE SEQUENCE "TEST_DHP_COMPLEXMODEL_ID_SEQ" '
                'RESET BY SELECT IFNULL(MAX("ID"),0) + 1 FROM "TEST_DHP_COMPLEXMODEL"',
                []
            ),
            call('CREATE INDEX "TEST_DHP_COMPLEXMODEL_D7C9D0CA" ON "TEST_DHP_COMPLEXMODEL" ("SLUG_FIELD")', []),
        ]

        with connection.schema_editor() as editor:
            editor.create_model(ComplexModel)
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_execute
    def test_create_column_table(self, mock_execute):
        expected_statements = [
            call(
                'CREATE COLUMN TABLE "TEST_DHP_SIMPLECOLUMNMODEL" '
                '("ID" INTEGER NOT NULL PRIMARY KEY, "CHAR_FIELD" NVARCHAR(50) NOT NULL)',
                None
            ),
            call(
                'CREATE SEQUENCE "TEST_DHP_SIMPLECOLUMNMODEL_ID_SEQ" '
                'RESET BY SELECT IFNULL(MAX("ID"),0) + 1 FROM "TEST_DHP_SIMPLECOLUMNMODEL"',
                []
            ),
        ]

        with connection.schema_editor() as editor:
            editor.create_model(SimpleColumnModel)
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_execute
    def test_create_row_table(self, mock_execute):
        expected_statements = [
            call(
                'CREATE ROW TABLE "TEST_DHP_SIMPLEROWMODEL" '
                '("ID" INTEGER NOT NULL PRIMARY KEY, "CHAR_FIELD" NVARCHAR(50) NOT NULL)',
                None
            ),
            call(
                'CREATE SEQUENCE "TEST_DHP_SIMPLEROWMODEL_ID_SEQ" '
                'RESET BY SELECT IFNULL(MAX("ID"),0) + 1 FROM "TEST_DHP_SIMPLEROWMODEL"',
                []
            ),
        ]

        with connection.schema_editor() as editor:
            editor.create_model(SimpleRowModel)
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_execute
    def test_create_row_table(self, mock_execute):
        expected_statements = [
            call(
                'CREATE COLUMN TABLE "TEST_DHP_SPATIALMODEL" ("ID" INTEGER NOT NULL PRIMARY KEY, '
                '"POINT_FIELD" ST_POINT NOT NULL, '
                '"LINE_STRING_FIELD" ST_GEOMETRY NOT NULL, '
                '"POLYGON_FIELD" ST_GEOMETRY NOT NULL, '
                '"MULTI_POINT_FIELD" ST_GEOMETRY NOT NULL, '
                '"MULTI_LINE_STRING_FIELD" ST_GEOMETRY NOT NULL, '
                '"MULTI_PLOYGON_FIELD" ST_GEOMETRY NOT NULL'
                ')',
                None
            ),
            call(
                'CREATE SEQUENCE "TEST_DHP_SPATIALMODEL_ID_SEQ" '
                'RESET BY SELECT IFNULL(MAX("ID"),0) + 1 FROM "TEST_DHP_SPATIALMODEL"',
                []
            ),
        ]

        with connection.schema_editor() as editor:
            editor.create_model(SpatialModel)
        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_execute
    def test_add_column_default_value(self, mock_execute):
        expected_statements = [
            call(
                'ALTER TABLE "TEST_DHP_SIMPLEMODEL" '
                'ADD ("NEW_CHAR_FIELD" NVARCHAR(50) NOT NULL)',
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
            '"BIG_INTEGER_FIELD"',
            '"BINARY_FIELD"',
            '"BOOLEAN_FIELD"',
            '"CHAR_FIELD"',
            '"DATE_FIELD"',
            '"DATE_TIME_FIELD"',
            '"DECIMAL_FIELD"',
            '"DURATION_FIELD"',
            '"EMAIL_FIELD"',
            '"FILE_FIELD"',
            '"FILE_PATH_FIELD"',
            '"FLOAT_FIELD"',
            '"IMAGE_FIELD"',
            '"INTEGER_FIELD"',
            '"GENERIC_IP_ADDRESS_FIELD"',
            '"NULL_BOOLEAN_FIELD"',
            '"POSITIVE_INTEGER_FIELD"',
            '"POSITIVE_SMALL_INTEGER_FIELD"',
            '"SLUG_FIELD"',
            '"SMALL_INTEGER_FIELD"',
            '"TEXT_FIELD"',
            '"TIME_FIELD"',
            '"URL_FIELD"',
            '"UUID_FIELD"',
        ]
        expected_statements = [
            call(
                'INSERT INTO "TEST_DHP_COMPLEXMODEL" (id,%(field_names)s) '
                'VALUES (test_dhp_complexmodel_id_seq.nextval, %(param_placeholders)s)' % {
                    'field_names': ', '.join(expected_field_names),
                    'param_placeholders': ', '.join('?' * len(expected_field_names)),
                },
                [
                    9223372036854775807,
                    Database.Blob(b'foobar'),
                    0,
                    'foobar',
                    '2017-01-01',
                    '2017-01-01 13:45:21',
                    123.45,
                    1234567890,
                    'foo@foobar.com',
                    'uploads/foobar.txt',
                    'uploads/barbaz.txt',
                    12.34567,
                    'uploads/image.png',
                    -2147483648,
                    '192.0.2.30',
                    None,
                    2147483647,
                    32767,
                    'something-foobar-1234',
                    -32768,
                    'some long text',
                    '13:45:21',
                    'https://foo.bar.com/baz/',
                    '12345678123456781234567812345678',
                ]
            ),
            call('select test_dhp_complexmodel_id_seq.currval from dummy', ()),
        ]
        mock_fetchone.side_effect = [[1]]

        ComplexModel.objects.create(
            big_integer_field=9223372036854775807,
            binary_field=b'foobar',
            boolean_field=False,
            char_field='foobar',
            date_field=datetime.date(2017, 1, 1),
            date_time_field=datetime.datetime(2017, 1, 1, 13, 45, 21),
            decimal_field=123.45,
            duration_field=datetime.timedelta(microseconds=1234567890),
            email_field='foo@foobar.com',
            file_field='uploads/foobar.txt',
            file_path_field='uploads/barbaz.txt',
            float_field=12.34567,
            image_field='uploads/image.png',
            integer_field=-2147483648,
            generic_ip_address_field='192.0.2.30',
            null_boolean_field=None,
            positive_integer_field=2147483647,
            positive_small_integer_field=32767,
            slug_field='something-foobar-1234',
            small_integer_field=-32768,
            text_field='some long text',
            time_field=datetime.time(13, 45, 21),
            url_field='https://foo.bar.com/baz/',
            uuid_field='12345678-12345678-12345678-12345678',
        )

        # Blob of pyhdb is not comparable. Therefore comparing `mock_execute.call_args_list` and `expected_statements`
        # will fail. We check the value of the binary file manually and patch the expected statement afterwards.
        first_call = mock_execute.call_args_list[0]
        (_, first_call_args), _ = first_call
        self.assertIsInstance(first_call_args[1], Database.Blob)
        self.assertEqual(first_call_args[1].read(), b'foobar')

        # Patch expected_statement
        first_expected_call = expected_statements[0]
        _, (_, first_expected_call_args), _ = first_expected_call
        first_expected_call_args[1] = first_call_args[1]

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
    valid_db_values = [
        1234,                                           # id
        9223372036854775807,                            # big integer
        Database.Blob(b'foobar'),                       # binary
        0,                                              # boolean
        'foobar',                                       # char
        datetime.date(2017, 1, 1),                      # date
        datetime.datetime(2017, 1, 1, 13, 45, 21),      # date time
        decimal.Decimal(123.45),                        # decimal
        1234567890,                                     # duration
        'foo@foobar.com',                               # email
        'uploads/foobar.txt',                           # file
        'uploads/barbaz.txt',                           # file path
        12.34567,                                       # float
        'uploads/image.png',                            # image
        -2147483648,                                    # integer
        '192.0.2.30',                                   # generic ip address
        None,                                           # null boolean
        2147483647,                                     # postive integer
        32767,                                          # positive small integer
        'something-foobar-1234',                        # slug
        -32768,                                         # small integer
        Database.NClob('some long text'),               # text
        datetime.time(13, 45, 21),                      # time
        'https://foo.bar.com/baz/',                     # url
        '12345678-12345678-12345678-12345678',          # uuid
    ]

    @mock_hana
    @patch_db_execute
    @patch_db_fetchmany
    def test_select_model(self, mock_fetchmany, mock_execute):
        expected_field_names = [
            '"TEST_DHP_COMPLEXMODEL"."BIG_INTEGER_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."BINARY_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."BOOLEAN_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."CHAR_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."DATE_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."DATE_TIME_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."DECIMAL_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."DURATION_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."EMAIL_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."FILE_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."FILE_PATH_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."FLOAT_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."IMAGE_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."INTEGER_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."GENERIC_IP_ADDRESS_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."NULL_BOOLEAN_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."POSITIVE_INTEGER_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."POSITIVE_SMALL_INTEGER_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."SLUG_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."SMALL_INTEGER_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."TEXT_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."TIME_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."URL_FIELD"',
            '"TEST_DHP_COMPLEXMODEL"."UUID_FIELD"',
        ]
        expected_statements = [
            call(
                'SELECT "TEST_DHP_COMPLEXMODEL"."ID", %(field_names)s FROM "TEST_DHP_COMPLEXMODEL"' % {
                    'field_names': ', '.join(expected_field_names),
                }, ()),
        ]
        mock_fetchmany.side_effect = [
            [
                tuple(self.valid_db_values),
            ],
        ]

        objects = list(ComplexModel.objects.all())  # trigger database query with list()
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].id, 1234)
        self.assertEqual(objects[0].big_integer_field, 9223372036854775807)
        self.assertEqual(objects[0].binary_field, b'foobar')
        self.assertEqual(objects[0].boolean_field, False)
        self.assertEqual(objects[0].char_field, 'foobar')
        self.assertEqual(objects[0].date_field, datetime.date(2017, 1, 1))
        self.assertEqual(objects[0].date_time_field, datetime.datetime(2017, 1, 1, 13, 45, 21))
        self.assertEqual(objects[0].decimal_field, 123.45)
        self.assertEqual(objects[0].duration_field, datetime.timedelta(0, 1234, 567890))
        self.assertEqual(objects[0].email_field, 'foo@foobar.com')
        self.assertEqual(objects[0].file_field, FieldFile(objects[0], objects[0].file_field, 'uploads/foobar.txt'))
        self.assertEqual(objects[0].file_path_field, 'uploads/barbaz.txt')
        self.assertEqual(objects[0].float_field, 12.34567)
        self.assertEqual(objects[0].image_field, 'uploads/image.png')
        self.assertEqual(objects[0].integer_field, -2147483648)
        self.assertEqual(objects[0].generic_ip_address_field, '192.0.2.30')
        self.assertEqual(objects[0].null_boolean_field, None)
        self.assertEqual(objects[0].positive_integer_field, 2147483647)
        self.assertEqual(objects[0].positive_small_integer_field, 32767)
        self.assertEqual(objects[0].slug_field, 'something-foobar-1234')
        self.assertEqual(objects[0].small_integer_field, -32768)
        self.assertEqual(objects[0].text_field, 'some long text')
        self.assertEqual(objects[0].time_field, datetime.time(13, 45, 21))
        self.assertEqual(objects[0].url_field, 'https://foo.bar.com/baz/')
        self.assertEqual(objects[0].uuid_field, uuid.UUID('12345678-12345678-12345678-12345678'))

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
