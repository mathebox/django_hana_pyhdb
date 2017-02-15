import mock


class MockCursor(object):
    def execute(self, sql, params=()):
        raise NotImplementedError('Unexpected call to "execute". You need to use "patch_db_execute".')

    def executemany(self, sql, param_list):
        raise NotImplementedError()

    def fetchone(self):
        return (None,)

    def fetchmany(self, count):
        return []

    def fetchall(self):
        return []

    def close(self):
        pass


class MockConnection(object):
    autocommit = False
    closed = False

    def setautocommit(self, autocommit):
        self.autocommit = autocommit

    def commit(self):
        pass

    def close(self):
        pass

    def cursor(self):
        return MockCursor()

    def rollback(self):
        return


def mock_connect(*args, **kwargs):
    return MockConnection()


mock_hana = mock.patch('pyhdb.connect', mock_connect)
patch_db_execute = mock.patch.object(MockCursor, 'execute')
