import mock


class MockCursor(object):
    def execute(self, sql, params=()):
        raise NotImplementedError('Unexpected call to "execute". You need to use "patch_db_execute".')

    def executemany(self, sql, param_list):
        raise NotImplementedError('Unexpected call to "executemany". You need to use "patch_db_executemany".')

    def fetchone(self):
        raise NotImplementedError('Unexpected call to "fetchone". You need to use "patch_db_fetchone".')

    def fetchmany(self, count):
        raise NotImplementedError('Unexpected call to "fetchmany". You need to use "patch_db_fetchmany".')

    def fetchall(self):
        raise NotImplementedError('Unexpected call to "fetchall". You need to use "patch_db_fetchall".')

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
patch_db_executemany = mock.patch.object(MockCursor, 'executemany')
patch_db_fetchone = mock.patch.object(MockCursor, 'fetchone')
patch_db_fetchmany = mock.patch.object(MockCursor, 'fetchmany')
patch_db_fetchall = mock.patch.object(MockCursor, 'fetchall')
