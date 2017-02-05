# REGISTER
MODEL_STORE = {}


# Model class decorators
def column_store(klass):
    """Register model use HANA's column store"""
    MODEL_STORE[klass.__name__] = 'COLUMN'
    return klass


def row_store(klass):
    """Register model use HANA's column store"""
    MODEL_STORE[klass.__name__] = 'ROW'
    return klass
