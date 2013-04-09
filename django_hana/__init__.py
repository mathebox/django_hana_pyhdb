### REGISTER
COLUMN_STORE = {}
ROW_STORE = {}

### Model class decorators
def column_store(klass):
    """Register model use HANA's column store"""
    COLUMN_STORE[klass.__name__] = klass
    return klass

def row_store(klass):
    """Register model use HANA's column store"""
    ROW_STORE[klass.__name__] = klass
    return klass