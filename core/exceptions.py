class Error(BaseException):
    """Base class to inherit from"""
    pass


class InvalidDataType(Error):
    """Raised when invalid data type is passed"""
    pass


class InvalidInput(Error):
    """Raised when method was provided with incorrect data"""
    pass

