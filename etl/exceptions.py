"""Custom exceptions"""


class InvalidDataException(Exception):
    """Raised when data does not meet validation conditions"""

class DataParserError(Exception):
    """Raised when could not parse data"""
