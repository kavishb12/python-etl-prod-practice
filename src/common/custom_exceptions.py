"""Custom Exceptions"""
class WrongFormatException(Exception):
    """ Can be raised when the file format is unavialable """

class WrongMetaFileException(Exception):
    """
    WrongMetaFileException class
    Exception that can be raised when the meta file
    format is not correct.
    """