class MissingValueError(ValueError):
    """Missing value exception"""

    pass


class DuplicateValueError(ValueError):
    """Raised when values meant to be unique are found duplicated"""

    pass


class IllegalValueError(ValueError):
    """Raised for illegal values"""

    pass
