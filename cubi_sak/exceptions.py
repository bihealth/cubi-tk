"""Exceptions for ``cubi-sak``."""


class CubiSakWarning(Warning):
    """Base ``Warning`` class."""


class CubiSakException(Exception):
    """Base ``Exception`` class."""


class UnsupportedIsaTabFeaturException(CubiSakException):
    """Raised when an unsupported ISA-tab feature occurs."""
