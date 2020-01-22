"""Exceptions for ``cubi-sak``."""


class CubiSakWarning(Warning):
    """Base ``Warning`` class."""


class IrodsIcommandsUnavailableWarning(CubiSakWarning):
    """Raised when iRODS icommands are not available."""


class CubiSakException(Exception):
    """Base ``Exception`` class."""


class IrodsIcommandsUnavailableException(CubiSakException):
    """Raised when iRODS icommands are not available but required."""


class UnsupportedIsaTabFeaturException(CubiSakException):
    """Raised when an unsupported ISA-tab feature occurs."""


class MissingFileException(CubiSakException):
    """Raised on missing file for transfer."""
