"""Exceptions for ``cubi-sak``."""


class CubiSakWarning(Warning):
    """Base ``Warning`` class."""


class IrodsIcommandsUnavailableWarning(CubiSakWarning):
    """Raised when iRODS icommands are not available."""


class CubiSakException(Exception):
    """Base ``Exception`` class."""


class IrodsIcommandsUnavailableException(CubiSakException):
    """Raised when iRODS icommands are not available but required."""


class UnsupportedIsaTabFeatureException(CubiSakException):
    """Raised when an unsupported ISA-tab feature occurs."""


class InvalidIsaTabException(CubiSakException):
    """Raised when ISA-tab breaks an assumption."""


class MissingFileException(CubiSakException):
    """Raised on missing file for transfer."""


class ResourceDownloadError(CubiSakException):
    """Raised when something went wrong with a file download error."""
