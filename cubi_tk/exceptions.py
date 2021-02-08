"""Exceptions for ``cubi-tk``."""


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


class OverwriteRefusedException(CubiSakException):
    """Raised when refusing to overwrite a file."""


class ResourceDownloadError(CubiSakException):
    """Raised when something went wrong with a file download error."""


class ParameterException(CubiSakException):
    """Raised in case of problems with parameterisation."""


class ParseOutputException(CubiSakException):
    """Problem with parsing output of sub command."""


class UserCanceledException(CubiSakException):
    """Raised when user doesn't allow the process to continue."""
