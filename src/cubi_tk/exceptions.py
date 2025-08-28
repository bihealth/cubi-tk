"""Exceptions for ``cubi-tk``."""


class CubiTkWarning(Warning):
    """Base ``Warning`` class."""


class IrodsIcommandsUnavailableWarning(CubiTkWarning):
    """Raised when iRODS icommands are not available."""


class CubiTkException(Exception):
    """Base ``Exception`` class."""


class IrodsIcommandsUnavailableException(CubiTkException):
    """Raised when iRODS icommands are not available but required."""


class UnsupportedIsaTabFeatureException(CubiTkException):
    """Raised when an unsupported ISA-tab feature occurs."""


class InvalidIsaTabException(CubiTkException):
    """Raised when ISA-tab breaks an assumption."""


class MissingFileException(CubiTkException):
    """Raised on missing file for transfer."""


class OverwriteRefusedException(CubiTkException):
    """Raised when refusing to overwrite a file."""


class ResourceDownloadError(CubiTkException):
    """Raised when something went wrong with a file download error."""


class ParameterException(CubiTkException):
    """Raised in case of problems with parameterisation."""


class ParseOutputException(CubiTkException):
    """Problem with parsing output of sub command."""


class UserCanceledException(CubiTkException):
    """Raised when user doesn't allow the process to continue."""


class InvalidReadmeException(CubiTkException):
    """Raised if the Readme does not meet the specification."""


class FileChecksumMismatchException(CubiTkException):
    """Raised if the recorded checksum for a file does not match the (re)computed value."""


class SodarApiException(CubiTkException):
    def __init__(self, status_code: int, *args):
        super().__init__(*args)
        self.status_code = status_code
    """Raised when the SODAR API does not return success."""
