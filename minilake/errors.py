class MiniLakeError(Exception):
    """Base exception for expected MiniLake failures."""


class ConfigurationError(MiniLakeError):
    """Raised when a supplied configuration is invalid."""


class DataValidationError(MiniLakeError):
    """Raised when source data does not match the event contract."""


class OptionalDependencyError(MiniLakeError):
    """Raised when a cloud command needs an uninstalled optional dependency."""

