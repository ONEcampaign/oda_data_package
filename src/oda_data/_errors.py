"""Errors and warnings.

Every error subclasses ``OdaDataError`` and, where one fits, a built-in
exception, so callers can catch by intent. Each class has a stable ``code``,
and each message ends with ``[<code>]``. Errors survive a pickle round trip
with their attributes.
"""

from collections.abc import Iterable
from typing import Any, ClassVar


def _restore(cls: type[BaseException], args: tuple[Any, ...], state: dict) -> Any:
    """Rebuild an error without running its ``__init__``, which would re-append the code."""
    error = cls.__new__(cls, *args)
    error.args = args
    error.__dict__.update(state)
    return error


class OdaDataError(Exception):
    """Base class of every oda-data error.

    Attributes:
        code: A stable machine-readable code, also the suffix of the message.
        message: The message without the code suffix.
    """

    code: ClassVar[str] = "oda_data_error"

    def __init__(self, message: str = "") -> None:
        self.message = message
        super().__init__(f"{message} [{self.code}]" if message else f"[{self.code}]")

    def __reduce__(self) -> tuple[Any, ...]:
        return (_restore, (type(self), self.args, self.__dict__.copy()))


class UnknownIndicatorError(OdaDataError, LookupError):
    """An indicator id does not exist. ``suggestions`` holds near matches.

    A 2.x code suggests the 3.0 call that gives the same number.
    """

    code = "unknown_indicator"

    def __init__(self, message: str = "", *, suggestions: Iterable[str] = ()) -> None:
        self.suggestions = tuple(suggestions)
        super().__init__(message)


class UnknownEntityError(OdaDataError, LookupError):
    """A code or name matched no entity.

    Attributes:
        suggestions: Near matches.
        system: The code system that was tried.
    """

    code = "unknown_entity"

    def __init__(
        self,
        message: str = "",
        *,
        suggestions: Iterable[str] = (),
        system: str | None = None,
    ) -> None:
        self.suggestions = tuple(suggestions)
        self.system = system
        super().__init__(message)


class AmbiguousEntityError(OdaDataError, ValueError):
    """A name could mean more than one entity, such as an OECD aggregate and a group.

    ``candidates`` holds the entities it could mean.
    """

    code = "ambiguous_entity"

    def __init__(self, message: str = "", *, candidates: Iterable[str] = ()) -> None:
        self.candidates = tuple(candidates)
        super().__init__(message)


class DimensionError(OdaDataError, ValueError):
    """An indicator has no such dimension. The message names an indicator that has it."""

    code = "dimension"


class BasisRangeError(OdaDataError, ValueError):
    """A basis was asked for outside its years. The message names the basis that covers them."""

    code = "basis_range"


class UnsupportedQueryError(OdaDataError, ValueError):
    """The call is not defined: ``unit=`` on a ratio, a currency oda-data cannot convert to, ``records()`` on a ratio, ``total()`` outside ``by=``, or a code the indicator leaves out."""

    code = "unsupported_query"


class MissingFactorError(OdaDataError):
    """A currency or price factor is missing. The message names the provider and year."""

    code = "missing_factor"


class DataUnavailableError(OdaDataError, ConnectionError):
    """oda-reader or pydeflate could not fetch the data. The cause is chained."""

    code = "data_unavailable"


class OfflineCacheMissError(OdaDataError):
    """The context is offline and the cache cannot answer the call."""

    code = "offline_cache_miss"


class ReleaseMismatchError(OdaDataError):
    """The local release of a table differs from the pinned release."""

    code = "release_mismatch"


class RemovedIn3Error(OdaDataError, AttributeError):
    """A 2.x name that 3.0 removed. The message gives the replacement."""

    code = "removed_in_3"


class ImputationConservationError(OdaDataError):
    """An imputation did not conserve the amount it spread."""

    code = "imputation_conservation"


class UnmappedChannelError(OdaDataError):
    """A multilateral provider and agency pair has no channel in the crosswalk."""

    code = "unmapped_channel"


class OdaDataWarning(UserWarning):
    """A caveat on a result, such as a basis that few members report."""


# Every class here is public and reports the package it is imported from.
for _cls in [c for c in globals().values() if isinstance(c, type)]:
    if _cls.__module__ == __name__:
        _cls.__module__ = "oda_data"
