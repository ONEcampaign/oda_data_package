from __future__ import annotations

from oda_data.api.constants import CURRENCIES


def validate_providers(
    providers: list | int | None, as_int: bool = False
) -> list | None:
    """Validate the providers parameter."""
    if as_int:
        return check_integers(providers) if providers else None
    return check_strings(providers) if providers else None


def validate_recipients(recipients: list | int | None) -> list[int] | None:
    """Validate the recipients parameter."""
    return check_integers(recipients) if recipients else None


def validate_currency(currency: str) -> str:
    """Validate the currency parameter."""
    if currency not in CURRENCIES:
        raise ValueError(f"Currency {currency} is not supported")
    return currency


def validate_measure(measure: list | str) -> list:
    """Validate the measure parameter."""
    return [measure] if isinstance(measure, str) else measure


def validate_years_providers_recipients(
    years: list[int] | int | range | None,
    providers: list | int | None,
    recipients: list | int | None,
) -> tuple:
    years = check_integers(years)
    providers = validate_providers(providers, as_int=True)
    recipients = validate_recipients(recipients)

    return years, providers, recipients


def validate_input_parameters(
    providers: list | int | None,
    recipients: list | int | None,
    currency: str,
    measure: list | str,
    prices: str,
    base_year: int | None,
) -> tuple:
    """Validate the input parameters."""
    providers = validate_providers(providers)
    recipients = validate_recipients(recipients)
    measure = validate_measure(measure)
    validate_currency(currency)

    return providers, recipients, measure


def _checktype(values: list | int | float, type_: type) -> list:
    """Take a list, int or float and return a list of integers."""

    if isinstance(values, list):
        return [type_(d) for d in values]
    elif isinstance(values, str | float | int):
        return [type_(values)]
    else:
        raise ValueError("Invalid values passed. Please check the type and try again.")


def check_integers(values: list | int | range | None) -> list[int] | None:
    """Take a list or int and return a list of integers."""
    if values is None:
        return None

    if isinstance(values, range):
        return list(values)

    return _checktype(values, int)


def check_strings(values: list | int | str | None) -> list[str] | None:
    """Take a list or int and return a list of strings.

    Falsy inputs (None, empty containers, 0, False, "") are returned unchanged
    (a long-standing contract — see test_check_strings_with_false_returns_false).
    Callers (validate_providers/recipients) only invoke this on truthy values, so
    the falsy branch is unreachable from them; the broad falsy return is typed
    away here rather than widening every caller's signature.
    """
    if not values:
        return values  # ty: ignore[invalid-return-type]

    if isinstance(values, range):
        return [str(i) for i in list(values)]

    if isinstance(values, str):
        return [values]

    if isinstance(values, int):
        return [str(values)]

    return [str(i) for i in values]
