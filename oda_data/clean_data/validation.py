from oda_data.api.constants import CURRENCIES
from oda_data.get_data.common import check_strings, check_integers


def validate_providers(providers: list | int | None) -> list:
    """Validate the providers parameter."""
    return check_strings(providers) if providers else None


def validate_recipients(recipients: list | int | None) -> list:
    """Validate the recipients parameter."""
    return check_integers(recipients) if recipients else None


def validate_currency(currency: str) -> None:
    """Validate the currency parameter."""
    if currency not in CURRENCIES:
        raise ValueError(f"Currency {currency} is not supported")


def validate_measure(measure: list | str) -> list:
    """Validate the measure parameter."""
    return [measure] if isinstance(measure, str) else measure


def validate_prices(prices: str) -> None:
    """Validate the prices parameter."""
    if prices not in ["current", "constant"]:
        raise ValueError(f"Prices must be 'current' or 'constant'")


def validate_base_year(base_year: int | None, prices: str) -> None:
    """Validate the base year parameter."""
    if base_year is not None and prices == "current":
        raise ValueError(f"Base year can only be specified if prices are constant")
    if base_year is None and prices == "constant":
        raise ValueError(f"Base year must be specified for constant prices")


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
    validate_prices(prices)
    validate_base_year(base_year=base_year, prices=prices)

    return providers, recipients, measure
