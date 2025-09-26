from oda_data.api.constants import CURRENCIES


def validate_providers(providers: list | int | None, as_int: bool = False) -> list:
    """Validate the providers parameter."""
    if as_int:
        return check_integers(providers) if providers else None
    return check_strings(providers) if providers else None


def validate_recipients(recipients: list | int | None) -> list:
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
    years: list[int] | int | range,
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
    elif isinstance(values, str):
        return [type_(values)]
    elif isinstance(values, float):
        return [type_(values)]
    elif isinstance(values, int):
        return [type_(values)]
    else:
        raise ValueError("Invalid values passed. Please check the type and try again.")


def check_integers(values: list | int | None) -> list[int] | None:
    """Take a list or int and return a list of integers."""
    if values is None:
        return

    if isinstance(values, range):
        return list(values)

    return _checktype(values, int)


def check_strings(values: list | int | str) -> list[str]:
    """Take a list or int and return a list of integers."""
    if not values:
        return values

    if isinstance(values, range):
        return [str(i) for i in list(values)]

    if isinstance(values, str):
        return [values]

    if isinstance(values, int):
        return [str(values)]

    return [str(i) for i in values]
