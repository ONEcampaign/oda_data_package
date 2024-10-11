from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def _get_driver() -> webdriver.chrome:
    """Get driver for Chrome. A folder name must be provided to save the files to.

    Returns:
        A webdriver.chrome object.

    """

    # Create options
    options = webdriver.ChromeOptions()

    # Add arguments and options to options
    options.add_argument("--no-sandbox")
    options.add_argument("headless")

    # Get driver
    chrome = ChromeDriverManager().install()

    # Return driver with the options
    return webdriver.Chrome(service=Service(chrome), options=options)


def get_url_selenium(url: str) -> webdriver.chrome:
    """
    Get the url using selenium

    Args:
        url: The URL to fetch the file from.

    Returns:
        A webdriver.chrome object.

    """
    # get driver
    driver = _get_driver()

    # Get page
    driver.get(url)

    return driver


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


def check_integers(values: list | int) -> list[int]:
    """Take a list or int and return a list of integers."""
    if isinstance(values, range):
        return list(values)

    return _checktype(values, int)


def resolve_crs_year_name(year: int) -> tuple[int, int | str]:
    name = year
    if year in [2004, 2005]:
        year = 2004
        name = "2004-05"
    if year in [2002, 2003]:
        year = 2002
        name = "2002-03"
    if year in [2000, 2001]:
        year = 2000
        name = "2000-01"
    if year in range(1995, 2000):
        year = 1995
        name = "1995-99"
    if year in range(1973, 1995):
        year = 1973
        name = "1973-94"

    return year, name
