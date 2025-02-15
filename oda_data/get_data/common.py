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


def check_integers(values: list | int | None) -> list[int] | None:
    """Take a list or int and return a list of integers."""
    if values is None:
        return

    if isinstance(values, range):
        return list(values)

    return _checktype(values, int)


def check_strings(values: list | int | str) -> list[str]:
    """Take a list or int and return a list of integers."""
    if isinstance(values, range):
        return [str(i) for i in list(values)]

    if isinstance(values, str):
        return [values]

    if isinstance(values, int):
        return [str(values)]

    return [str(i) for i in values]
