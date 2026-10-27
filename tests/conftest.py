"""Shared pytest configuration."""

import os

import pytest


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo) -> object:
    """Fail a skipped ``data`` test when ``ODA_DATA_REQUIRE_DATA=1``.

    A warm cache is a precondition of the data tests, so a skip there means the
    gate did not run.
    """
    outcome = yield
    report = outcome.get_result()
    if (
        os.environ.get("ODA_DATA_REQUIRE_DATA") == "1"
        and report.skipped
        and item.get_closest_marker("data") is not None
    ):
        report.outcome = "failed"
        report.longrepr = "data test skipped while ODA_DATA_REQUIRE_DATA=1"
