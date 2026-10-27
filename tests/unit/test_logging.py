import importlib
import logging
import subprocess
import sys

import pytest

from oda_data.clean_data import dtypes

# Run in a fresh interpreter so loggers configured by other tests don't count.
_IMPORT_CHECK = """
import logging
import sys

import oda_data
import oda_data.api.oecd
import oda_data.api.sources
import oda_data.cache.api
import oda_data.cache.config
import oda_data.clean_data.channels
import oda_data.clean_data.common
import oda_data.clean_data.dtypes
import oda_data.tools.cache

assert "oda_data.logger" not in sys.modules
loggers = {
    name: logger
    for name, logger in logging.root.manager.loggerDict.items()
    if name.split(".")[0] == "oda_data" and isinstance(logger, logging.Logger)
}
assert "oda_data.clean_data.dtypes" in loggers
for name, logger in loggers.items():
    assert logger.handlers == [], name
    assert logger.level == logging.NOTSET, name
    assert logger.propagate, name
"""


def test_import_leaves_logging_configuration_to_the_application():
    """Importing oda_data sets no handlers or levels, and its loggers propagate."""
    result = subprocess.run(
        [sys.executable, "-c", _IMPORT_CHECK],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr


def test_module_records_reach_the_root_logger(caplog):
    """A record logged by an oda_data module reaches a handler on the root logger."""
    with caplog.at_level(logging.INFO):
        dtypes.logger.info("reaches root")

    assert [(record.name, record.message) for record in caplog.records] == [
        ("oda_data.clean_data.dtypes", "reaches root")
    ]


def test_logger_module_is_deprecated(monkeypatch):
    """``oda_data.logger`` still imports, warns, and points at the package logger."""
    monkeypatch.delitem(sys.modules, "oda_data.logger", raising=False)

    with pytest.warns(DeprecationWarning, match=r"oda_data\.logger.*3\.0"):
        module = importlib.import_module("oda_data.logger")

    assert module.logger is logging.getLogger("oda_data")


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_setup_logger_still_configures_the_named_logger():
    """``setup_logger`` keeps attaching an INFO handler and stopping propagation."""
    from oda_data.logger import setup_logger

    name = "oda_data_setup_logger_test"
    logger = setup_logger(name)
    try:
        assert logger is logging.getLogger(name)
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1
        assert not logger.propagate
    finally:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        logger.setLevel(logging.NOTSET)
        logger.propagate = True
