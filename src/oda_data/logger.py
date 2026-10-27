"""Deprecated until 3.0: configure logging.getLogger("oda_data") instead."""

import logging
import warnings

warnings.warn(
    "oda_data.logger is deprecated and will be removed in 3.0. "
    "Configure logging.getLogger('oda_data') instead.",
    DeprecationWarning,
    stacklevel=2,
)

logger = logging.getLogger("oda_data")


def setup_logger(name: str) -> logging.Logger:
    """Attach an INFO stderr handler to the named logger and stop it propagating.

    Args:
        name: The name of the logger.

    Returns:
        The configured logger.
    """
    logger_ = logging.getLogger(name)
    logger_.setLevel(logging.INFO)

    if not logger_.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger_.addHandler(console_handler)

    logger_.propagate = False

    return logger_
