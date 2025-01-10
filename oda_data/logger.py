import logging


def setup_logger(name: str) -> logging.Logger:
    """Set up the logger.

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: The logger.
    """
    logger_ = logging.getLogger(name)
    logger_.setLevel(logging.INFO)

    # Avoid adding multiple handlers
    if not logger_.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)

        logger_.addHandler(console_handler)

    # Disable propagation to the root logger
    logger_.propagate = False

    return logger_


logger = setup_logger("oda_data")
