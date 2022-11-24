import logging
from oda_data.config import OdaPATHS

# Create a root logger
logger = logging.getLogger(__name__)

# Create two handlers (terminal and file)
shell_handler = logging.StreamHandler()
file_handler = logging.FileHandler(f"{OdaPATHS.logs}/scripts_log.log")

# Set levels for the logger, shell and file
logger.setLevel(logging.DEBUG)
shell_handler.setLevel(logging.DEBUG)
file_handler.setLevel(logging.INFO)

# Format the outputs
fmt_file = "%(levelname)s (%(asctime)s): %(message)s"
fmt_shell = (
    "%(levelname)s %(asctime)s [%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
)

# Create formatters
shell_formatter = logging.Formatter(fmt_shell)
file_formatter = logging.Formatter(fmt_file)

# Add formatters to handlers
shell_handler.setFormatter(shell_formatter)
file_handler.setFormatter(file_formatter)

# Add handlers to the logger
logger.addHandler(shell_handler)
logger.addHandler(file_handler)
