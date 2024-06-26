from typing import Optional
import logging
from logging.handlers import RotatingFileHandler
from rich.console import Console
from rich.logging import RichHandler
import os


def get_logger(
    name: str,
    logfile_path: Optional[str] = None,
    to_console: bool = True,
    base_level: str = "DEBUG",
    format: str = "%(asctime)s  %(levelname)s  %(message)s",
    date_format: str = "%d/%m/%Y %H:%M:%S (%z)",
    file_mode: str = "a+",
) -> logging.Logger:
    """
    Get an already setup `logging.Logger` instance

    :param name: The name of the logger.
    :param logfile_path: The name or path of the log file to log messages into. It can be a relative or absolute path.
    If the file does not exist, it will be created.
    :param base_level: The base level for logging message. Defaults to "DEBUG".
    :param format: log message format. Defaults to "%(asctime)s - %(levelname)s - %(message)s".
    :param date_format: Log date format string. Defaults to "%d/%m/%Y %H:%M:%S (%Z)".
    :param file_mode: Log file write mode. Defaults to 'a+'.
    :return: `logging.Logger` instance
    """
    if not any((logfile_path, to_console)):
        raise ValueError(
            "At least one of `logfile_path` or `to_console` has to be provided."
        )

    if logfile_path and os.path.splitext(logfile_path)[-1].lower() != ".log":
        raise ValueError("Invalid extension type for log file")

    logger = logging.getLogger(name)
    if logfile_path is not None:
        file_handler = RotatingFileHandler(
            filename=logfile_path,
            mode=file_mode,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=10,
        )
        formatter = logging.Formatter(fmt=format, datefmt=date_format)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if to_console is True:
        error_console = Console(stderr=True, width=152)
        rich_handler = RichHandler(
            level=base_level,
            console=error_console,
            show_time=False,
            rich_tracebacks=True,
            markup=True,
            show_path=False,
        )
        formatter = logging.Formatter(
            fmt=format.replace("%(levelname)s", ""), datefmt=date_format
        )
        rich_handler.setFormatter(formatter)
        rich_handler.setLevel(base_level.upper())
        logger.addHandler(rich_handler)

    logger.setLevel(base_level.upper())
    return logger
