import logging
from typing import Callable


class ErrorHandler:
    """
    Error handling utilities for logging and managing exceptions.

    This class provides a decorator to wrap functions with error handling,
    logging exceptions with specific error codes, and allowing them to be raised.
    """

    @staticmethod
    def handle_errors(func: Callable) -> Callable:
        """
        Decorator for handling and logging errors during function execution.

        This decorator logs the arguments and keyword arguments passed to the function,
        and catches common exceptions like FileNotFoundError and PermissionError (these are just example exceptions,
        this is extendable with adding new exceptions with their error codes),
        logging them with specific error codes. It raises the original exception after logging.

        Error codes can be used to analyse pattern and distribution of each error in the system.

        param func: The function to decorate.
        return: The wrapped function with error handling.
        """
        def wrapper(*args, **kwargs):
            try:
                arg_str = ", ".join([repr(arg) for arg in args])
                kwarg_str = ", ".join([f"{key}={repr(value)}" for key, value in kwargs.items()])
                logging.info(f"Calling {func.__name__} with args: {arg_str}, kwargs: {kwarg_str}")
                return func(*args, **kwargs)
            except FileNotFoundError as err:
                logging.error(f"{err}, {func.__name__}, ETL1002")
                raise
            except PermissionError as err:
                logging.error(f"{err}, {func.__name__}, ETL1003")
                raise
            except Exception as err:
                logging.error(f"{err}, {func.__name__}, ETL1001")
                raise

        return wrapper
