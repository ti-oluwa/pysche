from __future__ import annotations
from typing import Any, Optional, TextIO, final, Iterable, Coroutine, Union
import datetime
import sys

try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo
import random
import uuid
import asyncio

from .descriptors import null


def utcoffset_to_zoneinfo(offset: datetime.timedelta) -> zoneinfo.ZoneInfo:
    """
    Convert a UTC offset to a ZoneInfo object.

    :param offset: The UTC offset.
    :return: The ZoneInfo object.
    """
    # Calculate the UTC offset in seconds
    offset_seconds = offset.total_seconds() if offset is not None else 0
    # Generate a time zone name based on the UTC offset. If offset is positive
    # Then GMT is behind the timezone (GMT-) else GMT is ahead (GMT+)
    zone_name = (
        f"Etc/GMT{'-' if offset_seconds >= 0 else '+'}{int(abs(offset_seconds)//3600)}"
    )

    try:
        # Try to load the ZoneInfo object for the calculated time zone
        return zoneinfo.ZoneInfo(zone_name)
    except zoneinfo.ZoneInfoNotFoundError:
        raise ValueError(f"No ZoneInfo found for UTC offset {offset_seconds}")


def parse_time(time: str, tzinfo: datetime.tzinfo | zoneinfo.ZoneInfo) -> datetime.time:
    """
    Parse time string in format  "%H:%M" or "%H:%M:%S" to datetime.time object

    :param time: The time string.
    :param tzinfo: The timezone info. If not provided, the timezone of the machine is used.
    """
    if not isinstance(time, str):
        raise TypeError(f"Expected time to be of type str not {type(time).__name__}")

    split = time.split(":")
    length = len(split)
    if length != 3:
        if length == 2:
            split.append("00")
        else:
            raise ValueError("Time must be in the format, 'HH:MM' or 'HH:MM:SS'")

    hour, minute, second = split
    tzinfo = tzinfo or get_datetime_now().tzinfo
    return datetime.time(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        tzinfo=tzinfo,
    )


def construct_datetime_from_time(time: datetime.time) -> datetime.datetime:
    """
    Return the current date plus the specified time.
    The datetime object is converted to the timezone of the datetime.time object provided.
    If no timezone is provided, the timezone of the machine is used.

    :param time: The time.
    """
    return (
        get_datetime_now(time.tzinfo)
        .replace(
            hour=time.hour,
            minute=time.minute,
            second=time.second,
            microsecond=0,
        )
        .astimezone()
    )


def parse_datetime(
    dt: str, tzinfo: datetime.tzinfo | zoneinfo.ZoneInfo = None
) -> datetime.datetime:
    """
    Parse a datetime string in "%Y-%m-%d %H:%M:%S" format into a datetime.datetime object.
    The datetime object is converted to the specified timezone on return. If no timezone is provided,
    the timezone of the machine is used.

    :param dt: datetime string.
    """
    tzinfo = tzinfo or get_datetime_now().tzinfo
    return (
        datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        .replace(tzinfo=tzinfo)
        .astimezone()
    )


def get_datetime_now(
    tzinfo: datetime.tzinfo | zoneinfo.ZoneInfo = None,
) -> datetime.datetime:
    """
    Returns the current datetime converted to the specified timezone.
    It basically calls `datetime.datetime.now(tz=tzinfo).astimezone()`

    :param tzinfo: The timezone info.
    """
    return datetime.datetime.now(tz=tzinfo).astimezone()


class MinMaxValidator:
    """
    Validates that value is not less than minimum value and not greater than maximum value
    """

    def __init__(
        self, min_value: Optional[int] = None, max_value: Optional[int] = None
    ):
        """
        Instantiate validator

        :param min_value: Minimum allowed value
        :param max_value: Maximum allowed value
        """
        if min_value is None and max_value is None:
            raise ValueError("At least one of min_value or max_value must be specified")
        if min_value and not isinstance(min_value, int):
            raise ValueError("min_value must be an integer")
        if max_value and not isinstance(max_value, int):
            raise ValueError("max_value must be an integer")
        self.min_value = min_value
        self.max_value = max_value
        return None

    def __call__(self, value: int) -> bool:
        """
        Validate value

        :param value: value to validate
        :return: True if value is valid. Otherwise, False.
        """
        if not isinstance(value, int):
            raise ValueError("Value must be an integer")

        is_valid = True
        if self.min_value is not None:
            is_valid = is_valid and value >= self.min_value
        if self.max_value is not None:
            is_valid = is_valid and value <= self.max_value
        return is_valid


def get_current_datetime(with_tz: bool = False) -> str:
    """
    Get the current datetime in string format

    :param with_tz: Whether to include the timezone in the string
    :return: The current datetime string
    """
    if with_tz:
        return datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S (%z)")
    return datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")


@final
class _RedirectStandardOutputStream:
    """
    Context manager that redirects all standard output streams within the block
    to a stream that will 'always' write to console (stderr by default).

    Can be used to ensure that all output streams are written to console, even if
    the output stream is in a different thread.
    """

    __slots__ = ("stream", "og_stdout")

    def __init__(self, redirect_to: TextIO = sys.stderr):
        self.stream = redirect_to
        self.og_stdout = None
        return None

    def __call__(self, __o: Any, /) -> Any:
        return self.stream.write(str(__o))

    def __enter__(self):
        # Store the original sys.stdout when entering the block
        self.og_stdout = sys.stdout
        # Redirect sys.stdout to the preferred stream
        sys.stdout = self.stream
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Restore the original sys.stdout when exiting the block
        sys.stdout = self.og_stdout


def underscore_string(__s: str, /, replaceables: tuple[str, ...] = None) -> str:
    """
    Convert string to underscore format.

    :param __s: The string to underscore
    """
    replaceables = replaceables or (" ", "-", ".", "/", "\\")
    for char in replaceables:
        __s = __s.replace(char, "_")
    return __s


def underscore_datetime(__dt: str, /):
    """
    Convert datetime string to underscore format.

    :param __dt: The datetime string to underscore
    """
    replaceables = (" ", ":", "-", ".", "/", "\\")
    return underscore_string(__dt, replaceables)


def generate_random_id(length: int = 6) -> str:
    return "".join(random.choices(uuid.uuid4().hex, k=length)).upper()


def ensure_value_is_null(value: Any) -> None:
    """Private validator to ensure that the `wait_duration` value of a time period schedule is null."""
    if not isinstance(value, null):
        raise ValueError(
            f"Expected value to be of type '{null.__name__}', not '{type(value).__name__}'."
        )
    return


def validate_schedules_iterable(value: Iterable) -> None:
    from .schedules import Schedule

    for item in value:
        if not isinstance(item, Schedule):
            raise TypeError("All items in the iterable must be instances of 'Schedule'")
        try:
            item.wait_duration
        except AttributeError:
            raise ValueError(
                f"'{item}' is not a valid entry."
                f" The '{item.__class__.__name__}' schedule cannot be used solely."
                " It has to be chained with a schedule that has it's wait duration specified to form a useable schedule clause."
            )
    return None


def weekday_to_str(weekday: int) -> str:
    """Convert weekday integer to string."""
    if weekday == 0:
        return "Monday"
    elif weekday == 1:
        return "Tuesday"
    elif weekday == 2:
        return "Wednesday"
    elif weekday == 3:
        return "Thursday"
    elif weekday == 4:
        return "Friday"
    elif weekday == 5:
        return "Saturday"
    elif weekday == 6:
        return "Sunday"
    raise ValueError("Invalid weekday value. Must be between 0 and 6.")


def str_to_weekday(weekday: str) -> int:
    """Convert weekday string to integer."""
    weekday = weekday.lower()
    if weekday == "monday":
        return 0
    elif weekday == "tuesday":
        return 1
    elif weekday == "wednesday":
        return 2
    elif weekday == "thursday":
        return 3
    elif weekday == "friday":
        return 4
    elif weekday == "saturday":
        return 5
    elif weekday == "sunday":
        return 6
    raise ValueError(
        "Invalid weekday value. Must be one of 'Monday', 'Tuesday', 'Wednesday', "
        "'Thursday', 'Friday', 'Saturday', 'Sunday'."
    )


def month_to_str(month: int) -> str:
    """Convert month integer to string."""
    return datetime.date(2000, month, 1).strftime("%B")


def str_to_month(month: str) -> int:
    """Convert month string to integer."""
    month = month.lower()
    if month == "january":
        return 1
    elif month == "february":
        return 2
    elif month == "march":
        return 3
    elif month == "april":
        return 4
    elif month == "may":
        return 5
    elif month == "june":
        return 6
    elif month == "july":
        return 7
    elif month == "august":
        return 8
    elif month == "september":
        return 9
    elif month == "october":
        return 10
    elif month == "november":
        return 11
    elif month == "december":
        return 12
    raise ValueError(
        "Invalid month value. Must be one of 'January', 'February', 'March', 'April', 'May', 'June', 'July', "
        "'August', 'September', 'October', 'November', 'December'."
    )


def _strip_text(text: str, remove_prefix: Optional[str] = None) -> str:
    """
    Private helper function that strips the given text of
    necessary characters and remove the prefix if specified.
    """
    if remove_prefix:
        text = text.removeprefix(remove_prefix)
    return text.strip().rstrip(".")


def await_future(future: asyncio.Future, *, suppress_exc: bool = False) -> Any | None:
    """
    Await in a purely synchronous context.
    Calling this function will block the current thread until the future is resolved.

    Do not call this function in an asynchronous context.

    :param future: The future to await.
    :param suppress_exc: Whether to suppress exceptions or not.
    :return: The result of the awaited future or
    None if an exception occurred and suppress_exc is True.
    """

    async def wrapper() -> Any | None:
        try:
            return await future
        except BaseException as exc:
            if not suppress_exc:
                raise exc
        return None

    return asyncio.run(wrapper())
