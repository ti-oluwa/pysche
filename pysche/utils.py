from __future__ import annotations
from typing import Any, List, Callable, TypeVar
import inspect
import sys
import datetime
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo


def utcoffset_to_zoneinfo(offset: datetime.timedelta) -> zoneinfo.ZoneInfo:
    """
    Convert a UTC offset to a ZoneInfo object.

    :param offset: The UTC offset.
    :return: The ZoneInfo object.
    """
    # Calculate the UTC offset in seconds
    offset_seconds = offset.total_seconds() if offset is not None else 0
    # Generate a time zone name based on the UTC offset
    zone_name = f"Etc/GMT{'+' if offset_seconds >= 0 else '-'}{int(abs(offset_seconds)//3600)}"

    try:
        # Try to load the ZoneInfo object for the calculated time zone
        return zoneinfo.ZoneInfo(zone_name)
    except zoneinfo.ZoneInfoNotFoundError:
        raise ValueError(f"No ZoneInfo found for UTC offset {offset_seconds}")


def parse_time(time: str, tzinfo: datetime.tzinfo | zoneinfo.ZoneInfo) -> datetime.time:
    """
    Parse time string in format "%H:%M:%S" to datetime.time object

    :param time: The time string.
    :param tzinfo: The timezone info.
    """
    split = time.split(":")
    if len(split) != 3:
        raise ValueError("Time must be in the format, 'HH:MM:SS'") 
        
    hour, minute, second = split
    tzinfo = tzinfo or datetime.datetime.now().tzinfo
    return datetime.time(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        tzinfo=tzinfo,
    )


def get_current_datetime_from_time(time: datetime.time) -> datetime.datetime:
    """
    Return the current datetime with the specified time.
    The datetime object is converted to the timezone of the datetime.time object provided.

    :param time: The time.
    """
    tzinfo = time.tzinfo or datetime.datetime.now().tzinfo
    return get_datetime_now(tzinfo).replace(
        hour=time.hour,
        minute=time.minute,
        second=time.second,
        microsecond=0,
    )


def parse_datetime(dt: str, tzinfo: datetime.tzinfo | zoneinfo.ZoneInfo = None) -> datetime.datetime:
    """
    Parse a datetime string in "%Y-%m-%d %H:%M:%S" format into a datetime.datetime object.
    The datetime object is converted to the specified timezone on return.

    :param dt: datetime string.
    """
    tzinfo = tzinfo or datetime.datetime.now().tzinfo
    return datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tzinfo).astimezone()


def get_datetime_now(tzinfo: datetime.tzinfo | zoneinfo.ZoneInfo = None) -> datetime.datetime:
    """
    Returns the current datetime converted to the specified timezone.
    It basically calls `datetime.datetime.now(tz=tzinfo).astimezone()`

    :param tzinfo: The timezone info.
    """
    return datetime.datetime.now(tz=tzinfo).astimezone()


Klass = TypeVar("Klass", bound=object)
NO_DEFAULT = object() # sentinel object to indicate that no default value was provided
Validator = Callable[[Any], None]


class SetOnceDescriptor:
    """
    Descriptor that allows an attribute to be set to a 'not-None' value only once on an instance.
    """
    def __init__(
        self, 
        attr_type: Klass = None, 
        *,
        default: Klass = NO_DEFAULT,
        validators: List[Validator] | None = None
    ) -> None:
        """
        Initialize the descriptor

        :param attr_type: type of value expected for the attribute. 
        If the value is not of this type and is not None, a TypeError is raised
        :param validators: list of validators to be used to validate the attribute's value
        """
        if attr_type is not None and not inspect.isclass(attr_type):
            raise TypeError('attr_type must be a class')
        if validators and not isinstance(validators, list):
            raise TypeError('validators must be a list')
        
        self.attr_type: Klass = attr_type
        self.validators = validators or []
        self.default = default
        for validator in self.validators:
            if not callable(validator):
                raise TypeError('validators must be a list of callables')
        return None
            
    
    def __set_name__(self, owner, name: str) -> None:
        if not isinstance(name, str):
            raise TypeError('name must be a string')
        self.name = name


    def __get__(self, instance, owner) -> SetOnceDescriptor | Klass:
        """
        Get the property value

        :param instance: instance of the class
        :param owner: class that owns the instance
        :return: value of the attribute
        """
        if instance is None:
            return self
        try:
            value: Klass = instance.__dict__[self.name]
        except KeyError:
            if self.default is NO_DEFAULT:
                raise
            value = self.default
        return value


    def __set__(self, instance, value) -> None:
        """
        Set the attribute value on the instance

        :param instance: instance of the class
        :param value: value to be set
        """
        if self.name in instance.__dict__ and instance.__dict__[self.name] is not None:
            raise AttributeError(f'Attribute {self.name} has already been set')
        
        if value is not None and self.attr_type is not None:
            if not isinstance(value, self.attr_type):
                raise TypeError(f'{self.name} must be of type {self.attr_type}')
        
        for validator in self.validators:
            r = validator(value)
            # Peradventure the validator returns a boolean value, 
            # we assume that the validation failed if the value is not True
            if isinstance(r, bool) and r is not True:
                raise ValueError(f'Validation failed for {self.name}')
        instance.__dict__[self.name] = value
        return None



class MinMaxValidator:
    """
    Validates that value is not less than minimum value and not greater than maximum value
    """
    def __init__(self, min_value: int=None, max_value: int=None):
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



class _RedirectStandardOutputStream:
    """
    Context manager that redirects all standard output streams within the block 
    to a stream that will 'always' write to console.

    Can be used to ensure that all output streams are written to console, even if
    the output stream is in a different thread.
    """
    def __init__(self):
        self.stream = None
        return None

    def __call__(self, __o: Any) -> Any:
        return self.stream.write(str(__o))    

    def __enter__(self):
        # Store the original sys.stdout
        self.og_stream = sys.stdout
        # Redirect sys.stdout to the sys.stderr
        self.stream = sys.stderr
        sys.stdout = self.stream
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        # Restore the original sys.stdout
        sys.stdout = self.og_stream
