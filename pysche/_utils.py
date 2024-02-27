from typing import Any, List, Callable, TypeVar
import inspect
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
    zone_name = f"Etc/GMT{'+' if offset_seconds >= 0 else '-'}{abs(offset_seconds)//3600}"

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
    if not tzinfo:
        tzinfo = datetime.datetime.now().tzinfo
    return datetime.time(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        tzinfo=tzinfo,
    )


def get_current_datetime_from_time(time: datetime.time) -> datetime.datetime:
    """
    Return the current datetime with the specified time

    :param time: The time.
    """
    tzinfo = time.tzinfo or datetime.datetime.now().tzinfo
    return datetime.datetime.now(tz=tzinfo).replace(
        hour=time.hour,
        minute=time.minute,
        second=time.second,
        microsecond=0,
    )


def parse_datetime(dt: str, tzinfo: datetime.tzinfo = None) -> datetime.datetime:
    """
    Parse a datetime string in "%Y-%m-%d %H:%M:%S" format into a datetime.datetime object.

    :param dt: datetime string.
    """
    tzinfo = tzinfo or datetime.datetime.now().tzinfo
    return datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tzinfo)



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
        default: Any = NO_DEFAULT,
        validators: List[Validator] = None
    ) -> None:
        """
        Initialize the descriptor

        :param attr_type: type of value expected for the attribute. 
        If the value is not of this type and is not None, a TypeError is raised
        :param validators: list of validators to be used to validate the attribute's value
        """
        if attr_type and not inspect.isclass(attr_type):
            raise TypeError('attr_type must be a class')
        if validators and not isinstance(validators, list):
            raise TypeError('validators must be a list')
        
        self.attr_type = attr_type or object
        self.validators = validators or []
        self.default = default
        for validator in self.validators:
            if not callable(validator):
                raise TypeError('validators must be a list of callables')
        return None
            
    
    def __set_name__(self, owner, name: str):
        if not isinstance(name, str):
            raise TypeError('name must be a string')
        self.name = name


    def __get__(self, instance: object, owner: object):
        """
        Get the property value

        :param instance: instance of the class
        :param owner: class that owns the instance
        :return: value of the attribute
        """
        if instance is None:
            return self
        try:
            value = instance.__dict__[self.name]
        except KeyError:
            if self.default is NO_DEFAULT:
                raise
            value = self.default
        return value


    def __set__(self, instance: object, value: object) -> None:
        """
        Set the attribute value on the instance

        :param instance: instance of the class
        :param value: value to be set
        """
        if self.name in instance.__dict__ and instance.__dict__[self.name] is not None:
            raise AttributeError(f'Attribute {self.name} has already been set')
        
        if value is not None and not isinstance(value, self.attr_type):
            raise TypeError(f'{self.name} must be of type {self.attr_type}')
        
        for validator in self.validators:
            r = validator(value)
            # Peradventure the validator returns a boolean value, 
            # we assume that the validation failed if the value is not True
            if isinstance(r, bool) and r is not True:
                raise ValueError(f'Validation failed for {self.name}')
        instance.__dict__[self.name] = value

