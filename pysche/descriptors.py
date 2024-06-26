from typing import Any, List, Callable, Type, Generic, TypeVar, Optional


class null:
    """Sentinel object to indicate that no value was provided."""

    pass


NOT_SET = null()
T = TypeVar("T")
D = TypeVar("D")


class AttributeDescriptor(Generic[T, D]):
    """Implements a descriptor for an attribute of a class."""

    def __init__(
        self,
        attr_type: Optional[Type[T]] = None,
        *,
        default: D = NOT_SET,
        validators: Optional[List[Callable]] = None,
    ) -> None:
        """
        Initialize the descriptor

        :param attr_type: Type of value expected for the attribute.
        If the value is not of this type and is not None, a TypeError is raised
        :param validators: list of validators to be used to validate the attribute's value.

        The validators are callables that take the attribute's value as an argument and return True if the value is valid.
        However the validator are also allowed to raise their own exceptions if the value is not valid. This is especially
        useful when the validation logic is complex or custom exception message is needed.
        """
        if attr_type is not None and not isinstance(attr_type, type):
            raise TypeError("attr_type must be a type or None")
        if validators and not isinstance(validators, list):
            raise TypeError("validators must be a list")

        self.attr_type = attr_type
        self.validators = validators or []
        self.default = default
        for validator in self.validators:
            if not callable(validator):
                raise TypeError("validators must be a list of callables")
        return None

    def __set_name__(self, owner, name: str) -> None:
        if not isinstance(name, str):
            raise TypeError("name must be a string")
        self.name = name

    def __get__(self, instance: Any, owner: Type[Any]) -> T | D:
        """
        Get the property value

        :param instance: instance of the class
        :param owner: class that owns the instance
        :return: value of the attribute
        """
        if instance is None:
            return self
        try:
            return instance.__dict__[self.name]
        except KeyError:
            if isinstance(self.default, null):
                raise AttributeError(f"{self.name} is not set")
            return self.default

    def __set__(self, instance: Any, value: T) -> None:
        """
        Set the attribute value on the instance.

        :param instance: instance of the class
        :param value: value to be set
        :raises TypeError: if the value is not of the expected type
        :raises ValueError: if the value is not valid
        :raises AttributeError: if the attribute has already been set
        """
        if value is not None and self.attr_type is not None:
            if not isinstance(value, self.attr_type):
                raise TypeError(f"{self.name} must be of type {self.attr_type}")

        for validator in self.validators:
            r = validator(value)
            # Peradventure the validator returns a boolean value,
            # we assume that the validation failed if the value is not True
            if isinstance(r, bool) and r is not True:
                raise ValueError(f"Validation failed for {self.name}")
        instance.__dict__[self.name] = value
        return None


class SetOnceDescriptor(AttributeDescriptor[T, D]):
    """
    Descriptor that allows an attribute to be set to a 'not-None' value only once on an instance.
    """

    def __set__(self, instance: Any, value: T) -> None:
        """
        Set the attribute value on the instance.
        The attributes value can only be set to a not-None value once,
        after which it cannot be changed.

        :param instance: instance of the class
        :param value: value to be set
        :raises TypeError: if the value is not of the expected type
        :raises ValueError: if the value is not valid
        :raises AttributeError: if the attribute has already been set and is not None.
        """
        if self.name in instance.__dict__ and instance.__dict__[self.name] is not None:
            raise AttributeError(
                f"Not allowed! Attribute {self.name} has already been set"
            )
        return super().__set__(instance, value)
