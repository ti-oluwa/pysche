from __future__ import annotations
from typing import List, TypeVar, Union, Callable, Coroutine, Any
import asyncio
import datetime
from asgiref.sync import sync_to_async

from .abc import AbstractBaseSchedule
from ._utils import utcoffset_to_zoneinfo, get_datetime_now, _strip_text, underscore_string
from .descriptors import SetOnceDescriptor, AttributeDescriptor


NO_RESULT = object()
"""A sentinel object to indicate that the schedule func did not execute the task because the schedule is not yet due."""


class Schedule(AbstractBaseSchedule):
    """
    Base schedule class.

    A schedule determines when a task will be executed.

    Schedules can be chained together to create a complex schedule
    called a "schedule clause". Although, they can still be used singly.

    A schedule clause must always end with a schedule that has a wait duration.

    NOTE: A Schedule instance can be used for multiple tasks. Therefore, because of this,
    the next execution time of the tasks based on the schedule is not available.
    """
    
    removable_prefix = "run_"
    """
    This indicates a prefix that can be removed from the string representation of a schedule.
    It usually utilized when generating a string representation of a schedule clause.

    By default, the __str__ method assumes that the string representation (returned by `as_string()`) takes the form "run_<schedule_name>".
    Hence, the default behavior is to remove the prefix "run_" from the string representation when it is a
    child schedule in a schedule clause.

    So, if your schedule subclass' `as_string()` method returns a string representation that does not start with "run_",
    you should set this attribute to the removeable prefix of the string representation of the schedule.
    """
    parent = SetOnceDescriptor(AbstractBaseSchedule, default=None)
    """The schedule this schedule is chained to. If not specified, the schedule will be a standalone schedule."""

    wait_duration = AttributeDescriptor(datetime.timedelta, default=None)
    """The time period to wait before the schedule is due."""

    def __init__(self, **kwargs) -> None:
        """
        Creates a schedule.

        :kwarg parent: The schedule this schedule is chained to.
        :kwarg tz: The timezone to use for the schedule. If not specified, the timezone of the parent schedule will be used.
        """
        super().__init__(**kwargs)
        parent = kwargs.get("parent", None)
        if parent and not isinstance(parent, Schedule):
            raise TypeError(f"'parent' must be an instance of '{Schedule.__name__}'")
        
        self.parent = parent
        # If timezone is not set, use the timezone of the parent schedule phrase
        if not self.tz and self.parent:
            if self.parent.tz is not None:
                self.tz = self.parent.tz

        # If timezone is still not set, use the machine/local timezone
        if not self.tz:
            self.tz = utcoffset_to_zoneinfo(get_datetime_now().utcoffset())
        return None
    

    def make_schedule_func_for_task(self, scheduledtask) -> Callable[..., Coroutine[Any, Any, Any]]:
        from .tasks import ScheduledTask
        task: ScheduledTask = scheduledtask

        async def schedule_func(*args, **kwargs) -> None:
            # If the schedule has a wait duration, sleep for the duration before running the task
            if self.wait_duration is not None and task.is_paused is False:
                await asyncio.sleep(self.wait_duration.total_seconds())

            # If the task is paused, do not proceed
            while task.is_paused is True:
                await asyncio.sleep(0.0001)
                continue
            
            if await sync_to_async(self.is_due)() is True:
                task._last_executed_at = get_datetime_now(self.tz)
                return await task.func(*args, **kwargs)
            return NO_RESULT

        schedule_func.__qualname__ = f"schedule_func_for_{underscore_string(task.name)}"
        return schedule_func

    
    def get_ancestors(self) -> List[ScheduleType]:
        """
        Return a list of all previous schedules this schedule is chained to,
        starting from the first schedule in the chain.

        Example:
        ```python
        import pysche

        s = psyche.schedules
        run_in_march_from_mon_to_fri_at_12_30pm = s.run_in_month(month=3).within_weekday(start=0, end=4).at("12:30:00")

        print(run_in_march_from_mon_to_fri_at_12_30pm.get_ancestors())
        # >>> [run_in_month(month=3), run_within_weekday(start=0, end=4)]
        """
        ancestors = []
        schedule = self
        while schedule.parent:
            ancestors.append(schedule.parent)
            schedule = schedule.parent

        if ancestors:
            # Reverse the list so that the first schedule in the chain is the first item in the list
            ancestors.reverse()
        return ancestors
    
    
    def as_string(self) -> str:
        attrs_dict = self.__dict__.copy()
        attrs_dict.pop("tz", None)
        attrs_list = []
        for k, v in attrs_dict.items():
            if k != "parent":
                attrs_list.append(f"{k}={v}")
        
        if self.tz and (not self.parent or self.tz != self.parent.tz):
            # if this is a standalone schedule, or this is the first schedule in a schedule clause, 
            # or the tzinfo of this schedule is different from its parent's, add the tzinfo attribute
            attrs_list.append(f"tz='{self.tz}'")
        return f"{type(self).__name__.lower()}({', '.join(attrs_list)})"
    

    def description(self) -> str:
        desc = _strip_text(self.__describe__())
        if self.parent:
            desc = f"{_strip_text(self.parent.description())}, {_strip_text(desc.lower(), "task will run")}"
        return f"{desc.capitalize()}."


    def __str__(self) -> str:
        """Returns a string representation of the entire schedule/schedule clause (including any parent(s) if any)."""
        representation = self.as_string()
        if self.parent:
            # If the schedule has a parent(s) return the representation of the parent(s) and the schedule
            # joined together in the same order as they are chained.
            parents_representations = []
            for index, ancestor in enumerate(self.get_ancestors()):
                parent_representation: str = ancestor.as_string()
                if index != 0:
                    parents_representations.append(parent_representation.removeprefix(self.removable_prefix))
                    continue
                parents_representations.append(parent_representation)
            return f"{'.'.join(parents_representations)}.{representation.removeprefix(self.removable_prefix)}"
        
        # Just return the representation of the schedule.
        return representation
    

    def __hash__(self) -> int:
        # Schedules/schedule clauses with the same string representation should have the same hash
        return hash(str(self))
    

    def __eq__(self, other: ScheduleType) -> bool:
        if not isinstance(other, Schedule):
            raise NotImplementedError(
                f"Cannot compare {type(self).__name__} and {type(other).__name__}"
            )
        # Schedules/schedule clauses with the same string representation should be considered equal
        # since they will both work the same way
        return hash(self) == hash(other)


    def __add__(self, other: ScheduleType):
        """Combines two schedules into a schedule group."""
        from .schedulegroups import group_schedules
        try:
            return group_schedules(self, other)
        except ValueError as exc:
            raise ValueError(
                f"Cannot add {self} and {other}"
            ) from exc

    # Allows the addition of schedules using the `+` operator
    __iadd__ = __add__
    __radd__ = __add__


# Type variable for Schedule and its subclasses
ScheduleType = TypeVar("ScheduleType", bound=Schedule)


    
