from __future__ import annotations
from typing import Callable, Coroutine, Any, List, TypeVar, Optional
import datetime
import asyncio
import functools
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo
from abc import ABC, abstractmethod

from .manager import TaskManager
from ._utils import utcoffset_to_zoneinfo, get_datetime_now
from .descriptors import SetOnceDescriptor



class AbstractBaseSchedule(ABC):
    """Abstract base class for all schedules."""

    def __call__(
        self, 
        *, 
        manager: TaskManager,
        name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        execute_then_wait: bool = False,
        save_results: bool = False,
        resultset_size: Optional[int] = None,
        stop_on_error: bool = False,
        max_retries: int = 0,
        start_immediately: bool = True
    ):
        """
        Function decorator. Decorated function will run on this schedule and will be executed by the specified manager.

        :param manager: The manager to execute the task.
        :param name: The preferred name for the task. If not specified, the name of the function will be used.
        :param tags: A list of tags to attach to the task. Tags can be used to group tasks together.
        :param execute_then_wait: If True, the function will be dry run first before applying the schedule.
        Also, if this is set to True, errors encountered on dry run will be propagated and will stop the task
        without retry, irrespective of `stop_on_error` or `max_retries`.
        :param save_results: If True, the results of each task execution will be saved.
        :param resultset_size: The maximum number of results to save. If the number of results exceeds this value, the oldest results will be removed.
        If `save_results` is False, this will be ignored. If this is not specified, the default value will be 10.
        :param stop_on_error: If True, the task will stop running when an error is encountered during its execution.
        :param max_retries: The maximum number of times the task will be retried consecutively after an error is encountered.
        :param start_immediately: If True, the task will start immediately after creation. 
        This is only applicable if the manager is already running.
        Otherwise, task execution will start when the manager starts executing tasks.
        """
        from .tasks import ScheduledTask
        def decorator(func: Callable) -> Callable[..., ScheduledTask]:
            """Create function that will run on this schedule"""
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> ScheduledTask:
                return ScheduledTask(
                    func, 
                    self, 
                    manager, 
                    args=args, 
                    kwargs=kwargs,
                    name=name,
                    tags=tags,
                    execute_then_wait=execute_then_wait,
                    save_results=save_results,
                    resultset_size=resultset_size,
                    stop_on_error=stop_on_error,
                    max_retries=max_retries,
                    start_immediately=start_immediately,
                )
            
            wrapper.__name__ = name or func.__name__
            return wrapper
        
        return decorator
    

    @abstractmethod
    def is_due(self) -> bool:
        """Returns True if the schedule is due otherwise False."""
        pass

    @abstractmethod
    def make_schedule_func_for_task(self, scheduledtask) -> Callable[..., Coroutine[Any, Any, None]]:
        """
        Returns coroutine function that runs the scheduled task on the appropriate schedule

        :param scheduledtask: The scheduled task to run.
        """
        pass

    @abstractmethod
    def get_ancestors(self) -> List[AbstractBaseSchedule]:
        """Returns a list of all previous schedules this schedule is chained to."""
        pass

    @abstractmethod
    def as_string(self) -> str:
        """Returns a string representation of the schedule object."""
        pass


NO_RESULT = object()


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
    
    _removable_str_prefix = "run_"
    """
    This indicates a prefix that can be removed from the string representation of a schedule.
    It usually utilized when generating a string representation of a schedule clause.

    By default, the __str__ method assumes that the string representation schedules take the form "run_<schedule_name>".
    Hence, the default behavior is to remove the prefix "run_" from the string representation of a schedule when it is a
    child schedule in a schedule clause.

    So, if your schedule subclass' `as_string` method returns a string representation that does not start with "run_",
    you should set this attribute to the removeable prefix of the string representation of the schedule.
    """
    parent = SetOnceDescriptor(AbstractBaseSchedule, default=None)
    """The schedule this schedule is chained to. If not specified, the schedule will be a standalone schedule."""
    tz = SetOnceDescriptor(zoneinfo.ZoneInfo)
    """
    The timezone to use for the schedule. If not specified, the timezone of the parent schedule will be used.
    If there is no parent schedule, the machine/local timezone will be used.
    """
    wait_duration = SetOnceDescriptor(datetime.timedelta)
    """The wait duration to use for the schedule."""

    def __init__(self, **kwargs):
        """
        Creates a schedule.

        :kwarg parent: The schedule this schedule is chained to.
        :kwarg tz: The timezone to use for the schedule. If not specified, the timezone of the parent schedule will be used.
        """
        parent = kwargs.get("parent", None)
        tz = kwargs.get("tz", None)
        if parent and not isinstance(parent, Schedule):
            raise TypeError(f"'parent' must be an instance of '{Schedule.__name__}'")
        
        self.parent = parent
        self.tz = zoneinfo.ZoneInfo(tz) if tz and isinstance(tz, str) else tz
        # If timezone is not set, use the timezone of the parent schedule phrase
        if not self.tz and self.parent:
            if self.parent.tz is not None:
                self.tz = self.parent.tz

        # If timezone is still not set, use the machine/local timezone
        if not self.tz:
            self.tz = utcoffset_to_zoneinfo(get_datetime_now().utcoffset())
        return None


    def make_schedule_func_for_task(self, scheduledtask) -> Callable[..., Coroutine[Any, Any, None]]:
        from .tasks import ScheduledTask
        task: ScheduledTask = scheduledtask
        schedule_is_due: Callable[..., bool] = task.manager._make_asyncable(self.is_due)

        async def schedule_func(*args, **kwargs) -> None:
            # If the schedule has a wait duration, sleep for the duration before running the task
            if self.wait_duration is not None and task.is_paused is False:
                await asyncio.sleep(self.wait_duration.total_seconds())

            # If the task is paused, do not proceed
            while task.is_paused is True:
                await asyncio.sleep(0.0001)
                continue
            
            if await schedule_is_due() is True:
                task._last_executed_at = get_datetime_now(self.tz)
                return await task.func(*args, **kwargs)
            return NO_RESULT

        schedule_func.__name__ = task.name
        schedule_func.__qualname__ = task.name
        return schedule_func

    
    def get_ancestors(self) -> List[ScheduleType]:
        """
        Return a list of all previous schedules this schedule is chained to,
        starting from the first schedule in the chain.

        Example:
        ```python
        import pysche

        s = psyche.schedules
        run_in_march_from_mon_to_fri_at_12_30pm = s.run_in_month(month=3).from_weekday__to(_from=0, _to=4).at("12:30:00")

        print(run_in_march_from_mon_to_fri_at_12_30pm.get_ancestors())
        # >>> [run_in_month(month=3), run_from_weekday__to(_from=0, _to=4)]
        """
        ancestors = []
        schedule = self
        while schedule.parent:
            ancestors.append(schedule.parent)
            schedule = schedule.parent

        if ancestors:
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
        return f"{self.__class__.__name__.lower()}({', '.join(attrs_list)})"
    

    def __repr__(self) -> str:
        return self.as_string()


    def __str__(self) -> str:
        """Returns a string representation of the schedule including its parent(s) if any."""
        self_representation = self.as_string()
        if self.parent:
            # If the schedule has a parent(s) return the representation of the parent(s) and the schedule
            # joined together in the same order as they are chained.
            parents_representations = []
            for index, ancestor in enumerate(self.get_ancestors()):
                parent_representation: str = ancestor.as_string()
                if index != 0:
                    parents_representations.append(parent_representation.removeprefix(self._removable_str_prefix or "run_"))
                    continue
                parents_representations.append(parent_representation)
            return f"{'.'.join(parents_representations)}.{self_representation.removeprefix(self._removable_str_prefix or "run_")}"
        
        # Just return the representation of the schedule.
        return self_representation
    

    def __hash__(self) -> int:
        # Schedules with the same representation should have the same hash
        return hash(str(self))
    

    def __eq__(self, other: ScheduleType) -> bool:
        if not isinstance(other, Schedule):
            return False
        # Schedules with the same representation should be considered equal
        # since they will both work the same way
        return str(self) == str(other)




# Type variable for Schedule and its subclasses
ScheduleType = TypeVar("ScheduleType", bound=Schedule)
