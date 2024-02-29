from __future__ import annotations
from typing import Callable, Coroutine, Any, List, TypeVar
import datetime
import asyncio
import functools
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo
from abc import ABC, abstractmethod

from .manager import TaskManager
from .utils import SetOnceDescriptor, utcoffset_to_zoneinfo, get_datetime_now



class AbstractBaseSchedule(ABC):
    """
    Abstract base class for all schedules.
    """
    def __call__(
        self, 
        *, 
        manager: TaskManager,
        name: str = None,
        execute_then_wait: bool = False,
        stop_on_error: bool = False,
        max_retry: int = 0,
        start_immediately: bool = True
    ):
        """
        Creates a function that will run on this schedule and will be executed by the specified manager.

        :param manager: The manager to execute the task.
        :param name: The name of the task. If not specified, the name of the function will be used.
        :param execute_then_wait: If True, the task will be executed immediately before waiting for the schedule to be due.
        :param stop_on_error: If True, the task will stop running if an exception is encountered.
        :param max_retry: The maximum number of times the task will be retried if an exception is encountered.
        :param start_immediately: If True, the task will start immediately after being created.
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
                    execute_then_wait=execute_then_wait,
                    stop_on_error=stop_on_error,
                    max_retry=max_retry,
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




class Schedule(AbstractBaseSchedule):
    """
    Base schedule class.

    A schedule determines when a task will be executed.

    Schedules can be chained together to create a complex schedule
    called a "schedule clause". Although, they can still be used singly.

    A schedule clause must always end with a schedule that has a timedelta value.
    """
    parent = SetOnceDescriptor(attr_type=AbstractBaseSchedule)
    tz = SetOnceDescriptor(attr_type=zoneinfo.ZoneInfo)
    timedelta = SetOnceDescriptor(attr_type=datetime.timedelta, default=None)

    def __init__(self, **kwargs):
        """
        Creates a schedule.

        :param parent: The schedule this schedule is chained to.
        :param tz: The timezone to use for the schedule. If not specified, the timezone of the parent schedule will be used.
        """
        parent = kwargs.get("parent", None)
        tz = kwargs.get("tz", None)
        if parent and not isinstance(parent, Schedule):
            raise TypeError(f"'parent' must be an instance of '{Schedule.__name__}'")
        
        self.parent = parent
        self.tz = zoneinfo.ZoneInfo(tz) if tz and isinstance(tz, str) else tz
        # If timezone is not set, use the timezone of the parent schedule phrase
        if not self.tz and self.parent:
            if getattr(self.parent, "tz", None):
                self.tz = self.parent.tz

        # If timezone is still not set, use the machine/local timezone
        if not self.tz:
            self.tz = utcoffset_to_zoneinfo(get_datetime_now().utcoffset())
        return None
    

    def __call__(
        self, 
        *, 
        manager: TaskManager, 
        name: str = None, 
        execute_then_wait: bool = False, 
        stop_on_error: bool = False, 
        max_retry: int = 0, 
        start_immediately: bool = True
    ):
        if self.timedelta is None:
            raise ValueError(
                f"The '{self.__class__.__name__}' schedule cannot be used solely to create a task."
                " It has to be chained with a schedule that has its timedelta defined to form a useable schedule clause."
            )
        return super().__call__(
            manager=manager, 
            name=name, 
            execute_then_wait=execute_then_wait, 
            stop_on_error=stop_on_error, 
            max_retry=max_retry, 
            start_immediately=start_immediately
        )
    

    def make_schedule_func_for_task(self, scheduledtask) -> Callable[..., Coroutine[Any, Any, None]]:
        schedule_is_due: Callable[..., bool] = scheduledtask.manager._make_asyncable(self.is_due)

        async def schedule_func(*args, **kwargs) -> None:
            # If the schedule has a timedelta, sleep for the timedelta period
            if self.timedelta is not None and scheduledtask.is_paused is False:
                await asyncio.sleep(self.timedelta.total_seconds())

            # If the task is paused, do not proceed
            while scheduledtask.is_paused is True:
                await asyncio.sleep(0.0001)
                continue
            
            if await schedule_is_due() is True:
                scheduledtask._last_ran_at = datetime.datetime.now(tz=self.tz)
                await scheduledtask.func(*args, **kwargs)
            return

        schedule_func.__name__ = scheduledtask.name
        schedule_func.__qualname__ = scheduledtask.name
        return schedule_func

    
    def get_ancestors(self) -> List[ScheduleType]:
        """
        Return a list of all previous schedules this schedule is chained to,
        starting from the first schedule in the chain.

        Example:
        ```python
        run_in_march_from_mon_to_fri_at_12_30pm = RunInMonth(month=3).from_weekday__to(_from=0, _to=4).at("12:30:00")

        print(run_in_march_from_mon_to_fri_at_12_30pm.get_ancestors())
        # [RunInMonth(month=3), RunInMonth(month=3).RunFromWeekday__To(_from=0, _to=4)]
        """
        ancestors = []
        if self.parent:
            ancestors.append(self.parent)
            ancestors.extend(self.parent.get_ancestors())
        ancestors.reverse()
        return ancestors
    

    def __repr__(self) -> str:
        """Returns a representation of the schedule."""
        return f"{self.__class__.__name__}({', '.join([f'{k}={v}' for k, v in self.__dict__.items() if k != 'parent'])})"
    

    def __str__(self) -> str:
        """Returns a string representation of the schedule."""
        if self.parent:
            # If the schedule has a parent(s) return the representation of the parent(s) and the schedule
            # joined together in the same order as they are chained.
            return f"{'.'.join([ repr(ancestor) for ancestor in self.get_ancestors() ])}.{repr(self)}"
        # Just return the representation of the schedule.
        return repr(self)


# Type variable for Schedule and its subclasses
ScheduleType = TypeVar("ScheduleType", bound=Schedule)
