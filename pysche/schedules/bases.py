from __future__ import annotations
from typing import Callable, Coroutine, Any, List, Literal
import datetime
import asyncio
import functools
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo
from abc import ABC, abstractmethod

from ..manager import TaskManager
from .._utils import SetOnceDescriptor, parse_time, utcoffset_to_zoneinfo, get_current_datetime_from_time



class Schedule(ABC):
    """
    Abstract base class for all schedules.

    Determines when a task will be executed.
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
        Create a function that will run on this schedule and will be executed by the specified manager.

        :param manager: The manager to execute the task.
        :param name: The name of the task. If not specified, the name of the function will be used.
        :param execute_then_wait: If True, the task will be executed immediately before waiting for the schedule to be due.
        :param stop_on_error: If True, the task will stop running if an exception is encountered.
        :param max_retry: The maximum number of times the task will be retried if an exception is encountered.
        :param start_immediately: If True, the task will start immediately after being created.
        """
        from ..task import ScheduledTask
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



# ------------------- #
# ----- PHRASES ----- #
# ------------------- #

class BaseSchedulePhrase(Schedule):
    """
    Base class for schedule phrases.

    A schedule phrase is a schedule that can be chained with other schedule phrases to create a complex schedule
    called a "schedule clause".

    Base schedule phrases cannot be chained with other schedule phrases but other schedule phrases can be chained to them to form clauses.
    They are usually the last schedule phrase in a schedule clause.
    """
    parent = SetOnceDescriptor(attr_type=Schedule)
    tz = SetOnceDescriptor(attr_type=zoneinfo.ZoneInfo)
    timedelta = SetOnceDescriptor(attr_type=datetime.timedelta, default=None)
    # __slots__ = ("__dict__",)

    def __init__(self, **kwargs):
        parent = kwargs.get("parent", None)
        tz = kwargs.get("tz", None)
        if parent and not isinstance(parent, Schedule):
            raise TypeError("'parent' must be an instance of 'Schedule'")
        
        self.parent = parent
        self.tz = zoneinfo.ZoneInfo(tz) if tz and isinstance(tz, str) else tz
        # If timezone is not set, use the timezone of the parent schedule phrase
        if not self.tz and self.parent:
            if getattr(self.parent, "tz", None):
                self.tz = self.parent.tz

        # If timezone is still not set, use the machine/local timezone
        if not self.tz:
            self.tz = utcoffset_to_zoneinfo(datetime.datetime.now().utcoffset())
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
                f"The '{self.__class__.__name__}' schedule phrase cannot be used solely to create a task."
                " It has to be chained with a base schedule phrase that has a timedelta."
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
            if self.timedelta is not None:
                await asyncio.sleep(self.timedelta.total_seconds())

            # If the task is paused, do not proceed
            while scheduledtask.is_paused is True:
                await asyncio.sleep(0.1)
                continue
            
            if await schedule_is_due() is True:
                scheduledtask._last_ran_at = datetime.datetime.now(tz=self.tz)
                await scheduledtask.func(*args, **kwargs)
            return

        schedule_func.__name__ = scheduledtask.name
        schedule_func.__qualname__ = scheduledtask.name
        return schedule_func

    
    def get_ancestors(self) -> List[BaseSchedulePhrase]:
        """
        Return a list of all previous schedule phrases this schedule phrase is chained to,
        starting from the first schedule phrase in the chain.

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
        representation = f"{self.__class__.__name__}({', '.join([f'{k}={v}' for k, v in self.__dict__.items() if k != 'parent'])})"
        # if self.parent:
        #     representation = f"{"@".join([repr(ancestor) for ancestor in self.get_ancestors()])}.{representation}"
        #     return ".".join(set(representation.split("@")))
        return representation



class RunAt(BaseSchedulePhrase):
    """Task will run at the specified time, everyday"""
    time = SetOnceDescriptor(attr_type=datetime.time)

    def __init__(self, time: str = None, **kwargs) -> None:
        """
        Create a schedule that will be due at the specified time, everyday.

        :param time: The time to run the task. The time must be in the format, "HH:MM:SS".
        
        Example:
        ```
        run_at_12_30pm_daily = RunAt(time="12:30:00", tz="Africa/Lagos")
        @run_at_12_30pm_daily(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(**kwargs)
        self.time = parse_time(time=time, tzinfo=self.tz)
        datetime_from_time = get_current_datetime_from_time(self.time)
        current_datetime = datetime.datetime.now(tz=self.tz)

        if datetime_from_time > current_datetime: 
            # That is, the time is in the future
            self.timedelta = datetime_from_time - current_datetime
        else:
            # If the time is in the past, the time will be due the next day minus 
            # the time difference between the current time and the specified time.
            self.timedelta = datetime.timedelta(days=1) - (current_datetime - datetime_from_time)
        return None
    

    # def is_due(self) -> bool:
    #     time = self.time.strftime("%H:%M:%S")
    #     time_now = datetime.datetime.now(tz=self.tz).time().strftime("%H:%M:%S")
    #     # Use datetime.time "%H:%M:%S" string comparison instead of direct datetime.time comparison because
    #     # the program cannot be precise enough to compare the time to the nearest microsecond.
    #     if self.parent:
    #         return self.parent.is_due() and time_now == time
    #     return time_now == time

    def is_due(self) -> Literal[True]:
        return True
    
        


class RunAfterEvery(BaseSchedulePhrase):
    """Task will run after the specified interval, repeatedly"""
    def __init__(
        self,
        *, 
        weeks: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        **kwargs,
    ) -> None:
        """
        Create a schedule that will be due after the specified interval, repeatedly.

        :param weeks: The number of weeks.
        :param days: The number of days.
        :param hours: The number of hours.
        :param minutes: The number of minutes.
        :param seconds: The number of seconds.

        Example:
        ```
        run_afterevery_5_seconds = RunAfterEvery(seconds=5)
        @run_afterevery_5_seconds(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        self.timedelta = datetime.timedelta(
            days=days,
            seconds=seconds,
            minutes=minutes,
            hours=hours,
            weeks=weeks,
        )
        if self.timedelta.total_seconds() <= 0:
            raise ValueError("At least one of the arguments must be greater than 0")
        self._last_due_at = None
        self._next_due_at = None
        super().__init__(**kwargs)


    # def is_due(self) -> bool:
    #     if not self._next_due_at:
    #         self._next_due_at = (datetime.datetime.now(tz=self.tz) + self.timedelta).replace(microsecond=0)

    #     next_due_dt = self._next_due_at.strftime("%Y-%m-%d %H:%M:%S")
    #     dt_now = datetime.datetime.now(tz=self.tz).strftime("%Y-%m-%d %H:%M:%S")
    #     # Use datetime.time "%Y-%m-%d %H:%M:%S" string comparison instead of direct datetime.time comparison because
    #     # the program cannot be precise enough to compare the datetime to the nearest microsecond.
    #     if self.parent:
    #         is_due = self.parent.is_due() and dt_now == next_due_dt
    #     else:
    #         is_due = dt_now == next_due_dt
    #     if is_due:
    #         self._last_due_at = self._next_due_at
    #         self._next_due_at = (self._last_due_at + self.timedelta).replace(microsecond=0)
    #     return is_due
    #     # the microsecond is set to 0 since the program cannot be precise 
    #     # enough to compare the time to the nearest microsecond.Also, It is not used for comparison.
        
    def is_due(self) -> Literal[True]:
        return True
    


class _AtPhraseMixin:
    """Allows chaining of the 'at' phrase to other schedule phrases."""
    def at(self: Schedule, time: str, **kwargs) -> RunAt:
        """
        Task will run at the specified time, everyday.

        :param time: The time to run the task. The time must be in the format, "HH:MM:SS".
        """
        return RunAt(time=time, parent=self, **kwargs)



class _AfterEveryPhraseMixin:
    """Allows chaining of the 'afterevery' phrase to other schedule phrases."""
    def afterevery(
        self: Schedule,
        *,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
    ) -> RunAfterEvery:
        """
        Task will run after specified interval, repeatedly.

        :param weeks: The number of weeks.
        :param days: The number of days.
        :param hours: The number of hours.
        :param minutes: The number of minutes.
        :param seconds: The number of seconds.
        """
        return RunAfterEvery(
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            parent=self,
        )



# ------------------- #
# ----- FROM TO ----- #
# ------------------- #

class RunFrom__To(_AfterEveryPhraseMixin, BaseSchedulePhrase):
    """
    Task will only run within the specified time frame, everyday.

    This special schedule phrase is meant to be chained with the `afterevery` phrase.
    """
    def __init__(self, *, _from: str, _to: str, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified time frame, everyday.

        :param _from: The time to start running the task. The time must be in the format, "HH:MM:SS".
        :param _to: The time to stop running the task. The time must be in the format, "HH:MM:SS".

        Example:
        ```
        run_from_12_30_to_13_30 = RunFrom__To(_from="12:30:00", _to="13:30:00", tz="Africa/Lagos")

        @run_from_12_30_to_13_30.afterevery(seconds=5)(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")
        
        func()
        ```
        """
        super().__init__(**kwargs)
        self._from = parse_time(time=_from, tzinfo=self.tz)
        self._to = parse_time(time=_to, tzinfo=self.tz)
        if self._from >= self._to:
            raise ValueError("'_from' time must be less than '_to' time")
        return None
    

    def is_due(self) -> bool:
        time_now = datetime.datetime.now(tz=self.tz).time()
        is_due = time_now >= self._from and time_now <= self._to
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due


    def afterevery(
            self,
            *,
            minutes: int = 0,
            seconds: int = 0,
        ) -> RunAfterEvery:
        return super().afterevery(
            minutes=minutes,
            seconds=seconds, 
        )



class _From__ToPhraseMixin:
    """Allows chaining of the 'from__to' phrase to other schedule phrases."""
    def from__to(self: Schedule, *, _from: str, _to: str) -> RunFrom__To:
        """
        Task will only run within the specified time frame, everyday.

        :param _from: The time to start running the task. The time must be in the format, "HH:MM:SS".
        :param _to: The time to stop running the task. The time must be in the format, "HH:MM:SS".
        """
        return RunFrom__To(_from=_from, _to=_to, parent=self)
