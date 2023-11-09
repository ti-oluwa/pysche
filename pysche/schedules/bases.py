from typing import Callable
import datetime
import asyncio
import functools
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

from ..manager import TaskManager



class Schedule:
    """
    Determines when a task will be executed.

    Base class for all schedules.
    """

    def __init__(self, *args, **kwargs):
        if self.__class__ is Schedule:
            raise TypeError("Cannot instantiate abstract class, 'Schedule', directly. Use a subclass instead")
        raise NotImplementedError("This method must be implemented by a subclass")


    def __call__(
            self, 
            *, 
            manager: TaskManager,
            name: str = None,
            do_then_wait: bool = False,
            stop_on_error: bool = False,
            max_retry: int = 0,
            start_immediately: bool = True
        ):
        """
        Create a function that will run on this schedule and will be executed by the specified manager.

        :param manager: The manager to execute the task.
        :param name: The name of the task. If not specified, the name of the function will be used.
        :param do_then_wait: If True, the task will be executed immediately before waiting for the schedule to be due.
        :param stop_on_error: If True, the task will stop running if an exception is encountered.
        :param max_retry: The maximum number of times the task will be retried if an exception is encountered.
        :param start_immediately: If True, the task will start immediately after being created.
        """
        from ..task import ScheduledTask
        def decorator(func: Callable):
            """Create function that will run on this schedule"""
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return ScheduledTask(
                    func, 
                    self, 
                    manager, 
                    args=args, 
                    kwargs=kwargs,
                    name=name,
                    do_then_wait=do_then_wait,
                    stop_on_error=stop_on_error,
                    max_retry=max_retry,
                    start_immediately=start_immediately,
                )
            wrapper.__name__ = name or func.__name__
            return wrapper
        return decorator


    def is_due(self) -> bool:
        """Return True if the schedule is due otherwise False."""
        raise NotImplementedError("This method must be implemented by a subclass")
    

    def _make_schedule_func_for_task(self, task):
        """Return a function that will be used to schedule the task."""
        raise NotImplementedError("This method must be implemented by a subclass")



class _GenericScheduleFuncMixin:
    """Mixin class that adds a generic method for creating a schedule function for a task."""

    def _make_schedule_func_for_task(self: Schedule, task):
        task.func.has_been_done_first = False
        task.func.has_run = False
        schedule_is_due = task.manager._make_asyncable(self.is_due)

        async def schedule_func(*args, **kwargs):
            if task.do_then_wait and not task.func.has_been_done_first:
                # Try to do the func first if do_then_wait is True, and the func has not been done first already.
                # If an exception is encountered when running the func, raise the exception but register that the
                # func has already been done first.
                with task._lock:
                    try:
                        await task.func(*args, **kwargs)
                    except:
                        raise
                    finally:
                        task.func.has_been_done_first = True
            if await schedule_is_due():
                # The scheduled time (HH:MM:SS) specified for task.func to run 
                # may be encountered multiple times, especially when 'func'
                # is used in a loop. To prevent task.func from running multiple times, 
                # check if task.func has run and set the 'has_run' attribute to True if it has been run.
                if not task.func.has_run:
                    task._last_ran_at = datetime.datetime.now(tz=self.tz)
                    await task.func(*args, **kwargs)
                    task.func.has_run = True
            else:
                # Reset the 'has_run' attribute to False if the scheduled time (HH:MM:SS) 
                # specified for task.func to run has passed or has not been reached yet.
                task.func.has_run = False
            return

        schedule_func.__name__ = task.name
        schedule_func.__qualname__ = schedule_func.__name__
        return schedule_func


# ------------------- #
# ----- PHRASES ----- #
# ------------------- #


class BaseSchedulePhrase(_GenericScheduleFuncMixin, Schedule):
    """
    Base class for schedule phrases.

    A schedule phrase is a schedule that can be chained with other schedule phrases to create a complex schedule
    called a "schedule clause".

    Base schedule phrases cannot be chained with other schedule phrases but other schedule phrases can be chained to them to form clauses.
    They are usually the last schedule phrase in a schedule clause.
    """
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
        # If timezone is still not set, use the local timezone
        if not self.tz:
            self.tz = datetime.datetime.now().astimezone().tzinfo
        return None
    

    def get_ancestors(self):
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
    

    def __repr__(self):
        representation = f"{self.__class__.__name__}({', '.join([f'{k}={v}' for k, v in self.__dict__.items() if k != 'parent'])})"
        # if self.parent:
        #     representation = f"{"@".join([repr(ancestor) for ancestor in self.get_ancestors()])}.{representation}"
        #     return ".".join(set(representation.split("@")))
        return representation


def _parse_time(time: str, tzinfo: datetime.tzinfo) -> datetime.time:
    """Parse time string in format "%H:%M:%S" to datetime.time object"""
    split = time.split(":")
    if len(split) != 3:
        raise ValueError("Time must be in the format, 'HH:MM:SS'") 
        
    hour, minute, second = split
    if not tzinfo:
        tzinfo = datetime.datetime.now().astimezone().tzinfo
    return datetime.time(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        tzinfo=tzinfo,
    )



class RunAt(BaseSchedulePhrase):
    """Task will run at the specified time, everyday"""
    def __init__(self, time: str = None, **kwargs):
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
        self.time = _parse_time(time=time, tzinfo=self.tz)
        return None


    def is_due(self) -> bool:
        time = self.time.strftime("%H:%M:%S")
        time_now = datetime.datetime.now(tz=self.tz).time().strftime("%H:%M:%S")
        if self.parent:
            return self.parent.is_due() and time_now == time
        return time_now == time
    
        


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
            milliseconds: int = 0,
            microseconds: int = 0,
            **kwargs,
        ):
        """
        Create a schedule that will be due after the specified interval, repeatedly.

        :param weeks: The number of weeks.
        :param days: The number of days.
        :param hours: The number of hours.

        Example:
        ```
        run_afterevery_5_seconds = RunAfterEvery(seconds=5)
        @run_afterevery_5_seconds(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        self.delay = datetime.timedelta(
            days=days,
            seconds=seconds,
            microseconds=microseconds,
            milliseconds=milliseconds,
            minutes=minutes,
            hours=hours,
            weeks=weeks,
        ).total_seconds()
        if self.delay == 0:
            raise ValueError("At least one of the arguments must be greater than 0")
        super().__init__(**kwargs)


    def is_due(self) -> bool:
        if self.parent:
            if not self.parent.is_due():
                return False
        return True
    

    def _make_schedule_func_for_task(self, task):
        task.func.has_been_done_first = False
        schedule_is_due = task.manager._make_asyncable(self.is_due)

        async def schedule_func(*args, **kwargs):
            if task.do_then_wait and not task.func.has_been_done_first:
                # Try to do the func first if do_then_wait is True, and the func has not been done first already.
                # If an exception is encountered when running the func, raise the exception but register that the
                # func has already been done first.
                with task._lock:
                    try:
                        await task.func(*args, **kwargs)
                    except:
                        raise
                    finally:
                        task.func.has_been_done_first = True
            if await schedule_is_due():
                await asyncio.sleep(self.delay)
                task._last_ran_at = datetime.datetime.now(tz=self.tz)
                await task.func(*args, **kwargs)
            return

        schedule_func.__name__ = task.name
        schedule_func.__qualname__ = schedule_func.__name__
        return schedule_func



class _AtPhraseMixin:
    """Allows chaining of the 'at' phrase to other schedule phrases."""
    def at(self: Schedule, time: str, **kwargs):
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
            milliseconds: int = 0,
            microseconds: int = 0,
        ):
        """
        Task will run after specified interval, repeatedly.

        :param weeks: The number of weeks.
        :param days: The number of days.
        :param hours: The number of hours.
        :param minutes: The number of minutes.
        :param seconds: The number of seconds.
        :param milliseconds: The number of milliseconds.
        :param microseconds: The number of microseconds.
        """
        return RunAfterEvery(
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            milliseconds=milliseconds,
            microseconds=microseconds,
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
    def __init__(self, *, _from: str, _to: str, **kwargs):
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
        self._from = _parse_time(time=_from, tzinfo=self.tz)
        self._to = _parse_time(time=_to, tzinfo=self.tz)
        if self._from > self._to:
            raise ValueError("'_from' time must be less than '_to' time")
        return None
    

    def is_due(self) -> bool:
        time_now = datetime.datetime.now(tz=self.tz).time()
        if self.parent:
            return self.parent.is_due() and time_now >= self._from and time_now <= self._to
        return time_now >= self._from and time_now <= self._to


    def afterevery(
            self,
            *,
            minutes: int = 0,
            seconds: int = 0,
            milliseconds: int = 0,
        ):
        return super().afterevery(
            minutes=minutes,
            seconds=seconds, 
            milliseconds=milliseconds
        )



class _From__ToPhraseMixin:
    """Allows chaining of the 'from__to' phrase to other schedule phrases."""
    def from__to(self: Schedule, *, _from: str, _to: str):
        """
        Task will only run within the specified time frame, everyday.

        :param _from: The time to start running the task. The time must be in the format, "HH:MM:SS".
        :param _to: The time to stop running the task. The time must be in the format, "HH:MM:SS".
        """
        return RunFrom__To(_from=_from, _to=_to, parent=self)
