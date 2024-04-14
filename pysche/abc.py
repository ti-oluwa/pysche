from __future__ import annotations
from typing import Callable, Coroutine, Any, List, Optional
import functools
from abc import ABC, abstractmethod
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

from .manager import TaskManager
from .descriptors import SetOnceDescriptor
from ._utils import _strip_description



class AbstractBaseSchedule(ABC):
    """Abstract base class for all schedules."""

    tz = SetOnceDescriptor(zoneinfo.ZoneInfo, default=None)
    """
    The timezone to use for the schedule. This should only be set once.
    """
    def __init__(self, **kwargs) -> None:
        """
        Creates a schedule.

        :kwarg parent: The schedule this schedule is chained to.
        :kwarg tz: The timezone to use for the schedule.
        """
        tz = kwargs.get("tz", None)
        if tz and not isinstance(tz, (str, zoneinfo.ZoneInfo)):
            raise TypeError(f"'tz' must be a string or an instance of '{zoneinfo.ZoneInfo.__name__}'")

        self.tz = zoneinfo.ZoneInfo(tz) if tz and isinstance(tz, str) else tz
        return None
    

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
            if not callable(func):
                raise TypeError(
                    f"Decorated object must be a callable not type: {type(func).__name__}"
                )
            
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
    

    def __repr__(self) -> str:
        return self.as_string()
    

    @abstractmethod
    def is_due(self) -> bool:
        """Returns True if the schedule is due otherwise False."""
        pass


    @abstractmethod
    def as_string(self) -> str:
        """Returns a string representation of this schedule object."""
        pass


    @abstractmethod
    def __describe__(self) -> str:
        """
        Returns a human-readable description of the schedule.

        Descriptions should always start with "Task will run" followed by the schedule description.
        An example of a description is "Task will run every 5 minutes".
        """
        pass


    def __format__(self, format_spec: str) -> str:
        if format_spec.strip() == "desc":
            return self.description()
        return str(self)
    

    @abstractmethod
    def make_schedule_func_for_task(self, scheduledtask) -> Callable[..., Coroutine[Any, Any, None]]:
        """
        Returns coroutine function that runs the scheduled task on the appropriate schedule

        The function created waits for the duration specified by the `wait_duration` attribute of the schedule,
        and then checks if the schedule is due after the wait period, before the task is executed.

        Override this method to customize how the schedule function is created.

        :param scheduledtask: The scheduled task to run.
        """
        pass


    def description(self) -> str:
        """Returns a human-readable description of the schedule."""
        desc = _strip_description(self.__describe__().lower())
        if not desc.startswith("task will run"):
            raise ValueError("Description must start with 'Task will run'")
        return f"{desc.capitalize()}."
