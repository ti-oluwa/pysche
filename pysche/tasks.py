from __future__ import annotations
import uuid
import asyncio
import time
from typing import Callable, Any, Coroutine, Sequence, Mapping, List
import functools
import datetime
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

from .manager import TaskManager
from .bases import ScheduleType, Schedule
from .utils import get_datetime_now
from .descriptors import SetOnceDescriptor
from .exceptions import TaskDuplicationError



class ScheduledTask:
    """Task that runs a function on a specified schedule"""
    id = SetOnceDescriptor(attr_type=str)
    manager = SetOnceDescriptor(attr_type=TaskManager)
    schedule = SetOnceDescriptor(attr_type=Schedule)
    func = SetOnceDescriptor(validators=[asyncio.iscoroutinefunction])
    __slots__ = (
        "name", "args", "kwargs", "execute_then_wait", 
        "stop_on_error", "max_retry", "_is_running", 
        "_is_paused", "_failed", "_errors", "_last_ran_at", 
        "__dict__", "__weakref__"
    )

    def __init__(
        self,
        func: Callable,
        schedule: ScheduleType,
        manager: TaskManager,
        *,
        args: Sequence[Any] = (),
        kwargs: Mapping[str, Any] = {},
        name: str = None,
        execute_then_wait: bool = False,
        stop_on_error: bool = False,
        max_retry: int = 0,
        start_immediately: bool = True,
    ) -> None:
        """
        Create a new scheduled task.

        :param func: The function to be scheduled.
        :param schedule: The schedule for the task.
        :param manager: The manager to execute the task.
        :param args: The arguments to be passed to the scheduled function.
        :param kwargs: The keyword arguments to be passed to the scheduled function.
        :param name: The name of the task.
        :param execute_then_wait: If True, the function will be dry run first before applying the schedule.
        Also, if this is set to True, errors encountered on dry run will be propagated and will stop the task
        without retry, irrespective of `stop_on_error` or `max_retry`
        :param stop_on_error: If True, the task will stop running when an error is encountered during its execution.
        :param max_retry: The maximum number of times the task will be retried after an error is encountered.
        :param start_immediately: If True, the task will start immediately after creation. 
        This is only applicable if the manager is already running.
        Otherwise, task execution will start when the manager starts executing tasks.
        """
        if not callable(func):
            raise TypeError("Invalid type for 'func'. Should be a callable")
        
        self.id = uuid.uuid4().hex[-6:]
        self.manager = manager
        func = self._wrap_func_for_time_stats(func)
        self.func = self.manager._make_asyncable(func)
        self.name = name or self.func.__name__

        if abs(self.manager.max_duplicates) == self.manager.max_duplicates: # If positive
            siblings = self.manager.get_tasks(self.name)
            if len(siblings) >= self.manager.max_duplicates:
                raise TaskDuplicationError(
                    f"'{self.manager.name}' can only manage {self.manager.max_duplicates} duplicates of '{self.name}'."
                )
        
        self.args = args
        self.kwargs = kwargs
        self.execute_then_wait = execute_then_wait
        self.stop_on_error = stop_on_error
        self.max_retry = max_retry
        self.schedule = schedule
        self._is_running = False
        self._is_paused = False
        self._failed = False
        self._errors = []
        self._last_ran_at: datetime.datetime = None
        self.manager._tasks.append(self)
        if start_immediately:
            self.start()
        return None
    
    
    @property
    def is_running(self) -> bool:
        """
        Returns True if task has been scheduled for execution or has started been executed.
        """
        return self._is_running
    
    @property
    def is_paused(self) -> bool:
        """
        Returns True if further task execution is currently paused.

        `task.is_paused` and `task.is_running` being True simultaneous is okay.
        They are mutually inclusive.
        """
        return self._is_paused
    
    @property
    def failed(self) -> bool:
        """Returns True if task execution failed at some point"""
        return self._failed
    
    @property
    def cancelled(self) -> bool:
        """Returns True if the task execution has been cancelled"""
        # Check if the future handling this task's coroutine has been cancelled
        future = self.manager._get_future(self.id)
        if future:
            return future.cancelled()
        else:
            # If the future cannot be found, and it had already started being executed, return True.
            if self.is_running:
                return True
        return False
    
    @property
    def errors(self) -> List[Exception]:
        """Returns a list of errors"""
        return self._errors
    
    @property
    def last_ran_at(self) -> datetime.datetime | None:
        """Returns the last time this task ran (in the timezone in which the task was scheduled)"""
        return self._last_ran_at
    
    @property
    def status(self):
        if self.cancelled:
            return "cancelled"
        return (
            "failed" if self.failed else "paused" 
            if self.is_paused else "running" 
            if self.is_running else "stopped" 
            if self.last_ran_at else "pending"
        )
    

    def log(self, msg: str, exception: str = False, **kwargs) -> None:
        """
        Record a task log.

        :param msg: The message to be logged.
        :param exception: If True, the message will be logged as an exception.
        """
        self.manager.log(f"{self.manager.name}({self.name})  {msg}", **kwargs)
        if exception:
            kwargs.pop("level", None)
            self.manager.log("An exception occurred: ", level="DEBUG", exc_info=1, **kwargs)
        return None

    
    async def __call__(self) -> Coroutine[Any, Any, None]:
        """Returns a coroutine that will be run to execute this task"""
        if self.cancelled:
            raise RuntimeError(f"{self.name} has already been cancelled. {self.name} cannot be called.")

        try:
            if self.execute_then_wait is True:
                # Execute the task first if execute_then_wait is True.
                self.log("Task added for execution.\n")
                self._is_running = True
                self._last_ran_at = get_datetime_now(self.schedule.tz)
                await self.func(*self.args, **self.kwargs)

            schedule_func = self.schedule.make_schedule_func_for_task(self)
            err_count = 0
            while self.manager._continue and not (self.failed or self.cancelled): # The prevents the task from running when the manager has not been started (or is stopped)
                if not self.is_running:
                    self.log("Task added for execution.\n")
                    self._is_running = True

                try:
                    await schedule_func(*self.args, **self.kwargs)
                except (
                    SystemExit, KeyboardInterrupt, 
                    asyncio.CancelledError, RuntimeError
                ):
                    break
                
                except Exception as exc:
                    self._errors.append(exc)
                    self.log(f"{exc}\n", level="ERROR", exception=True)

                    if self.stop_on_error is True or err_count >= self.max_retry:
                        self._failed = True
                        self._is_running = False
                        self.log("Task execution failed.\n", level="CRITICAL")
                        break

                    err_count += 1
                    continue
        finally:
            # If task exits loop, task has stopped executing
            if self.is_running:
                self.log("Task execution stopped.\n")
                self._is_running = False
        return None

    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.status} func=<{self.func.__name__}> name='{self.name}'>"
    

    def _wrap_func_for_time_stats(self, func: Callable) -> Callable:
        """Wrap function to log time stats"""
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            self.log("Executing task...\n")
            start_timestamp = time.time()
            r = func(*args, **kwargs)
            self.log(f"Task execution completed in {time.time() - start_timestamp :.4f} seconds.\n")
            return r
        return wrapper


    def update(self, **kwargs) -> None:
        """
        Update task's attributes (Instantiation parameters)

        :param **kwargs: new attributes and the values to update them with
        """
        for key, val in kwargs.items():
            try:
                setattr(self, key, val)
            except Exception as exc:
                raise AttributeError(f"Error! Cannot update '{key}' to '{val}'") from exc
        return None
    

    def start(self) -> None:
        """
        Start task execution. You cannot start a failed or cancelled task

        The task will not start if it is already running, paused, failed or cancelled.
        Also, the task will not start until its manager has been started.
        """
        if not any((self.is_running, self.is_paused, self.failed, self.cancelled)):
            # Task starts automatically if manager is already running 
            # else, it waits for the manager to start
            self.manager._make_future(self)
            if self.manager.has_started:
                # wait for task to start running if manager has already started task execution
                while not self.is_running:
                    continue
        return None


    def rerun(self) -> None:
        """Re-run failed task"""
        if not self.failed:
            raise RuntimeError(f"Cannot rerun '{self.name}'. '{self.name}' has not failed.")
        self._failed = False
        self._is_paused = False
        self.start()
        return None
    

    def join(self) -> None:
        """Wait for task to finish running before proceeding"""
        while self.is_running:
            try:
                time.sleep(0.0001)
                continue
            except (SystemExit, KeyboardInterrupt):
                break
        return None
    

    def pause(self) -> None:
        """Pause task. Stops task from running, temporarily"""
        if self.is_paused:
            raise RuntimeError(f"Cannot pause '{self.name}'. '{self.name}' is already paused.")
        self._is_paused = True
        self.log("Task execution paused.\n")
        return None
    

    def resume(self) -> None:
        """Resume paused task"""
        if not self.is_paused:
            return
        self._is_paused = False
        self.log("Task execution resumed.\n")
        return None
    

    def pause_after(self, delay: int | float, /) -> ScheduledTask:
        """
        Create a new task to pause this task after specified delay in seconds

        :param delay: The delay in seconds after which this task will be paused
        :return: The created task
        """
        return self.manager.run_after(delay, self.pause, task_name=f"pause_{self.name}_after_{delay}s")


    def pause_for(self, seconds: int | float, /) -> ScheduledTask:
        """
        Pauses this task and creates a new task to resume this task after the specified number of seconds

        :param seconds: The number of seconds to pause this task for
        :return: The created task 
        """
        self.pause()
        return self.manager.run_after(seconds, self.resume, task_name=f"resume_{self.name}_after_{seconds}s")


    def pause_until(self, time: str, /) -> ScheduledTask:
        """
        Pauses this task and creates a new task to resume this task at specified time. 

        :param time: The time to resume this task. Time should be in the format 'HH:MM:SS'
        :return: The created task
        """
        self.pause()
        return self.manager.run_at(time, self.resume, task_name=f"resume_{self.name}_at_{time}")
    

    def pause_on(self, datetime: str, /, tz: datetime.tzinfo | zoneinfo.ZoneInfo = None) -> ScheduledTask:
        """
        Creates a new task to pause this task on the specified datetime.

        :param datetime: The datetime to pause this task. The datetime should be in the format 'YYYY-MM-DD HH:MM:SS'.
        :param tz: The timezone of the datetime. If not specified, the task's schedule timezone will be used.
        :return: The created task
        """
        tz = tz or self.schedule.tz
        return self.manager.run_on(datetime, self.pause, tz=tz, task_name=f"pause_{self.name}_on_{datetime}")
    

    def pause_at(self, time: str, /) -> ScheduledTask:
        """
        Creates a new task that pauses this task at a specified time.

        :param time: The time to pause this task. Time should be in the format 'HH:MM:SS'
        :return: The created task
        """
        return self.manager.run_at(time, self.pause, task_name=f"pause_{self.name}_at_{time}")
    

    def update_after(self, delay: int | float, /, **kwargs) -> ScheduledTask:
        """
        Delays an update. Creates a new task that updates this task after the specified delay

        :param delay: The delay in seconds after which this task will be updated
        :param kwargs: The update parameters
        :return: The created task
        """
        return self.manager.run_after(delay, self.update, task_name=f"update_{self.name}_after_{delay}s", kwargs=kwargs)


    def update_at(self, time: str, **kwargs) -> ScheduledTask:
        """
        Delays an update. Creates a new task that updates this task at the specified time

        :param time: The time to update this task. Time should be in the format 'HH:MM:SS'
        :param kwargs: The update parameters
        :return: The created task
        """
        return self.manager.run_at(time, lambda: self.update(**kwargs), task_name=f"update_{self.name}_at_{time}")
    

    def update_on(self, datetime: str, /, tz: datetime.tzinfo | zoneinfo.ZoneInfo = None, **kwargs) -> ScheduledTask:
        """
        Delays an update. Creates a new task that updates this task at the specified datetime.

        :param datetime: The datetime to update this task. The datetime should be in the format 'YYYY-MM-DD HH:MM:SS'.
        :param tz: The timezone of the datetime. If not specified, the task's schedule timezone will be used.
        :param kwargs: The update parameters
        :return: The created task
        """
        tz = tz or self.schedule.tz
        return self.manager.run_on(datetime, lambda: self.update(**kwargs), tz=tz, task_name=f"update_{self.name}_on_{datetime}")

    
    def cancel(self, wait: bool = False) -> None:
        """
        Cancel task. Cancelling a task will invalidate task execution by the manager.

        Note that cancelling a task will not stop the manager from executing other tasks.

        :param wait: If True, wait for the current iteration of this task's 
        execution to end before cancelling.
        """
        if self.cancelled:
            return
        
        task_future = self.manager._get_future(self.id)
        if task_future is None:
            if self.is_running or self.last_ran_at:
                # If task is running or has run before and a future cannot 
                # be found for the task then `manager._futures` has been tampered with.
                # Raise a runtime error for this
                raise RuntimeError(
                    f"{self.__class__.__name__}: Cannot find future '{self.id}' in manager. '_tasks' is out of sync with '_futures'.\n"
                )
        else: 
            task_future.cancel()
            self.manager._futures.remove(task_future)
            del task_future

        self.manager._tasks.remove(self)
        if wait is True:
            self.join()
        self.log("Task cancelled.\n")
        return None


    def cancel_after(self, delay: int | float, /) -> ScheduledTask:
        """
        Creates a new task that cancels this task after specified delay in seconds

        :param delay: The delay in seconds after which this task will be cancelled
        :return: The created task
        """
        return self.manager.run_after(delay, self.cancel, task_name=f"cancel_{self.name}_after_{delay}s")
    

    def cancel_at(self, time: str, /) -> ScheduledTask:
        """
        Creates a new task that cancels this task at a specified time

        :param time: The time to cancel this task. Time should be in the format 'HH:MM:SS'
        :return: The created task
        '"""
        return self.manager.run_at(time, self.cancel, task_name=f"cancel_{self.name}_at_{time}")
    

    def cancel_on(self, datetime: str, /, tz: datetime.tzinfo | zoneinfo.ZoneInfo = None) -> ScheduledTask:
        """
        Creates a new task that cancels this task at the specified datetime

        :param datetime: The datetime to cancel this task. The datetime should be in the format 'YYYY-MM-DD HH:MM:SS'.
        :param tz: The timezone of the datetime. If not specified, the task's schedule timezone will be used.
        :return: The created task
        """
        tz = tz or self.schedule.tz
        return self.manager.run_on(datetime, self.cancel, tz=tz, task_name=f"cancel_{self.name}_on_{datetime}")


    
def scheduledtask(
    schedule: ScheduleType,
    manager: TaskManager,
    *,
    name: str = None,
    execute_then_wait: bool = False,
    stop_on_error: bool = False,
    max_retry: int = 0,
    start_immediately: bool = True,
) -> Callable[[Callable], Callable[..., ScheduledTask]]:
    """
    Decorator for creating a scheduled task.

    Calling the decorated function will create and return a new scheduled task.

    :param schedule: The schedule for the task.
    :param manager: The manager to execute the task.
    :param name: The name of the task.
    :param execute_then_wait: If True, the function will be dry run first before applying the schedule.
    Also, if this is set to True, errors encountered on dry run will be propagated and will stop the task
    without retry, irrespective of `stop_on_error` or `max_retry`
    :param stop_on_error: If True, the task will stop running when an error is encountered during its execution.
    :param max_retry: The maximum number of times the task will be retried after an error is encountered.
    :param start_immediately: If True, the task will start immediately after creation. 
    This is only applicable if the manager is already running.
    Otherwise, task execution will start when the manager starts executing tasks.
    """
    decorator = schedule(
        manager=manager,
        name=name,
        execute_then_wait=execute_then_wait,
        stop_on_error=stop_on_error,
        max_retry=max_retry,
        start_immediately=start_immediately,
    )
    return decorator

