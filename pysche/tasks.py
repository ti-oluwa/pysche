from __future__ import annotations
from collections import deque
import uuid
import random
import asyncio
import time
from typing import Callable, Any, Coroutine, List, Optional, Tuple, Dict
import functools
import datetime
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo
from dataclasses import dataclass, field, KW_ONLY, InitVar
from enum import Enum

from .manager import TaskManager
from .bases import ScheduleType, NO_RESULT
from ._utils import get_datetime_now, underscore_string, underscore_datetime, generate_random_id
from .exceptions import TaskCancelled, TaskDuplicationError, TaskError, TaskExecutionError



class CallbackTrigger(Enum):
    ERROR = "error"
    PAUSED = "paused"
    RESUMED = "resumed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    STARTED = "started"
    STOPPED = "stopped"



@dataclass(slots=True, eq=True, frozen=True)
class TaskCallback:
    """Encapsulates a callback to be executed when a specific event occurs during the task's execution."""

    task: ScheduledTask
    """The task to which the callback is attached"""
    func: Callable
    """The function to be called"""
    trigger: CallbackTrigger
    """The event that triggers the callback"""

    def __post_init__(self) -> None:
        if not callable(self.func):
            raise ValueError(f"Callback function must be callable. Got {self.func!r}.")
        return None
    
    def __call__(self, *args, **kwargs) -> None:
        async_func = self.task.manager._make_asyncable(self.func)
        self.task.manager._make_future(async_func, append=False, task=self.task, *args, **kwargs)
        return None 
    


@dataclass(slots=True, eq=True, frozen=True)
class TaskResult:
    """Encapsulates a result returned by a task after execution."""

    task: ScheduledTask
    """The task that returned the result"""
    index: int
    """The current execution index of the task when the result was returned"""
    value: Any
    """The result returned by the task"""
    date_created: datetime.datetime = field(init=False)
    """The time the result was created"""

    def __post_init__(self) -> None:
        object.__setattr__(self, 'date_created', get_datetime_now(self.task.schedule.tz))
        return None

    def __gt__(self, other: TaskResult) -> bool:
        if not isinstance(other, TaskResult):
            return False
        return self.index > other.index and self.date_created > other.date_created



@dataclass(slots=True)
class ScheduledTask:
    """
    Runs a function on a specified schedule.

    Scheduled tasks always run concurrently in the background.

    Example:
    ```python
    import pysche


    def say_goodnight(to: str):
        print(f"Goodnight {to}!")

    s = pysche.schedules
    manager = pysche.TaskManager("manager")

    def main():
        # Create task to run `say_goodnight` at 8pm Lagos timezone
        task = pysche.ScheduledTask(
            func=say_goodnight,
            schedule=s.run_at("20:00:00", tz="Africa/Lagos"),
            manager=manager,
            kwargs={'to': 'Tolu'},
            tags=["greeting"],
            start_immediately=True,
        )
        # Tell manager to start executing tasks
        manager.start()
        print(manager.is_managing(task)) 
        # >>> True
        # Tell manager to wait for all tasks to finish executing before proceeding
        manager.join()

    if __name__ == "__main__":
        main()
    ```
    """

    func: Callable
    """The function to be scheduled."""
    schedule: ScheduleType
    """The schedule to run the task on."""
    manager: TaskManager
    """The manager to execute the task."""

    # keyword-only arguments
    _: KW_ONLY
    args: Tuple[Any, ...] = field(default_factory=tuple)
    """The arguments to be passed to the scheduled function."""
    kwargs: Dict[str, Any] = field(default_factory=dict)
    """The keyword arguments to be passed to the scheduled function."""
    name: Optional[str] = None
    """The preferred name for the task. If not specified, the name of the function will be used."""
    tags: List[str] = field(default_factory=list)
    """A list of tags to attach to the task. Tags can be used to group tasks together."""
    execute_then_wait: bool = field(default=False)
    """
    If True, the function will be dry run first before applying the schedule.
    Also, if this is set to True, errors encountered on dry run will be propagated and will stop the task
    without retry, irrespective of `stop_on_error` or `max_retries`.
    """
    save_results: bool = field(default=False)
    """
    If True, the results of each iteration of the task's execution will be saved. 
    This is only applicable if the task returns a value.
    """
    resultset_size: Optional[int] = None
    """
    The maximum number of results to save. If the number of results exceeds this value, 
    the oldest results will be removed. If `save_results` is False, this will be ignored.

    Also If this is not specified, the default value will be 10.
    """
    stop_on_error: bool = field(default=False)
    """If True, the task will stop running when an error is encountered during its execution."""
    max_retries: int = field(default=0)
    """The maximum number of times the task will be retried consecutively after an error is encountered."""
    start_immediately: InitVar[bool] = field(default=True)
    """
    If True, the task will start immediately after creation. 
    This is only applicable if the manager is already running.
    Otherwise, task execution will start when the manager starts executing tasks.
    """

    # Internal attributes
    id: str = field(init=False, default_factory=generate_random_id)
    """A unique identifier for this task"""
    started_at: datetime.datetime = field(default=None, init=False)
    """The time the task was allowed to start execution"""
    callbacks: List[TaskCallback] = field(default_factory=list, init=False)
    """A list of `TaskCallback`s to be executed when a specific event occurs during the task's execution."""
    resultset: Optional[deque[TaskResult]]= field(default=None, init=False)
    """A deque containing results returned from each iteration of execution of this tasks."""
    _is_active: bool = field(default=False, init=False)
    """True if task has been scheduled for execution and has started been executed."""
    _is_paused: bool = field(default=False, init=False)
    """True if further execution of an active task is currently suspended."""
    _failed: bool = field(default=False, init=False)
    """True if task execution failed at some point"""
    _errors: List[Exception] = field(default_factory=list, init=False)
    """A list of errors that occurred during the task's execution."""
    _last_executed_at: datetime.datetime = field(default=None, init=False)
    """The last time this task was executed (in the timezone in which the task was scheduled)"""
    _exc_count: int = field(default=0, init=False)
    """The number of times the task has been executed (successfully or not) since it started."""

    def __post_init__(self, start_immediately: bool) -> None:
        if not self.name:
            self.name = self.func.__name__
        
        stats_wrapped_func = self._wrap_func_for_time_stats(self.func)
        self.func = self.manager._make_asyncable(stats_wrapped_func)

        if abs(self.manager.max_duplicates) == self.manager.max_duplicates: # If positive
            siblings = self.manager.get_tasks(self.name)
            if len(siblings) >= self.manager.max_duplicates:
                raise TaskDuplicationError(
                    f"'{self.manager.name}' can only manage {self.manager.max_duplicates} duplicates of '{self.name}'."
                )
        self.manager._tasks.append(self)

        if self.save_results is True:
            if self.resultset_size is not None and self.resultset_size < 1:
                raise ValueError("resultset_size must be greater than 0 if save_results is True.")
            self.resultset = deque(maxlen=self.resultset_size or 10)

        if start_immediately is True:
            self.start()
        return None

    @property
    def is_active(self) -> bool:
        """
        Returns True if task has been scheduled for execution and has started been executed.
        """
        return self._is_active is True and self.cancelled is False
    
    @property
    def is_paused(self) -> bool:
        """
        Returns True if further execution of an active task is currently suspended.
        """
        return self._is_paused and self.is_active
    
    @property
    def failed(self) -> bool:
        """Returns True if task execution failed at some point"""
        return self._failed
    
    @property
    def cancelled(self) -> bool:
        """Returns True if the task execution has been cancelled and is no longer active"""
        # Check if the future handling this task's coroutine has been cancelled
        future = self.manager._get_future(self.id)
        if future:
            return future.cancelled()
        # If the future cannot be found, and it probably has not been started
        # Hence, its future has not bee created and the task has not been cancelled
        return False
    
    @property
    def errors(self) -> List[Exception]:
        """Returns a list of errors"""
        return self._errors
    
    @property
    def last_executed_at(self) -> datetime.datetime | None:
        """Returns the last time this task was executed (in the timezone in which the task was scheduled)"""
        return self._last_executed_at

    @property
    def execution_count(self) -> int:
        """The number of times the task has been executed (successfully or not) since it started."""
        return self._exc_count
    
    @property
    def status(self):
        if self.cancelled:
            return "cancelled"
        return (
            "failed" if self.failed else "paused" 
            if self.is_paused else "active" 
            if self.is_active else "stopped" 
            if self.last_executed_at else "pending"
        )
    

    def add_tag(self, tag: str, /):
        """
        Add a tag to the task.

        Tags can be used to group tasks together.

        Get all tasks with a particular tag using `manager.get_tasks('<tag_name>')`
        """
        if tag not in self.tags:
            self.tags.append(tag)
        return None


    def remove_tag(self, tag: str, /):
        """Remove a tag from the task"""
        if tag in self.tags:
            self.tags.remove(tag)
        return None
    

    def log(self, msg: str, exception: str = False, **kwargs) -> None:
        """
        Record a task log.

        :param msg: The message to be logged.
        :param exception: If True, the message will be logged as an exception.
        """
        log_detail = f"{self.__class__.__name__}('{underscore_string(self.name)}', id='{self.id}', manager='{underscore_string(self.manager.name)}') >>> {msg}"
        self.manager.log(log_detail, **kwargs)
        if exception:
            kwargs.pop("level", None)
            self.manager.log("An exception occurred: ", level="DEBUG", exc_info=1, **kwargs)
        return None

    
    async def __call__(self) -> Coroutine[Any, Any, None]:
        """Returns a coroutine that will be run to execute this task"""
        if self.cancelled:
            raise TaskExecutionError(f"{self.name} has already been cancelled. {self.name} cannot be called.")

        try:
            if self.execute_then_wait is True:
                # Execute the task first if execute_then_wait is True.
                self.log("Task added for execution.\n")
                self._is_active = True
                self._last_executed_at = get_datetime_now(self.schedule.tz)
                await self.func(*self.args, **self.kwargs)

            schedule_func = self.schedule.make_schedule_func_for_task(self)
            err_count = 0
            while self.manager._continue and not (self.failed or self.cancelled): # The prevents the task from running when the manager has not been started (or is stopped)
                if not self.is_active:
                    self.log("Task added for execution.\n")
                    self._is_active = True

                try:
                    result = await schedule_func(*self.args, **self.kwargs)
                except (
                    SystemExit, KeyboardInterrupt, 
                    asyncio.CancelledError, RuntimeError
                ):
                    break

                except TaskCancelled as exc:
                    if exc.args:
                        self.log(f"Task cancellation requested: {exc.args[0]}\n", level="INFO")
                    self.cancel()
                    break

                except Exception as exc:
                    self._exc_count += 1
                    self._errors.append(exc)
                    self.log(f"{exc}\n", level="ERROR", exception=True)
                    self.run_callbacks(CallbackTrigger.ERROR)

                    if self.stop_on_error is True or err_count >= self.max_retries:
                        self._failed = True
                        self._is_active = False
                        self.log("Task execution failed.\n", level="CRITICAL")
                        self.run_callbacks(CallbackTrigger.FAILED)
                        break
                    
                    err_count += 1
                    self.log(f"Retrying task execution. Retries remaining: {self.max_retries - err_count}\n")
                    continue

                else:
                    self._exc_count += 1
                    # Ensures that the maximum allowed retries is reset 
                    # peradventure an error has occurred before, but it was resolved.
                    err_count = 0
                    if self.save_results is True and result is not NO_RESULT:
                        task_result = TaskResult(self, self.execution_count, result)
                        self.resultset.append(task_result)
                    continue

        except Exception as exc:
            self._errors.append(exc)
        finally:
            # If task exits loop, task has stopped executing
            if self.is_active:
                self.log("Task execution stopped.\n")
                self._is_active = False
                self.run_callbacks(CallbackTrigger.STOPPED)
        return None

    
    def __repr__(self) -> str:
        kwargs = [ f"{k}={v}" for k, v in self.kwargs.items() ]
        args = [ str(arg) for arg in self.args ]
        params = ', '.join((*args, *kwargs))
        return f"<{self.__class__.__name__} '{self.status}' name='{self.name}' id='{self.id}' func={self.func.__name__}({params})>"
    

    def __hash__(self) -> int:
        return hash(self.id)
    

    def __eq__(self, other: ScheduledTask) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.id == other.id
    

    def __del__(self) -> None:
        # Cancel task if it is still active when it is being deleted
        self.cancel(wait=False)
        return None
    

    def _wrap_func_for_time_stats(self, func: Callable) -> Callable:
        """Wrap function to log time stats"""
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            self.log("Executing task...\n")
            start_timestamp = time.perf_counter()
            r = func(*args, **kwargs)
            end_timestamp = time.perf_counter()
            self.log(f"Task execution completed in {end_timestamp - start_timestamp:.8f} seconds.\n")
            return r
        
        return wrapper


    def start(self) -> None:
        """
        Start task execution. You cannot start a failed or cancelled task

        The task will not start if it is already active, paused, failed or cancelled.
        Also, the task will not start until its manager has started.
        """
        if any((self.is_active, self.is_paused, self.failed, self.cancelled)):
            raise TaskExecutionError(f"Cannot start '{self.name}'. '{self.name}' is {self.status}.")
        
        # Task starts automatically if manager has already started task execution 
        # else, it waits for the manager to start
        self.manager._make_future(self)
        if self.manager.has_started:
            # wait for task to start running if manager has already started task execution
            while not self.is_active:
                continue
        self.started_at = get_datetime_now(self.schedule.tz)
        self.log("Task execution started.\n")
        self.run_callbacks(CallbackTrigger.STARTED)
        return None
        

    def join(self) -> None:
        """Wait for task to finish executing before proceeding"""
        while self.is_active:
            try:
                time.sleep(0.0001)
                continue
            except (SystemExit, KeyboardInterrupt):
                break
        return None
    

    def pause(self) -> None:
        """Pause task. Stops task execution, temporarily"""
        if self.is_paused:
            raise TaskExecutionError(f"Cannot pause '{self.name}'. '{self.name}' is already paused.")
        elif self.cancelled:
            raise TaskExecutionError(f"Cannot pause '{self.name}'. '{self.name}' has been cancelled.")
        elif self.failed:
            raise TaskExecutionError(f"Cannot pause '{self.name}'. '{self.name}' has failed.")
        elif not self.is_active:
            raise TaskExecutionError(f"Cannot pause '{self.name}'. '{self.name}' has not started yet.")
        
        self._is_paused = True
        self.log("Task execution paused.\n")
        self.run_callbacks(CallbackTrigger.PAUSED)
        return None
    

    def resume(self) -> None:
        """Resume paused task"""
        if not self.is_paused:
            # just ignore if task is not paused
            return
        self._is_paused = False
        self.log("Task execution resumed.\n")
        self.run_callbacks(CallbackTrigger.RESUMED)
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

        :param time: The time to resume this task. Time should be in the format 'HH:MM' or 'HH:MM:SS'
        :return: The created task
        """
        self.pause()
        return self.manager.run_at(time, self.resume, task_name=f"resume_{self.name}_at_{underscore_datetime(time)}")
    

    def pause_on(self, datetime: str, /, tz: datetime.tzinfo | zoneinfo.ZoneInfo = None) -> ScheduledTask:
        """
        Creates a new task to pause this task on the specified datetime.

        :param datetime: The datetime to pause this task. The datetime should be in the format 'YYYY-MM-DD HH:MM:SS'.
        :param tz: The timezone of the datetime. If not specified, the task's schedule timezone will be used.
        :return: The created task
        """
        tz = tz or self.schedule.tz
        return self.manager.run_on(datetime, self.pause, tz=tz, task_name=f"pause_{self.name}_on_{underscore_datetime(datetime)}")
    

    def pause_at(self, time: str, /) -> ScheduledTask:
        """
        Creates a new task that pauses this task at a specified time.

        :param time: The time to pause this task. Time should be in the format 'HH:MM:SS'
        :return: The created task
        """
        return self.manager.run_at(time, self.pause, task_name=f"pause_{self.name}_at_{underscore_datetime(time)}")
    
    
    def cancel(self, wait: bool = True) -> None:
        """
        Cancel task. Cancelling a task will invalidate task execution by the manager.

        Note that cancelling a task will not stop the manager from executing other tasks.

        :param wait: If True, wait for the current iteration of this task's 
        execution to end properly after cancel operation has been performed.
        """
        if self.cancelled:
            return
        
        task_future = self.manager._get_future(self.id)
        if task_future is None:
            if self.is_active or self.last_executed_at:
                # If task is being executed or has been executed before and a future cannot 
                # be found for the task then `manager._futures` has been tampered with.
                # Raise a runtime error for this
                raise TaskError(
                    f"Cannot find future for task '{self.name}[id={self.id}]' in '{self.manager.name}'. Tasks are out of sync with futures.\n"
                )
        else: 
            task_future.cancel()

        if wait is True:
            self.join()
        self.log("Task cancelled.\n")
        self.run_callbacks(CallbackTrigger.CANCELLED)
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

        :param time: The time to cancel this task. Time should be in the format 'HH:MM' or 'HH:MM:SS'
        :return: The created task
        '"""
        return self.manager.run_at(time, self.cancel, task_name=f"cancel_{self.name}_at_{underscore_datetime(time)}")
    

    def cancel_on(self, datetime: str, /, tz: datetime.tzinfo | zoneinfo.ZoneInfo = None) -> ScheduledTask:
        """
        Creates a new task that cancels this task at the specified datetime

        :param datetime: The datetime to cancel this task. The datetime should be in the format 'YYYY-MM-DD HH:MM:SS'.
        :param tz: The timezone of the datetime. If not specified, the task's schedule timezone will be used.
        :return: The created task
        """
        tz = tz or self.schedule.tz
        return self.manager.run_on(datetime, self.cancel, tz=tz, task_name=f"cancel_{self.name}_on_{underscore_datetime(datetime)}")


    def add_callback(self, callback_func: Callable, trigger: str | CallbackTrigger = CallbackTrigger.ERROR) -> None:
        """
        Add a callback to the task. Callbacks are functions whose execution are 
        triggered when a task encounters a specific event, while the task is being executed.

        :param callback_func: The function to be called.
        :param trigger: The event that triggers the callback. 
        The event can be one of 'error', 'paused', 'resumed', 'cancelled', 'failed', 'started', 'stopped'.
        """
        trigger = CallbackTrigger(trigger)
        callback = TaskCallback(self, callback_func, trigger)
        self.callbacks.append(callback)
        return None


    def get_callbacks(self, trigger: str | CallbackTrigger) -> List[TaskCallback]:
        """
        Get all callbacks for a specific event trigger

        :param trigger: The event that triggers the callback. 
        The event can be one of 'error', 'paused', 'resumed', 'cancelled', 'failed', 'started', 'stopped'.
        """
        trigger = CallbackTrigger(trigger)
        return list(filter(lambda cb: cb.task == self and cb.trigger == trigger, self.callbacks))


    def run_callbacks(self, trigger: str | CallbackTrigger, *args, **kwargs) -> None:
        """
        Run all callbacks for a specific event trigger

        :param trigger: The event that triggers the callback. 
        The event can be one of 'error', 'paused', 'resumed', 'cancelled', 'failed', 'started', 'stopped'.
        """
        trigger = CallbackTrigger(trigger)
        for callback in self.get_callbacks(trigger):
            callback(*args, **kwargs)
        return None


    def get_results(self) -> List[TaskResult]:
        """
        Returns an ordered list of results returned from each iteration of execution of this tasks.
        """
        return list(self.resultset) if self.resultset else []



# ------------- DECORATOR AND DECORATOR FACTORY ------------- #
def task(
    schedule: ScheduleType,
    manager: TaskManager,
    *,
    name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    execute_then_wait: bool = False,
    save_results: bool = False,
    resultset_size: Optional[int] = None,
    stop_on_error: bool = False,
    max_retries: int = 0,
    start_immediately: bool = True,
) -> Callable[[Callable], Callable[..., ScheduledTask]]:
    """
    Function decorator. Decorated function will return a new scheduled task when called.
    The returned task will be managed by the specified manager and will be executed according to the specified schedule.

    :param schedule: The schedule to run the task on.
    :param manager: The manager to execute the task.
    :param name: The preferred name for the task. If not specified, the name of the function will be used.
    :param tags: A list of tags to attach to the task. Tags can be used to group tasks together.
    :param execute_then_wait: If True, the function will be dry run first before applying the schedule.
    Also, if this is set to True, errors encountered on dry run will be propagated and will stop the task
    without retry, irrespective of `stop_on_error` or `max_retries`.
    :param save_results: If True, the results of each iteration of the task's execution will be saved.
    :param resultset_size: The maximum number of results to save. If the number of results exceeds this value, the oldest results will be removed.
    If `save_results` is False, this will be ignored. If this is not specified, the default value will be 10.
    :param stop_on_error: If True, the task will stop running when an error is encountered during its execution.
    :param max_retries: The maximum number of times the task will be retried consecutively after an error is encountered.
    :param start_immediately: If True, the task will start immediately after creation. 
    This is only applicable if the manager is already running.
    Otherwise, task execution will start when the manager starts executing tasks.
    """
    func_decorator = schedule(
        manager=manager,
        name=name,
        tags=tags,
        execute_then_wait=execute_then_wait,
        save_results=save_results,
        resultset_size=resultset_size,
        stop_on_error=stop_on_error,
        max_retries=max_retries,
        start_immediately=start_immediately,
    )
    return func_decorator



def make_task_decorator_for_manager(manager: TaskManager, /) -> Callable[..., Callable[[Callable], Callable[..., ScheduledTask]]]:
    """
    Convenience function for creating a task decorator for a given manager. This is useful when you want to create multiple
    tasks that are all managed by the same manager.

    :param manager: The manager to create the task decorator for.

    Example:
    ```python
    import pysche

    s = psyche.schedules
    manager = pysche.TaskManager(name="my_manager")
    task_for_manager = pysche.make_task_decorator_for_manager(my_manager)

    @task_for_manager(s.run_afterevery(seconds=10), ...)
    def function_one():
        pass
        
    
    @task_for_manager(s.run_at("20:00:00"), ...)
    def function_two():
        pass
        
    
    def main():
        manager.start() 

        task_one = function_one()
        task_two = function_two()

        manager.join()

    if __name__ = "__main__":
        main()
    ```
    """
    task_decorator_for_manager = functools.partial(task, manager=manager)

    @functools.wraps(task)
    def decorator_wrapper(
        schedule: ScheduleType,
        *,
        name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        execute_then_wait: bool = False,
        save_results: bool = False,
        resultset_size: Optional[int] = None,
        stop_on_error: bool = False,
        max_retries: int = 0,
        start_immediately: bool = True,
    ) -> Callable[[Callable], ScheduledTask]:
        return task_decorator_for_manager(
            schedule,
            name=name,
            tags=tags,
            execute_then_wait=execute_then_wait,
            save_results=save_results,
            resultset_size=resultset_size,
            stop_on_error=stop_on_error,
            max_retries=max_retries,
            start_immediately=start_immediately,
        )
    
    manager_name = underscore_string(manager.name)
    decorator_wrapper.__name__ = f"{task.__name__}_for_{manager_name}"
    decorator_wrapper.__qualname__ = f"{task.__qualname__}_for_{manager_name}"
    return decorator_wrapper
