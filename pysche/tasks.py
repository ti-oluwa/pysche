from __future__ import annotations
from collections import deque
import asyncio
import time
from typing import Callable, Any, Coroutine, List, Optional, Tuple, Dict, Union
import functools
import datetime
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo
from dataclasses import dataclass, field, KW_ONLY, InitVar
from asgiref.sync import sync_to_async


from .taskmanager import TaskManager
from .baseschedule import ScheduleType, NO_RESULT
from ._utils import get_datetime_now, underscore_string, underscore_datetime, generate_random_id
from .exceptions import StopTask, TaskError, TaskExecutionError
    


@dataclass(slots=True, frozen=True)
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

    # For sorting purposes
    def __gt__(self, other: TaskResult) -> bool:
        if not isinstance(other, TaskResult):
            return False
        return self.index > other.index and self.date_created > other.date_created



@dataclass(slots=True)
class ScheduledTask:
    """
    Runs a function on a specified schedule.

    Scheduled tasks always run concurrently in the background, not 
    necessarily in the order they were created.

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
    func: Union[Callable, Callable[..., Coroutine]]
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
    _exc: Exception = None # added to be used by schedule groups

    def __post_init__(self, start_immediately: bool) -> None:
        if not self.name:
            self.name = self.func.__name__
        
        func = self.func
        # If the function is not a coroutine, convert it to a coroutine
        if not asyncio.iscoroutinefunction(func):
            func = sync_to_async(func)
        self.func = self.stats_wrap(func)

        if self.tags is None:
            self.tags = []

        # Add task to manager
        self.manager._add_task(self)

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
        Returns True if task is currently being executed or is scheduled for execution.
        """
        return self._is_active and not self.stopped
    
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
    def stopped(self) -> bool:
        """
        Returns True if the task has stopped executing either because 
        it was cancelled, it failed or got an exception.
        """
        # Check if the future handling this task's coroutine is done or not
        future = self.manager._get_future(self.id)
        if future:
            return future.done()
        
        # If the future cannot be found and the task is already active, raise a runtime error
        if self.is_active:
            raise TaskError(
                f"Cannot find future for task '{self.name}[id={self.id}]' in '{self.manager.name}'. Tasks are out of sync with futures.\n"
            )
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
        if self.stopped:
            return "stopped"
        elif self.failed:
            return "failed"
        elif self.is_paused:
            return "paused"
        elif self.is_active:
            return "active"
        return "pending"


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
        if self.stopped:
            raise TaskExecutionError(f"Execution of {self.name} has already been stopped. {self.name} cannot be called.")

        if self.execute_then_wait is True:
            # Execute the task first if execute_then_wait is True.
            self.log("Task added for execution.")
            self._is_active = True
            self._last_executed_at = get_datetime_now(self.schedule.tz)
            await self.func(*self.args, **self.kwargs)

        schedule_func = self.schedule.make_schedule_func_for_task(self)
        err_count = 0
        # Prevents the task from running if the manager has not started (or is stopped)
        # or, if the task has failed or has been cancelled
        while self.manager._continue and (self.failed or self.stopped) is False:
            if not self.is_active:
                self.log("Task added for execution.")
                self._is_active = True

            try:
                result = await schedule_func(*self.args, **self.kwargs)
            except (
                SystemExit, KeyboardInterrupt, 
                asyncio.CancelledError, RuntimeError
            ):  
                break

            except StopTask as exc:
                msg = "Task stop requested."
                if exc.args:
                    msg = f"{msg} {exc.args[0]}"
                self.log(msg, level="INFO")
                break

            except Exception as exc:
                self._exc_count += 1
                self._errors.append(exc)
                self.log(f"{exc}", level="ERROR", exception=True)

                if self.stop_on_error is True or err_count >= self.max_retries:
                    self._failed = True
                    self.log("Task execution failed.", level="CRITICAL")
                    break
                
                err_count += 1
                self.log(f"Retrying task execution. Retries remaining: {self.max_retries - err_count}.", level="WARNING")
                continue

            else:
                self._exc_count += 1
                # Ensures that the maximum allowed retries is reset 
                # peradventure an error has occurred before, but it was resolved.
                err_count = 0
                # Add result to resultset if necessary
                self._add_result(result)
                continue

        # If task exits loop, task has stopped executing
        if self.is_active:
            self.log("Task execution stopped.")
            self._is_active = False
        return None
    

    def _add_result(self, result: Any) -> None:
        """
        Adds a result to the resultset if save_results is True
        
        Ignores the result if it is `NO_RESULT`
        """
        if self.save_results is True and result is not NO_RESULT:
            task_result = TaskResult(self, self.execution_count, result)
            self.resultset.append(task_result)
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
            raise NotImplementedError(
                f"Cannot compare '{self.__class__.__name__}' with '{other.__class__.__name__}'."
            )
        return self.id == other.id
    

    def __del__(self) -> None:
        # Stop the task if it is still active when it is being deleted
        self.stop()
        return None
    

    def stats_wrap(self, func: Callable[..., Coroutine]) -> Callable[..., Coroutine]:
        """Wrap coroutine function to log time stats"""
        is_coroutine = asyncio.iscoroutinefunction(func)
        if not is_coroutine:
            raise TypeError("func should be a coroutine function.")

        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            self.log("Executing task...")
            start_timestamp = time.perf_counter()
            
            result = await func(*args, **kwargs)
            end_timestamp = time.perf_counter()

            self.log(f"Task execution completed in {end_timestamp - start_timestamp:.8f} seconds.")
            return result
        
        return wrapper


    def start(self) -> None:
        """
        Start task execution.

        The task will not start if it is already active, paused, failed or stopped.
        Also, the task will not start until its manager has started.
        """
        if any((self.is_active, self.is_paused, self.failed, self.stopped)):
            raise TaskExecutionError(f"Cannot start '{self.name}'. '{self.name}' is {self.status}.")
        
        # Task starts automatically if manager has already started task execution 
        # else, it waits for the manager to start
        self.manager._run_in_workthread(self, append=True)
        if self.manager.is_active:
            # wait for task to start running if manager has start and is able to execute the task
            while not self.is_active:
                continue
        self.started_at = get_datetime_now(self.schedule.tz)
        self.log("Task execution started.")
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
        if self.stopped:
            raise TaskExecutionError(f"Cannot pause '{self.name}'. '{self.name}' has been stopped.")
        elif self.failed:
            raise TaskExecutionError(f"Cannot pause '{self.name}'. '{self.name}' has failed.")
        elif not self.is_active:
            raise TaskExecutionError(f"Cannot pause '{self.name}'. '{self.name}' has not started yet.")
        
        if self.is_paused:
            return
        self._is_paused = True
        self.log("Task execution paused.")
        return None
    

    def resume(self) -> None:
        """Resume paused task"""
        if not self.is_paused:
            # just ignore if task is not paused
            return
        self._is_paused = False
        self.log("Task execution resumed.")
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
    
    
    def stop(self) -> None:
        """
        Stop task. Stopping a task will invalidate task execution by the manager.

        Stopping a task will not prevent the manager from executing other tasks.

        PS: Always call `.join()` after calling `.stop()` to wait for the task to finish executing before proceeding.
        """
        if self.stopped:
            return
        
        task_future = self.manager._get_future(self.id)
        if task_future is None:
            if self.is_active or self.last_executed_at:
                # If task is being executed or has been executed before and a future cannot 
                # be found for the task, then `manager._futures` has been tampered with.
                # Raise a runtime error for this
                raise TaskError(
                    f"Cannot find future for task '{self.name}[id={self.id}]' in '{self.manager.name}'. Tasks are out of sync with futures.\n"
                )
        else:
            task_future.cancel()
        return None


    def stop_after(self, delay: int | float, /) -> ScheduledTask:
        """
        Creates a new task that stops this task after specified delay in seconds

        :param delay: The delay in seconds after which this task will be stopped
        :return: The created task
        """
        return self.manager.run_after(delay, self.stop, task_name=f"stop_{self.name}_after_{delay}s")
    

    def stop_at(self, time: str, /) -> ScheduledTask:
        """
        Creates a new task that stops this task at a specified time

        :param time: The time to stop this task. Time should be in the format 'HH:MM' or 'HH:MM:SS'
        :return: The created task
        '"""
        return self.manager.run_at(time, self.stop, task_name=f"stop_{self.name}_at_{underscore_datetime(time)}")
    

    def stop_on(self, datetime: str, /, tz: datetime.tzinfo | zoneinfo.ZoneInfo = None) -> ScheduledTask:
        """
        Creates a new task that stops this task at the specified datetime

        :param datetime: The datetime to stop this task. The datetime should be in the format 'YYYY-MM-DD HH:MM:SS'.
        :param tz: The timezone of the datetime. If not specified, the task's schedule timezone will be used.
        :return: The created task
        """
        tz = tz or self.schedule.tz
        return self.manager.run_on(datetime, self.stop, tz=tz, task_name=f"stop_{self.name}_on_{underscore_datetime(datetime)}")


    def get_results(self) -> List[TaskResult]:
        """
        Returns an ordered list of results returned from each iteration of execution of this tasks.
        """
        return list(self.resultset) if self.resultset else []


    def add_error_callback(self, callback: Callable) -> None:
        """
        Add an error callback to a task function. The callback is called on any exception in the task.

        :param callback: The callback to be called on exception in the task.
        The callback should accept two arguments: the task and the exception.

        NOTE: The callback is executed in a non-blocking manner.
        """
        if not callable(callback):
            raise TypeError("callback must be a callable.")
        if not asyncio.iscoroutinefunction(callback):
            callback = sync_to_async(callback)

        task_func = self.func

        @functools.wraps(task_func)
        async def wrapper(*args, **kwargs) -> Any:
            try:
                result = await task_func(*args, **kwargs)
            except Exception as exc:
                self.manager._run_in_workthread(callback, False, self, exc)
                raise exc
            return result
        
        self.func = wrapper
        return None


