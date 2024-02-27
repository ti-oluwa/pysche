from __future__ import annotations
import uuid
import asyncio
import time
from typing import Callable, Any, Iterable, Mapping, List
import functools
import traceback
import datetime

from .manager import TaskManager
from .bases import ScheduleType
from ._utils import SetOnceDescriptor



class TaskDuplicationError(Exception):
    """Raised when maximum number of duplicate task for a `TaskManger` is exceeded"""


class _TaskLogger:
    """Scheduled task logger"""
    def __init__(self, task: ScheduledTask):
        self.task = task
        return None
    
    def __call__(self, msg: str, err_trace: str = None, *args, **kwargs):
        """Log message"""
        task_manager = self.task.manager
        task_manager.log(f"{task_manager.name} : [{self.task.name}] {msg}", *args, **kwargs)
        
        if err_trace:
            # If exception traceback is provided, 
            # do not log to console. Log to file only to prevent
            # leakage of sensitive information to console
            og_state = task_manager.log_to_console
            task_manager.log_to_console = False
            task_manager.log(f"{task_manager.name} : [{self.task.name}] last error detail: \n{err_trace}\n", *args, **kwargs)
            task_manager.log_to_console = og_state
        return None



class ScheduledTask:
    """Task that runs a function on a specified schedule"""
    manager = SetOnceDescriptor(attr_type=TaskManager)
    __slots__ = (
        "_id", "func", "name", "args", "kwargs", "execute_then_wait", "stop_on_error", "max_retry", "schedule",
        "_is_running", "_is_paused", "_failed", "_cancelled", "_errors", "_last_ran_at", "_logger", "_manager",
        "__dict__", "__weakref__"
    )

    def __init__(
        self,
        func: Callable,
        schedule: ScheduleType,
        manager: TaskManager,
        *,
        args: Iterable[Any] = (),
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
        :param args: The arguments to be passed to the function.
        :param kwargs: The keyword arguments to be passed to the function.
        :param name: The name of the task.
        :param execute_then_wait: If True, the function will be run first before waiting for the schedule.
        :param stop_on_error: If True, the task will stop running when an error is encountered.
        :param max_retry: The maximum number of times the task will be retried when an error is encountered.
        :param start_immediately: If True, the task will start immediately after creation. This is only applicable if the manager is already running.
        Otherwise, the task will start when the manager is started.
        """
        if not callable(func):
            raise TypeError("Invalid type for 'func'. Should be a callable")
        
        self._id = uuid.uuid4().hex[-6:]
        self.manager = manager
        func = self._wrap_func_for_time_stats(func)
        self.func = self.manager._make_asyncable(func)
        self.name = name or self.func.__name__

        if abs(self.manager.max_duplicates) == self.manager.max_duplicates: # If positive
            siblings = self.manager.get_tasks(self.name)
            if len(siblings) >= self.manager.max_duplicates:
                raise TaskDuplicationError(
                    f"'{self.name}' has reached the maximum number of duplicates allowed by '{self.manager.name}' - {self.manager.max_duplicates}."
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
        self._cancelled = False
        self._errors = []
        self._last_ran_at: datetime.datetime = None
        self._logger = _TaskLogger(self)
        self.manager._tasks.append(self)
        if start_immediately:
            self.start()
        return None
    
    @property
    def id(self) -> str:
        return self._id
    
    @property
    def log(self) -> _TaskLogger:
        """Returns task logger"""
        return self._logger
    
    @property
    def is_running(self) -> bool:
        """Returns True if task is currently running"""
        return self._is_running
    
    @property
    def is_paused(self) -> bool:
        """Returns True if task is currently paused"""
        return self._is_paused
    
    @property
    def failed(self) -> bool:
        """Returns True if task execution failed at some point"""
        return self._failed
    
    @property
    def cancelled(self) -> bool:
        """Returns True if the task execution has been cancelled"""
        return self._cancelled
    
    @property
    def errors(self) -> List[Exception]:
        """Returns a list of errors"""
        return self._errors
    
    @property
    def last_ran_at(self) -> datetime.datetime | None:
        """Returns the last time the task ran"""
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
    

    async def __call__(self) -> None:
        """Returns task coroutine"""
        if self.cancelled:
            raise RuntimeError(f"{self.name} has already been cancelled. {self.name} cannot be called.")

        if self.execute_then_wait is True:
            # Execute the task first if execute_then_wait is True.
            self.log(f"Task execution started.\n")
            self._is_running = True
            await self.func(*self.args, **self.kwargs)

        schedule_func = self.schedule.make_schedule_func_for_task(self)
        err_count = 0
        while self.manager._continue and not (self.failed or self.cancelled): # The prevents the task from running when the manager has not been started (or is stopped)
            if not self.is_running:
                self.log(f"Task execution started.\n")
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
                self.log(f"{exc}\n", err_trace=traceback.format_exc(), level="ERROR")

                if self.stop_on_error or err_count >= self.max_retry:
                    self._failed = True
                    self._is_running = False
                    self.log("Failed.\n", level="CRITICAL")
                    break
                err_count += 1
                continue
        
        # If task exits loop, task has stopped executing
        if self.is_running:
            self.log("Task execution stopped.\n")
            self._is_running = False
        return None

    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.status} name='{self.name}' func='{self.func.__name__}'>"
    

    def _wrap_func_for_time_stats(self, func: Callable) -> Callable:
        """Wrap function to log time stats"""
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            self.log(f"Running...\n")
            start_timestamp = time.time()
            r = func(*args, **kwargs)
            self.log(f"Completed in {time.time() - start_timestamp :.4f} seconds.\n")
            return r
        return wrapper


    def update(self, **kwargs) -> None:
        """
        Update task (Instantiation parameters)
        """
        if kwargs.get("func", None):
            kwargs["func"] = self.manager._make_asyncable(kwargs["func"])
            kwargs["func"].has_been_done_first = self.func.has_been_done_first
            if hasattr(self.func, "has_run"):
                kwargs["func"].has_run = self.func.has_run
        for key, val in kwargs.items():
            setattr(self, key, val)
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
        """Resume task"""
        if not self.is_paused:
            return
        self._is_paused = False
        self.log("Task execution resumed.\n")
        return None
    

    def pause_after(self, delay: int | float) -> ScheduledTask:
        """Pause task after specified delay in seconds"""
        return self.manager.run_after(delay, self.pause, task_name=f"pause_{self.name}_after_{delay}s")


    def pause_for(self, seconds: int | float) -> ScheduledTask:
        """Pause task for a specified number of seconds then resume"""
        self.pause()
        return self.manager.run_after(seconds, self.resume, task_name=f"resume_{self.name}_after_{seconds}s")


    def pause_until(self, time: str) -> ScheduledTask:
        """Pause task then resume at specified time. Time should be in the format 'HH:MM:SS'"""
        self.pause()
        return self.manager.run_at(time, self.resume, task_name=f"resume_{self.name}_at_{time}")
    

    def pause_at(self, time: str) -> None:
        """Pause task at a specified time. Time should be in the format 'HH:MM:SS'"""
        self.manager.run_at(time, self.pause, task_name=f"pause_{self.name}_at_{time}")
        return None
    

    def update_after(self, delay: int | float, **kwargs) -> ScheduledTask:
        """
        Delay an update. Update task after a specified number of seconds

        Never attempt to update the manager. Doing so will result in unexpected behaviour.
        """
        return self.manager.run_after(delay, self.update, task_name=f"update_{self.name}_after_{delay}s", kwargs=kwargs)


    def update_at(self, time: str, **kwargs) -> ScheduledTask:
        """
        Delay an update. Update task at a specified time

        Never attempt to update the manager. Doing so will result in unexpected behaviour.
        """
        return self.manager.run_at(time, lambda: self.update(**kwargs), task_name=f"update_{self.name}_at_{time}")

    
    def cancel(self) -> None:
        """
        Cancel task. Cancelling a task will invalidate task execution by the manager.

        Note that cancelling a task will not stop the manager from executing other tasks.
        """
        task_future = self.manager._get_future(self.id)
        if task_future is None:
            if self.is_running or self.last_ran_at:
                # If task is running or has run before and a future cannot 
                # be found for the task then `manager._futures` has been tampered with.
                # Raise a runtime error for this
                raise RuntimeError(f"{self.__class__.__name__}: Cannot find future '{self.id}' in manager. '_tasks' is out of sync with '_futures'.\n")
        else: 
            self.manager._loop.call_soon_threadsafe(task_future.cancel)
            self.manager._futures.remove(task_future)
            task_future.__del__()

        self.manager._tasks.remove(self)
        self.join()
        self._cancelled = True
        self.log("Task cancelled.\n")
        return None


    def cancel_after(self, delay: int | float) -> ScheduledTask:
        """Cancel task after specified delay in seconds"""
        return self.manager.run_after(delay, self.cancel, task_name=f"cancel_{self.name}_after_{delay}s")
    

    def cancel_at(self, time: str) -> ScheduledTask:
        """Cancel task at a specified time. Time should be in the format 'HH:MM:SS'"""
        return self.manager.run_at(time, self.cancel, task_name=f"cancel_{self.name}_at_{time}")


    
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
    Decorator to schedule a function to run on a specified schedule.

    :param schedule: The schedule for the task.
    :param manager: The manager to execute the task.
    :param name: The name of the task.
    :param execute_then_wait: If True, the function will be run first before waiting for the schedule.
    :param stop_on_error: If True, the task will stop running when an error is encountered.
    :param max_retry: The maximum number of times the task will be retried when an error is encountered.
    :param start_immediately: If True, the task will start immediately after creation. This is only applicable if the manager is already running.
    If the manager is not running, the task will start when the manager is started.
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

