import uuid
import asyncio
import sys
import time
from typing import Callable, Any, Iterable, Mapping
import functools
import traceback
import datetime

from .manager import TaskManager
from .schedules.bases import Schedule



class TaskDuplicationError(Exception):
    """Raised when maximum number of duplicate task for a `TaskManger` is exceeded"""


class _TaskLogger:
    """Task logger"""
    def __init__(self, task: "ScheduledTask"):
        self.task = task
        return None
    
    def __call__(self, msg: str, err_trace: str = None, *args, **kwargs):
        """Log message"""
        task_scheduler_name = self.task.manager.name
        if self.task.manager._logger:
            self.task.manager._logger.log(f"{task_scheduler_name} : [{self.task.name}] {msg}", *args, **kwargs)
            
            if err_trace:
                # If exception traceback is provided, 
                # do not log to console. Log to file only to prevent
                # leakage of sensitive information to console
                og_state = self.task.manager._logger.to_console
                self.task.manager._logger.to_console = False
                self.task.manager._logger.log(f"{task_scheduler_name} : [{self.task.name}] last error detail: \n{err_trace}\n", *args, **kwargs)
                self.task.manager._logger.to_console = og_state
        else:
            sys.stderr.write(f"{task_scheduler_name} : [{self.task.name}] {msg}")
        return None



class ScheduledTask:
    """Task that runs a function on a specified schedule"""
    __init_manager__ = None

    def __init__(
            self,
            func: Callable,
            schedule: Schedule,
            manager: TaskManager,
            *,
            args: Iterable[Any] = (),
            kwargs: Mapping[str, Any] = {},
            name: str = None,
            do_then_wait: bool = False,
            stop_on_error: bool = False,
            max_retry: int = 0,
            start_immediately: bool = True,
        ):
        """
        Create a new scheduled task.

        :param func: The function to be scheduled.
        :param schedule: The schedule for the task.
        :param manager: The manager to execute the task.
        :param args: The arguments to be passed to the function.
        :param kwargs: The keyword arguments to be passed to the function.
        :param name: The name of the task.
        :param do_then_wait: If True, the function will be run first before waiting for the schedule.
        :param stop_on_error: If True, the task will stop running when an error is encountered.
        :param max_retry: The maximum number of times the task will be retried when an error is encountered.
        :param start_immediately: If True, the task will start immediately after creation. This is only applicable if the manager is already running.
        Otherwise, the task will start when the manager is started.
        """
        if not callable(func):
            raise TypeError("Invalid type for 'func'. Should be a callable")
        
        if not isinstance(manager, TaskManager):
            raise TypeError("Invalid type for 'manager'. Should be TaskScheduler")
        
        self._id = uuid.uuid4().hex[-6:]
        self.__init_manager__ = manager
        func = self._wrap_func_for_time_stats(func)
        self.func = self.manager._make_asyncable(func)
        self.name = name or self.func.__name__
        self.__name__ = self.name
        self.__qualname__ = self.name

        if abs(self.manager.max_duplicates) == self.manager.max_duplicates: # If positive
            siblings = self.manager.get_tasks(self.name)
            if len(siblings) >= self.manager.max_duplicates:
                raise TaskDuplicationError(f"'{self.name}' has reached the maximum number of duplicates allowed by '{self.manager.name}' - {self.manager.max_duplicates}.")
        
        self.args = args
        self.kwargs = kwargs
        self.do_then_wait = do_then_wait
        self.stop_on_error = stop_on_error
        self.max_retry = max_retry
        self.schedule = schedule
        self._is_running = False
        self._is_paused = False
        self._failed = False
        self._errors = []
        self._last_ran_at: datetime.datetime = None
        self._logger = _TaskLogger(self)
        self.manager._tasks.append(self)
        if start_immediately:
            self.start()
        return None
    
    @property
    def id(self):
        return self._id

    @property
    def manager(self):
        return self.__init_manager__
    
    @property
    def log(self):
        """Returns task logger"""
        return self._logger
    
    @property
    def is_running(self):
        """Returns True if task is running"""
        return self._is_running
    
    @property
    def is_paused(self):
        """Returns True if task is paused"""
        return self._is_paused
    
    @property
    def failed(self):
        """Returns True if task failed"""
        return self._failed
    
    @property
    def errors(self):
        """Returns a list of errors"""
        return self._errors
    
    @property
    def last_ran_at(self):
        """Returns the last time the task ran"""
        return self._last_ran_at
    
    @property
    def status(self):
        """Returns the status of the task"""
        return "failed" if self.failed else "paused" if self.is_paused else "running" if self.is_running else "stopped" if self.last_ran_at else "pending"
    

    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == "__init_manager__" and self.__init_manager__ is not None:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '__init_manager__'")
        return super().__setattr__(__name, __value)
    

    async def __call__(self):
        """Returns task coroutine"""
        schedule_func = self.schedule._make_schedule_func_for_task(self)
        err_count = 0
        while self.manager._continue and not self.failed: # The prevents the task from running when the manager has not been started (or is stopped)
            if self.is_paused:
                await asyncio.sleep(0.1)
                continue
            else:
                if not self.is_running:
                    self.log(f"Started.\n")
                    self._is_running = True

            try:
                await schedule_func(*self.args, **self.kwargs)
            except (SystemExit, KeyboardInterrupt, asyncio.CancelledError, RuntimeError) as exc:
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
        if self.is_running:
            self.log("Stopped.\n")
            self._is_running = False
        return None

    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.status} name='{self.name}' func='{self.func.__name__}'>"
    

    def _wrap_func_for_time_stats(self, func: Callable):
        """Wrap function to log time stats"""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.log(f"Running...\n")
            start_timestamp = time.time()
            r = func(*args, **kwargs)
            self.log(f"Completed in {time.time() - start_timestamp :.4f} seconds.\n")
            return r
        return wrapper


    def update(self, **kwargs):
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
    

    def start(self):
        """
        Start running task. You cannot start a failed task. Call `rerun` instead.

        The task will not start if it is already running, paused or failed.
        Also, the task will not start until its manager has been started.
        """
        if not self.is_running and not self.is_paused and not self.failed:
            # Task starts automatically if manager is already running 
            # else, it waits for the manager to start
            future = self.manager._make_future(self)
            if not getattr(future, "id", None) == self.id:
                raise RuntimeError(f"Invalid Future object for '{self.name}'.\n")
            
            self.manager._futures.append(future)
            # Give the task a chance to start running
            time.sleep(0.01)
        return None


    def rerun(self):
        """Re-run failed task"""
        if not self.failed:
            raise RuntimeWarning(f"Cannot rerun '{self.name}'. '{self.name}' has not failed.")
        self._failed = False
        self.manager._make_future(self)
        return None
    

    def join(self):
        """Wait for task to finish running before proceeding"""
        while self.is_running:
            try:
                continue
            except KeyboardInterrupt:
                break
        return None
    

    def pause(self):
        """Pause task. Stops task from running, temporarily"""
        if self.is_paused:
            raise RuntimeWarning(f"Cannot pause '{self.name}'. '{self.name}' is already paused.")
        self._is_paused = True
        self.log("Paused.\n")
        return None
    

    def resume(self):
        """Resume task"""
        if not self.is_paused:
            return
        self._is_paused = False
        self.log("Resumed.\n")
        return None
    

    def pause_after(self, delay: int | float):
        """Pause task after specified delay in seconds"""
        self.manager.run_after(delay, self.pause, task_name=f"pause_{self.name}_after_{delay}s")
        return None

    
    def pause_for(self, seconds: int | float):
        """Pause task for a specified number of seconds then resume"""
        self.pause()
        self.manager.run_after(seconds, self.resume, task_name=f"pause_{self.name}_for_{seconds}")
        return None


    def pause_until(self, time: str):
        """Pause task then resume at specified time. Time should be in the format 'HH:MM:SS'"""
        self.pause()
        self.manager.run_at(time, self.resume, task_name=f"pause_{self.name}_until_{time}")
        return None
    

    def pause_at(self, time: str):
        """Pause task at a specified time. Time should be in the format 'HH:MM:SS'"""
        self.manager.run_at(time, self.pause, task_name=f"pause_{self.name}_at_{time}")
        return None
    

    def update_after(self, delay: int | float, **kwargs):
        """
        Delay an update. Update task after a specified number of seconds

        Never attempt to update the manager. Doing so will result in unexpected behaviour.
        """
        self.manager.run_after(delay, self.update, task_name=f"update_{self.name}_after_{delay}s", kwargs=kwargs)
        return None


    def update_at(self, time: str, **kwargs):
        """
        Delay an update. Update task at a specified time

        Never attempt to update the manager. Doing so will result in unexpected behaviour.
        """
        self.manager.run_at(time, lambda: self.update(**kwargs), task_name=f"update_{self.name}_at_{time}")
        return None

    
    def cancel(self):
        """
        Cancel task. Cancelling a task will stop the task from running permanently.
        After this, no method can be called on the task as the resources used by the task will be freed.

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
            self.join() # Wait for task to finish running, if it is still running
            self.manager._futures.remove(task_future)
            del task_future

        self.manager._tasks.remove(self)
        self.log("Cancelled.\n")
        del self
        return None


    def cancel_after(self, delay: int | float):
        """Cancel task after specified delay in seconds"""
        self.manager.run_after(delay, self.cancel, task_name=f"cancel_{self.name}_after_{delay}s")
        return None
    

    def cancel_at(self, time: str):
        """Cancel task at a specified time. Time should be in the format 'HH:MM:SS'"""
        self.manager.run_at(time, self.cancel, task_name=f"cancel_{self.name}_at_{time}")
        return None


    
def scheduledtask(
        schedule: Schedule,
        manager: TaskManager,
        *,
        name: str = None,
        do_then_wait: bool = False,
        stop_on_error: bool = False,
        max_retry: int = 0,
        start_immediately: bool = True,
    ):
    """
    Decorator to schedule a function to run on a specified schedule.

    :param schedule: The schedule for the task.
    :param manager: The manager to execute the task.
    :param name: The name of the task.
    :param do_then_wait: If True, the function will be run first before waiting for the schedule.
    :param stop_on_error: If True, the task will stop running when an error is encountered.
    :param max_retry: The maximum number of times the task will be retried when an error is encountered.
    :param start_immediately: If True, the task will start immediately after creation. This is only applicable if the manager is already running.
    If the manager is not running, the task will start when the manager is started.
    """
    decorator = schedule(
        manager=manager,
        name=name,
        do_then_wait=do_then_wait,
        stop_on_error=stop_on_error,
        max_retry=max_retry,
        start_immediately=start_immediately,
    )
    return decorator

