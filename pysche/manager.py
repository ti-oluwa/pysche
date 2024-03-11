from __future__ import annotations
from collections.abc import Coroutine
import datetime
import logging
from collections import deque
import threading
import time
from typing import Any, Callable, Dict, Sequence, List, Mapping, Optional
import asyncio
import functools
from concurrent.futures import CancelledError, ThreadPoolExecutor


from .utils import (
    _RedirectStandardOutputStream, parse_datetime, 
    get_datetime_now, underscore_datetime
)
from .exceptions import UnregisteredTask
from .logging import get_logger



class TaskManager:
    """
    Manages the execution of scheduled tasks.
    """
    __slots__ = (
        "name", "_tasks", "_futures", "_continue", "_loop", 
        "_workthread", "_executor", "max_duplicates", "logger"
    )

    def __init__(self, name: str = None, max_duplicates: int = -1, log_to: str = None):
        """
        Creates a new task manager.

        :param name: The name of the task manager. Defaults to the class name and a unique ID.
        :param max_duplicates: The maximum number of task duplicates that can be managed at a time.
        Set to a negative number to allow unlimited instances.
        :param log_to: Path to log file. If provided, task execution details are logged to the file.

        Task execution details are always written to the console. Create a subclass and set `log_to_console` to False
        to disable writing to the console.
        """
        from .tasks import ScheduledTask
        if not isinstance(max_duplicates, int):
            raise TypeError("Max instances must be an integer")
        
        self.name: str = name or f"{self.__class__.__name__.lower()}{str(id(self))[-6:]}"
        # Used deque to allow for thread-safety and O(1) time complexity when removing tasks from the list
        self._tasks: deque[ScheduledTask] = deque([])
        self._futures: deque[asyncio.Future] = deque([])
        self._continue: bool = False
        self._loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self._workthread: threading.Thread | None = None
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix=f"{self.name}_sync_to_async_executor")
        self.max_duplicates = max_duplicates
        self.logger = get_logger(
            name=f"{self.name}_logger",
            logfile_path=log_to,
            base_level="DEBUG",
            date_format="%Y-%m-%d %H:%M:%S (%z)"
        )
        return None
    
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} '{self.status}' name='{self.name}' task_count={len(self.tasks)}>"
    
    
    def __del__(self) -> None:
        self.shutdown()
        return None
    

    def __hash__(self) -> int:
        return hash(id(self))
    
    
    def __eq__(self, other: TaskManager) -> bool:
        if not isinstance(other, TaskManager):
            return False
        return hash(self) == hash(other)
    

    def __gt__(self, other: TaskManager) -> bool:
        if not isinstance(other, TaskManager):
            return False
        return len(self.tasks) > len(other.tasks)
    
    
    def __lt__(self, other: TaskManager) -> bool:
        return not self.__gt__(other)


    @property
    def has_started(self) -> bool:
        """Returns True if the task manager has started executing tasks"""
        return self._continue is True and self._loop.is_running()
    
    @property
    def tasks(self):
        """Returns a list of scheduled tasks being managed currently"""
        return [ task for task in self._tasks if task.manager == self ]
    
    @property
    def errors(self) -> Dict[str, List[Exception]]:
        """
        Returns a dictionary of errors/exceptions encountered while executing tasks.

        Errors/exceptions are mapped to the name of the task in which they occurred.
        """
        return { task.name : task.errors for task in self.tasks[:] if task.errors }
    
    @property
    def has_errors(self) -> bool:
        """Returns True if there was any error encountered while executing tasks"""
        return bool(self.errors)
    
    @property
    def is_occupied(self) -> bool:
        """
        Returns True if the task manager is currently has any active task (paused or not)
        """
        # iterate over a copy of the tasks list, self.tasks[:], instead of the list itself to avoid
        # runtime error / race conditions that may occur when trying modify the task list while it is been
        # iterated over.
        return self.has_started and any([ task.is_active for task in self.tasks[:] ])
    
    @property
    def status(self):
        """Returns the status of the task manager - 'occupied' or 'idle'"""
        return "occupied" if self.is_occupied else "idle"

    
    def _start_execution(self) -> None:
        """Start the event loop in the work thread and begin executing all scheduled tasks"""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()
        return None
        
        
    def _make_asyncable(self, func: Callable) -> Callable[..., Coroutine[Any, Any, Any]]:
        """
        Converts a blocking function to an non-blocking function

        :args: positional arguments to pass to function
        :kwargs: keyword arguments to pass to function
        """
        if not callable(func):
            raise TypeError("'func' must be a callable")
        
        @functools.wraps(func)
        async def async_func(*args, **kwargs) -> Any:
            try:
                # Use in case the function writes to the standard output stream in the background
                with _RedirectStandardOutputStream():
                    # Ensure that any exception raised by the function is caught and re-raised in the main thread
                    f = self._loop.run_in_executor(self._executor, lambda: func(*args, **kwargs))
                    return await f
            except CancelledError as exc:
                raise asyncio.CancelledError(exc)
            
        return async_func
    

    def _make_future(self, scheduledtask) -> asyncio.Future:
        """
        Creates and returns an `asyncio.Future` for the scheduled task.
        Adds the future to the list of futures handled by this manager
        """
        # Used `run_coroutine_threadsafe` to ensure that tasks(futures) are 
        # run concurrently and in a thread-safe manner once the loop starts running
        future = asyncio.run_coroutine_threadsafe(scheduledtask(), self._loop)
        future.name = scheduledtask.name
        future.id = scheduledtask.id
        self._futures.append(future)
        return future
    

    def _get_futures(self, name: str) -> List[asyncio.Future]:
        """Returns a list of futures with the specified name"""
        matches = []
        for future in self._futures:
            if future.name == name:
                matches.append(future)
        return matches

    
    def _get_future(self, future_id: str) -> asyncio.Future | None:
        """Returns future with specified ID if any"""
        for future in self._futures:
            if future.id == future_id:
                return future
        return None
       

    def start(self) -> None:
        """Start executing all scheduled tasks"""
        if self.has_started:
            raise RuntimeError(f"{self.name} has already started task execution.\n")
        
        self._continue = True # allows all ScheduleTasks to run
        self._workthread = threading.Thread(target=self._start_execution, name=f"{self.name}_workthread", daemon=True)
        self._workthread.start()
        if self.tasks:
            # If there are any tasks being managed wait for manager to get busy with them
            while not self.is_occupied:
                continue
        else:
            # If not, just wait for manager to start successfully
            while not self.has_started:
                continue
        return None
        

    def join(self) -> None:
        """
        Wait for this manager to finish executing all scheduled tasks.
        This method blocks the current thread until all tasks are no longer active/running.
        """
        try:
            while self.is_occupied:
                time.sleep(0.001)
                continue
        except (KeyboardInterrupt, SystemExit):
            self.stop()
        return None
      
    
    def stop(self) -> None:
        """
        Cancel all scheduled tasks and stop the manager from executing any more tasks.
        """
        if not self.has_started:
            raise RuntimeError(f"{self.name} has not started task execution yet.\n")

        self._continue = False # breaks outermost while loop in all ScheduleTasks

        # Cancel all tasks which eventually cancels all futures before stopping loop
        # This helps avoid the warning message that is thrown by the loop when it is stopped
        for task in self.tasks:
            task.cancel()

        self._loop.call_soon_threadsafe(self._loop.stop)

        self._workthread.join()
        del self._workthread
        self._workthread = None
        return None
    

    def shutdown(self) -> None:
        """
        Cancel all tasks and clean up resources.

        This method should only be called when the task manager is no longer needed
        as it indefinitely stops the manager from executing any task.
        """
        if self.is_occupied:
            self.stop() # stop all task execution, if the manager is occupied with any

        self._loop.close()
        self._executor.shutdown(wait=True, cancel_futures=True)
        del self._executor
        del self._loop
        return None


    def is_managing(self, name_or_id: str, /) -> bool:
        """Check if the task manager is managing a task with the specified name or ID"""
        for task in self.tasks:
            if task.name == name_or_id or task.id == name_or_id:
                return True
        return False
    

    def get_tasks(self, name_or_tag: str, /):
        """Returns a list of tasks with the specified name or tags"""
        from .tasks import ScheduledTask
        matches: List[ScheduledTask] = []

        for task in self.tasks:
            if task.name == name_or_tag or name_or_tag in task.tags:
                matches.append(task)
        return matches
    

    def get_task(self, task_id: str):
        """Returns the task with the specified ID if any else return None"""
        for task in self.tasks:
            if task.id == task_id:
                return task
        return None

    
    def run_after(
        self, 
        delay: int | float, 
        func: Callable, 
        /, *,
        args: Sequence[Any] = (), 
        kwargs: Mapping[str, Any] = {}, 
        task_name: str = None
    ):
        """
        Creates a task that runs a function once after a specified delay in seconds.

        :param delay: The number of seconds to wait before running the function
        :param func: The function to run as a task
        :param args: The arguments to pass to the function
        :param kwargs: The keyword arguments to pass to the function
        :param task_name: The name to give the task created to run the function. 
        This can make it easier to identify the task in logs in the case of errors.
        :return: The created task
        """
        from .schedules import RunAfterEvery
        from .tasks import ScheduledTask
        # Wraps function such that the function runs once and then the task is cancelled
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs) -> Any:
            result = func(*args, **kwargs)
            wrapped_func.task.cancel()
            return result
        
        task = ScheduledTask(
            func=wrapped_func,
            schedule=RunAfterEvery(seconds=delay),
            manager=self,
            args=args,
            kwargs=kwargs,
            name=task_name or f"run_{func.__name__}_after_{delay}s",
            stop_on_error=True,
            max_retry=0,
            start_immediately=False,
        )
        wrapped_func.task = task
        task.start()
        return task
    

    def run_on(
        self, 
        datetime: str, 
        func: Callable, 
        /, *,
        tz: str | datetime.tzinfo = None,
        args: Sequence[Any] = (), 
        kwargs: Mapping[str, Any] = {}, 
        task_name: str = None
    ):
        """
        Creates a task that runs the function on a specified datetime.

        :param datetime: The datetime to run the function. It should be in the format 'YYYY-MM-DD HH:MM:SS'
        :param func: The function to run as a task
        :param tz: The timezone to use. Defaults to local timezone.
        :param args: The arguments to pass to the function
        :param kwargs: The keyword arguments to pass to the function
        :param task_name: The name to give the task created to run the function. 
        This can make it easier to identify the task in logs in the case of errors.
        :return: The created task
        """
        dt = parse_datetime(datetime, tz)
        now = get_datetime_now(tz)
        if dt < now:
            raise ValueError("datetime cannot be in the past")
        timedelta = dt - now
        
        from .schedules import RunAfterEvery
        from .tasks import ScheduledTask
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs) -> Any:
            result = func(*args, **kwargs)
            wrapped_func.task.cancel()
            return result
        
        task = ScheduledTask(
            func=wrapped_func,
            schedule=RunAfterEvery(seconds=timedelta.total_seconds()),
            manager=self,
            args=args,
            kwargs=kwargs,
            name=task_name or f"run_{func.__name__}_on_{dt}",
            stop_on_error=True,
            max_retry=0,
            start_immediately=False,
        )
        wrapped_func.task = task
        task.start()
        return task
    

    def run_at(
        self, 
        time: str, 
        func: Callable, 
        /, *,
        tz: str | datetime.tzinfo = None,
        args: Sequence[Any] = (), 
        kwargs: Mapping[str, Any] = {}, 
        task_name: str = None
    ):
        """
        Creates a task that runs a function once at a specified time.

        :param time: The time to run the function. Must be in the format 'HH:MM:SS'
        :param func: The function to run as a task
        :param tz: The timezone to use. Defaults to local timezone.
        :param args: The arguments to pass to the function
        :param kwargs: The keyword arguments to pass to the function
        :param task_name: The name to give the task created to run the function. 
        This can make it easier to identify the task in logs in the case of errors.
        :return: The created task
        """
        from .schedules import RunAt
        from .tasks import ScheduledTask
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs) -> Any:
            result = func(*args, **kwargs)
            wrapped_func.task.cancel()
            return result
        
        task = ScheduledTask(
            func=wrapped_func,
            schedule=RunAt(time=time, tz=tz),
            manager=self,
            args=args,
            kwargs=kwargs,
            name=task_name or f"run_{func.__name__}_at_{time}",
            stop_on_error=True,
            max_retry=0,
            start_immediately=False,
        )
        wrapped_func.task = task
        task.start()
        return task


    def stop_after(self, delay: int | float, /):
        """
        Creates a task that stops the execution of all scheduled tasks after a specified delay in seconds

        :param delay: The number of seconds to wait before stopping the execution of all tasks
        :return: The created task
        """
        if not self.has_started:
            raise RuntimeError(f"{self.name} has not started task execution yet.\n")
        return self.run_after(delay, self.stop, task_name=f"stop_{self.name}_after_{delay}seconds")
    

    def stop_at(self, time: str, /):
        """
        Creates a task that stops the execution of all scheduled tasks at the specified time

        :param time: The time to stop the execution of all tasks. Must be in the format 'HH:MM:SS'
        :return: The created task
        """
        if not self.has_started:
            raise RuntimeError(f"{self.name} has not started task execution yet. Y\n")
        return self.run_at(time, self.stop, task_name=f"stop_{self.name}_at_{underscore_datetime(time)}")
    

    def stop_on(self, datetime: str, /):
        """
        Creates a task that stops the execution of all scheduled tasks at the specified datetime

        :param datetime: The datetime to stop the execution of all tasks. Must be in the format 'YYY-MM-DD HH:MM:SS'
        :return: The created task
        """
        if not self.has_started:
            raise RuntimeError(f"{self.name} has not started task execution yet. Y\n")
        return self.run_on(datetime, self.stop, task_name=f"stop_{self.name}_on_{underscore_datetime(datetime)}")
    

    def cancel_task(self, name_or_id: str, /, *, wait: bool = True) -> None:
        """Cancel all tasks with the specified name or ID"""
        if not self.is_managing(name_or_id):
            raise UnregisteredTask(f"Task '{name_or_id}' is not registered with {self.name}")
        
        for task in self.tasks:
            if task.name != name_or_id and task.id != name_or_id:
                continue
            task.cancel(wait=wait)
        return None
    

    def get_running_tasks(self):
        """Returns a list of tasks that are currently running and not paused"""
        from .tasks import ScheduledTask
        running_tasks: List[ScheduledTask] = []

        for task in self.tasks:
            if task.is_active:
                running_tasks.append(task)
        return running_tasks
    

    def get_paused_tasks(self):
        """Returns a list of tasks that are currently paused"""
        from .tasks import ScheduledTask
        paused_tasks: List[ScheduledTask] = []

        for task in self.tasks:
            if task.is_paused:
                paused_tasks.append(task)
        return paused_tasks
    

    def get_failed_tasks(self):
        """Returns a list of tasks that failed"""
        from .tasks import ScheduledTask
        failed_tasks: List[ScheduledTask] = []

        for task in self.tasks:
            if task.failed:
                failed_tasks.append(task)
        return failed_tasks
    

    def log(self, msg: object, level: str = "INFO", **kwargs) -> None:
        """
        Log message to console and/or to log file.

        :param msg: The message to log
        :param level: The log level. Defaults to "INFO"
        :param kwargs: Additional keyword arguments to pass to `logger.log`
        """
        level = getattr(logging, level.upper())
        try:
            return self.logger.log(level, msg, **kwargs)
        except ImportError:
            # Just to avoid the error message that is thrown by the rich library
            # Anytime logging is interrupted by a system exit or keyboard interrupt
            pass
    
    
    def newtask(
        self,
        schedule,
        func: Optional[Callable] = None,
        *,
        name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        execute_then_wait: bool = False,
        stop_on_error: bool = False,
        max_retry: int = 0,
        start_immediately: bool = True,
    ):
        """
        Takes a function to be scheduled and returns a version of the function such that when called,
        it creates a new scheduled task and returns it. 

        This method can also be used as a function decorator. The decorated function returns a new scheduled task when called.
        The returned task is managed by this manager and is executed on the specified schedule.

        :param schedule: The schedule to run the task on.
        :param func: The function to schedule. If not specified, the function being decorated will be scheduled.
        :param name: The preferred name for this task. If not specified, the name of the function will be used.
        :param tags: A list of tags to attach to the task. Tags can be used to group tasks together.
        :param execute_then_wait: If True, the function will be dry run first before applying the schedule.
        Also, if this is set to True, errors encountered on dry run will be propagated and will stop the task
        without retry, irrespective of `stop_on_error` or `max_retry`
        :param stop_on_error: If True, the task will stop running when an error is encountered during its execution.
        :param max_retry: The maximum number of times the task will be retried after an error is encountered.
        :param start_immediately: If True, the task will start immediately after creation. 
        This is only applicable if the manager is already running.
        Otherwise, task execution will start when the manager starts executing tasks.

        #### Usage as a function decorator:
        ```python
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules

        @manager.newtask(s.RunAfterEvery(seconds=2))
        def speak(msg):
            print(msg)

        task = speak('Hello!')
        ```

        #### Usage as a regular method:
        ```python
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules

        speak = manager.newtask(s.RunAfterEvery(seconds=2), speak)
        task = speak('Hey!!')
        ```
        """
        from .tasks import make_task_decorator_for_manager

        task_decorator_for_manager = make_task_decorator_for_manager(self)
        func_decorator = task_decorator_for_manager(
            schedule,
            name=name,
            tags=tags,
            execute_then_wait=execute_then_wait,
            stop_on_error=stop_on_error,
            max_retry=max_retry,
            start_immediately=start_immediately
        )

        if func is not None:
            return func_decorator(func)
        return func_decorator
