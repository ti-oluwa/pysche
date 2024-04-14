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
from dataclasses import KW_ONLY, dataclass, field, InitVar


from ._utils import (
    _RedirectStandardOutputStream, parse_datetime, 
    get_datetime_now, underscore_datetime
)
from .exceptions import UnregisteredTask
from .logging import get_logger



@dataclass(slots=True)
class TaskManager:
    """
    Manages the execution of scheduled tasks.

    Task managers can also perform one-time execution of functions on a specified schedule.

    Example:
    ```python
    import pysche

    manager = pysche.TaskManager()

    def say_hi(to: str):
        print(f"Hi! {to.title()}")

    def main():
        manager.start()
        # Task 1
        say_hi_to_michael_after_5s = manager.run_after(5, say_hi, args=("Michael",))
        # Task 2
        say_hi_to_dan_at_12pm = manager.run_at("12:00:00", say_hi, kwargs={"to": "Dan"})
        # Wait for tasks to finish executing
        manager.join()
    
    if __name__ == "__main__":
        main()
    ```
    """

    name: Optional[str] = None
    """The name of the task manager. Defaults to the class name and a unique ID."""
    _: KW_ONLY
    max_duplicates: int = -1
    """
    The maximum number of task duplicates that can be managed at a time.
    Set to a negative number to allow unlimited instances.
    """
    log_to: InitVar[Optional[str]] = None
    """
    Path to log file. If provided, task execution details are logged to the file.
    """

    # Used deque to allow for thread-safety and O(1) time complexity when removing tasks from the list
    _tasks: deque = field(init=False, default_factory=deque)
    """A list of scheduled tasks that are currently being managed."""
    _futures: deque[asyncio.Future]  = field(init=False, default_factory=deque)
    """A list of futures that from the scheduled tasks being managed."""
    _continue: bool = field(init=False, default=False)
    """A flag that allows all ScheduleTasks managed by this manager to run."""
    _loop: asyncio.AbstractEventLoop = field(init=False, default_factory=asyncio.new_event_loop)
    """The event loop used to run scheduled tasks."""
    _workthread: Optional[threading.Thread] = field(init=False, default=None)
    """The thread used to run the event loop."""
    _executor: ThreadPoolExecutor = field(init=False)
    """The executor used to run blocking functions as asynchronous tasks."""
    logger: logging.Logger = field(init=False)
    """The logger used to log task execution details."""

    def __post_init__(self, log_to: str) -> None:
        if not self.name:
            self.name = f"{self.__class__.__name__.lower()}{str(id(self))[-6:]}"

        self._executor = ThreadPoolExecutor(thread_name_prefix=f"{self.name}_sync_to_async_executor")
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
            raise NotImplementedError(
                f"Cannot compare {self.__class__.__name__} with {other.__class__.__name__}"
            )
        return hash(self) == hash(other)
    

    def __gt__(self, other: TaskManager) -> bool:
        if not isinstance(other, TaskManager):
            raise NotImplementedError(
                f"Cannot compare {self.__class__.__name__} with {other.__class__.__name__}"
            )
        return len(self.tasks) > len(other.tasks)
    
    
    def __lt__(self, other: TaskManager) -> bool:
        return not self.__gt__(other)
    

    def __getitem__(self, name_or_id: str):
        task = self.get_task(name_or_id)
        if not task:
            raise KeyError(
                f"Task '{name_or_id}' is not registered with {self.name}"
            )
        return task


    @property
    def has_started(self) -> bool:
        """
        Returns True if the task manager has started and is able to execute tasks
        """
        return self._continue is True and self._loop.is_running()
    
    @property
    def tasks(self):
        """Returns a list of scheduled tasks that are currently being managed"""
        from .tasks import ScheduledTask
        tasks: List[ScheduledTask] = list(filter(lambda task: task.manager == self, self._tasks))
        return tasks
    
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
        
        
    def _sync_to_async(self, func: Callable) -> Callable[..., Coroutine[Any, Any, Any]]:
        """
        Converts a blocking function to an non-blocking function that can run concurrently
        with other function converted by this method.

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
                    future = self._loop.run_in_executor(self._executor, lambda: func(*args, **kwargs))
                    return await future
            except CancelledError as exc:
                # Ensure that any exception raised by the function is caught and re-raised in the main thread
                raise asyncio.CancelledError(exc)
            
        return async_func
    

    def _make_future(self, coroutine_func, append: bool = True, *args, **kwargs) -> asyncio.Future:
        """
        Creates and returns an `asyncio.Future` that runs asynchronously in this manager's event loop
        for a coroutine function.

        :param coroutine_func: The coroutine function to create a future for. This could be a scheduled task.
        :param append: If True and the coroutine function is a scheduled task, the future is added to the list of futures handled by this manager
        """
        from .tasks import ScheduledTask
        # Used `run_coroutine_threadsafe` to ensure that tasks(futures) are 
        # run concurrently and in a thread-safe manner once the loop starts running
        future = asyncio.run_coroutine_threadsafe(coroutine_func(*args, **kwargs), self._loop)

        if append is True and isinstance(coroutine_func, ScheduledTask):
            future.name = coroutine_func.name
            future.id = coroutine_func.id
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
        self._workthread = threading.Thread(
            target=self._start_execution, 
            name=f"{self.name}_workthread", 
            daemon=True
        )
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
      
    
    def stop(self, wait: bool = True) -> None:
        """
        Cancel all scheduled tasks and stop the manager from executing any more tasks.

        :param wait: wait for the current iteration of all tasks 
        execution to end properly after cancel operation has been performed.
        """
        if not self.has_started:
            raise RuntimeError(f"{self.name} has not started task execution yet.\n")

        self._continue = False # breaks outermost while loop in all ScheduleTasks

        # Cancel all tasks which eventually cancels all futures before stopping loop
        # This helps avoid the warning message that is thrown by the loop when it is stopped
        for task in self.tasks:
            task.cancel(wait=wait)

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
            self.stop(wait=False) # stop all task execution, if the manager is occupied with any

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
        from .schedules import run_afterevery
        from .tasks import ScheduledTask
        # Wraps function such that the function runs once and then the task is cancelled
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs) -> Any:
            result = func(*args, **kwargs)
            wrapped_func.task.cancel()
            return result
        
        task = ScheduledTask(
            func=wrapped_func,
            schedule=run_afterevery(seconds=delay),
            manager=self,
            args=args,
            kwargs=kwargs,
            name=task_name or f"run_{func.__name__}_after_{delay}s",
            stop_on_error=True,
            max_retries=0,
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
        
        from .schedules import run_afterevery
        from .tasks import ScheduledTask
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs) -> Any:
            result = func(*args, **kwargs)
            wrapped_func.task.cancel()
            return result
        
        task = ScheduledTask(
            func=wrapped_func,
            schedule=run_afterevery(seconds=timedelta.total_seconds()),
            manager=self,
            args=args,
            kwargs=kwargs,
            name=task_name or f"run_{func.__name__}_on_{dt}",
            stop_on_error=True,
            max_retries=0,
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

        :param time: The time to run the function. Must be in the format 'HH:MM' or 'HH:MM:SS'
        :param func: The function to run as a task
        :param tz: The timezone to use. Defaults to local timezone.
        :param args: The arguments to pass to the function
        :param kwargs: The keyword arguments to pass to the function
        :param task_name: The name to give the task created to run the function. 
        This can make it easier to identify the task in logs in the case of errors.
        :return: The created task
        """
        from .schedules import run_at
        from .tasks import ScheduledTask
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs) -> Any:
            result = func(*args, **kwargs)
            wrapped_func.task.cancel()
            return result
        
        task = ScheduledTask(
            func=wrapped_func,
            schedule=run_at(time=time, tz=tz),
            manager=self,
            args=args,
            kwargs=kwargs,
            name=task_name or f"run_{func.__name__}_at_{time}",
            stop_on_error=True,
            max_retries=0,
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

        :param time: The time to stop the execution of all tasks. Must be in the format 'HH:MM' or 'HH:MM:SS'
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
        save_results: bool = False,
        resultset_size: Optional[int] = None,
        stop_on_error: bool = False,
        max_retries: int = 0,
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
        without retry, irrespective of `stop_on_error` or `max_retries`.
        :save_results: If True, the results of each iteration of the task's execution will be saved. 
        :param resultset_size: The maximum number of results to save. If the number of results exceeds this value, the oldest results will be removed.
        If `save_results` is False, this will be ignored. If this is not specified, the default value will be 10.
        :param stop_on_error: If True, the task will stop running when an error is encountered during its execution.
        :param max_retries: The maximum number of times the task will be retried consecutively after an error is encountered.
        :param start_immediately: If True, the task will start immediately after creation. 
        This is only applicable if the manager is already running.
        Otherwise, task execution will start when the manager starts executing tasks.

        #### Usage as a function decorator:
        ```python
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules

        @manager.newtask(s.run_afterevery(seconds=2))
        def speak(msg):
            print(msg)

        task = speak('Hello!')
        ```

        #### Usage as a regular method:
        ```python
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules

        def speak(msg):
            print(msg)

        speak = manager.newtask(s.run_afterevery(seconds=2), speak)
        task = speak('Hey!!')
        ```
        """
        from .tasks import make_task_decorator_for_manager
        task_decorator_for_manager = make_task_decorator_for_manager(self)
        func_decorator = task_decorator_for_manager(
            schedule=schedule,
            name=name,
            tags=tags,
            execute_then_wait=execute_then_wait,
            save_results=save_results,
            resultset_size=resultset_size,
            stop_on_error=stop_on_error,
            max_retries=max_retries,
            start_immediately=start_immediately
        )

        if func is not None:
            return func_decorator(func)
        return func_decorator
