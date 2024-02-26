from collections.abc import Coroutine
import datetime
import sys
from collections import deque
import threading
from typing import Any, Callable, Dict, Iterable, List, Mapping
import asyncio
import functools
from concurrent.futures import CancelledError, ThreadPoolExecutor
from bs4_web_scraper.logger import Logger


class UnregisteredTask(Exception):
    """Raised when a task is not being managed by a `TaskManager`"""


class _RedirectStandardOutputStream:
    """
    Context manager that redirects all standard output streams within the block 
    to a stream that will 'always' write to console.

    Can be used to ensure that all output streams are written to console, even if
    the output stream is in a different thread.
    """
    def __init__(self):
        self.stream = None
        return None

    def __call__(self, __o: Any) -> Any:
        return self.stream.write(str(__o))    

    def __enter__(self):
        # Store the original sys.stdout
        self.og_stream = sys.stdout
        # Redirect sys.stdout to the sys.stderr
        self.stream = sys.stderr
        sys.stdout = self.stream
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        # Restore the original sys.stdout
        sys.stdout = self.og_stream



class TaskManager:
    """
    Manages scheduled tasks.
    """
    log_to_console = True
    __slots__ = (
        "name", "_tasks", "_futures", "_continue", "_loop", 
        "_workthread", "_executor", "max_duplicates", "_logger"
    )

    def __init__(self, name: str = None, max_duplicates: int = 1, log_to: str = None):
        """
        Creates a new task manager.

        :param name: The name of the task manager. Defaults to the class name and a unique ID.
        :param max_duplicates: The maximum number of task duplicates that can be managed at a time.
        Set to a negative number to allow unlimited instances.
        :param log_to: Path to log file. If provided, task execution details are logged to the file.

        Task execution details are always written to the console. Create a subclass and set `log_to_console` to False
        to disable writing to the console.
        """
        from .task import ScheduledTask
        if not isinstance(max_duplicates, int):
            raise TypeError("Max instances must be an integer")
        
        self.name: str = name or f"{self.__class__.__name__.lower()}{str(id(self))[-6:]}"
        self._tasks: deque[ScheduledTask] = deque([])
        self._futures: deque[asyncio.Task] = deque([])
        self._continue: bool = False
        self._loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self._workthread: threading.Thread | None = None
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor()
        self.max_duplicates = max_duplicates

        if log_to:
            self._logger = Logger(name=f'{self.__class__.__name__.lower()}{id(self)}', log_filepath=log_to)
            self._logger.to_console = self.log_to_console
        else: 
            self._logger = None
        return None
    
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.status} name='{self.name}' tasks={self.tasks}>"
    
    
    def __del__(self) -> None:
        self.shutdown()
        return None


    @property
    def has_started(self) -> bool:
        """Returns True if the task manager has started executing tasks"""
        return self._continue and self._loop.is_running()
    
    @property
    def tasks(self):
        """Returns a list of scheduled tasks being managed currently"""
        return [ task for task in self._tasks if task.manager == self ]
    
    @property
    def errors(self) -> Dict[str, List[Exception]]:
        """
        Returns a dictionary of errors encountered while executing tasks.

        Errors are mapped to the name of the task in which they occurred.
        """
        return { task.name : task.errors for task in self.tasks if task.errors }
    
    @property
    def has_errors(self) -> bool:
        """Returns True if there was any error encountered while executing tasks"""
        return bool(self.errors)
    
    @property
    def is_busy(self) -> bool:
        """Returns True if the task manager is currently executing any task"""
        return any([ task.is_running for task in self.tasks ])
    
    @property
    def status(self):
        return "busy" if self.is_busy else "idle"

    
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
                    return await self._loop.run_in_executor(self._executor, lambda: func(*args, **kwargs))
            except CancelledError as exc:
                raise asyncio.CancelledError(exc)
        return async_func
    

    def _make_future(self, scheduledtask) -> asyncio.Task:
        future = self._loop.create_task(scheduledtask(), name=scheduledtask.name)
        future.id = scheduledtask.id
        self._futures.append(future)
        return future
    

    def _get_futures(self, name: str) -> List[asyncio.Task]:
        """Returns a list of futures with the specified name"""
        matches = []
        for future in self._futures:
            if future.get_name() == name:
                matches.append(future)
        return matches

    
    def _get_future(self, future_id: str) -> asyncio.Task | None:
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
            while not self.is_busy:
                continue
        else:
            # If not, just wait for manager to start successfully
            while not self.has_started:
                continue
        return None
        

    def join(self) -> None:
        """
        Wait for this manager to finish executing all scheduled tasks.
        This method blocks the current thread until all tasks are no longer running.
        """
        if not self.has_started:
            raise RuntimeError(f"{self.name} has not started task execution yet. Cannot join.\n")
        
        try:
            while self.is_busy:
                continue
        except (KeyboardInterrupt, SystemExit):
            self.stop()
        return None
      
    
    def stop(self) -> None:
        """
        Stop executing all scheduled tasks.
        """
        if not self.has_started:
            raise RuntimeError(f"{self.name} has not started task execution yet.\n")

        self._continue = False # breaks outermost while loop in all ScheduleTasks
        for fut in self._futures:
            self._loop.call_soon_threadsafe(fut.cancel)
        self._loop.call_soon_threadsafe(self._loop.stop)
        try:
            self._workthread.join()
        except:
            pass
        self._workthread = None
        return None
    

    def restart(self) -> None:
        """Quick way to restart all tasks"""
        if self.has_started:
            self.stop()
        self.start()
        return None


    def shutdown(self) -> None:
        """
        Cancel all tasks and shutdown.
        This method should be called when the task manager is no longer needed.
        """
        if self.is_busy:
            self.stop() # stop all task execution, if the manager is busy with any
        for task in self.tasks:
            task.cancel() # Cancel all scheduled tasks
        self._loop.close()
        self._executor.shutdown(wait=True, cancel_futures=True)
        return None


    def is_managing(self, name_or_id: str) -> bool:
        """Check if the task manager is managing a task with the specified name or ID"""
        for task in self.tasks:
            if task.name == name_or_id or task.id == name_or_id:
                return True
        return False
    

    def get_tasks(self, name: str):
        """Returns a list of tasks with the specified name"""
        matches = []
        for task in self.tasks:
            if task.name == name:
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
        func: Callable, *,
        args: Iterable[Any] = (), 
        kwargs: Mapping[str, Any] = {}, 
        task_name: str = None
    ):
        """
        Run function once after a specified number of seconds.

        :param delay: The number of seconds to wait before running the function
        :param func: The function to run as a task
        :param args: The arguments to pass to the function
        :param kwargs: The keyword arguments to pass to the function
        :param task_name: The name to give the task created to run the function. 
        This can make it easier to identify the task in logs in the case of errors.
        """
        from .schedules.bases import RunAfterEvery
        from .task import ScheduledTask
        # Wraps function such that the function runs once and then the task is cancelled
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            finally:
                _wrapper.task.cancel()
        
        task = ScheduledTask(
            func=_wrapper,
            schedule=RunAfterEvery(seconds=delay),
            manager=self,
            args=args,
            kwargs=kwargs,
            name=task_name or f"run_{func.__name__}_after_{delay}s",
            stop_on_error=True,
            max_retry=0,
            start_immediately=False,
        )
        _wrapper.task = task
        task.start()
        return task
    

    def run_at(
        self, 
        time: str, 
        func: Callable,
        tz: str | datetime.tzinfo = None,
            *,
        args: Iterable[Any] = (), 
        kwargs: Mapping[str, Any] = {}, 
        task_name: str = None
    ):
        """
        Run function once at a specified time.

        :param time: The time to run the function. Must be in the format 'HH:MM:SS'
        :param func: The function to run as a task
        :param tz: The timezone to use when running the function. Defaults to local timezone.
        :param args: The arguments to pass to the function
        :param kwargs: The keyword arguments to pass to the function
        :param task_name: The name to give the task created to run the function. 
        This can make it easier to identify the task in logs in the case of errors.
        """
        from .schedules.bases import RunAt
        from .task import ScheduledTask
        # Wraps function such that the function runs once and then the task is cancelled
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            finally:
                _wrapper.task.cancel()
        
        try:
            hr, min, sec = time.split(":")
            hr, min, sec = int(hr), int(min), int(sec)
        except ValueError:
            raise ValueError("Invalid time format. Time must be in the format 'HH:MM:SS'")
        task = ScheduledTask(
            func=_wrapper,
            schedule=RunAt(time=time, tz=tz),
            manager=self,
            args=args,
            kwargs=kwargs,
            name=task_name or f"run_{func.__name__}_at_{time}",
            stop_on_error=True,
            max_retry=0,
            start_immediately=False,
        )
        _wrapper.task = task
        task.start()
        return task


    def stop_after(self, delay: int | float) -> None:
        """Stop the execution of all scheduled tasks after a specified number of seconds"""
        if not self.has_started:
            raise RuntimeError(f"{self.name} has not started task execution yet.\n")
        self.run_after(delay, self.stop, task_name=f"stop_{self.name}_after_{delay}seconds")
        return None
    

    def stop_at(self, time: str) -> None:
        """Stop the execution of all scheduled tasks at a specified time. Time must be in the format 'HH:MM:SS'"""
        if not self.has_started:
            raise RuntimeError(f"{self.name} has not started task execution yet. Y\n")
        self.run_at(time, self.stop, task_name=f"stop_{self.name}_at_{time}")
        return None
    
    
    def cancel_task(self, name_or_id: str) -> None:
        """Cancel all tasks with the specified name or ID"""
        if not self.is_managing(name_or_id):
            raise UnregisteredTask(f"Task '{name_or_id}' is not registered with {self.name}")
        
        for task in self.tasks:
            if task.name != name_or_id and task.id != name_or_id:
                continue
            task.cancel()
        return None
    

    def get_running_tasks(self):
        """Returns a list of tasks that are currently running and not paused"""
        running_tasks = []
        for task in self.tasks:
            if task.is_running:
                running_tasks.append(task)
        return running_tasks
    

    def get_paused_tasks(self):
        """Returns a list of tasks that are currently paused"""
        paused_tasks = []
        for task in self.tasks:
            if task.is_paused:
                paused_tasks.append(task)
        return paused_tasks
    

    def get_failed_tasks(self):
        """Returns a list of tasks that failed"""
        failed_tasks = []
        for task in self.tasks:
            if task.failed:
                failed_tasks.append(task)
        return failed_tasks
    

    def log(self, msg: str, level: str = "INFO") -> None:
        """
        Log message to console and to log file (if `log_to` is set)
        :param msg: message to log.
        :param level: Log level. Defaults to INFO. Level only applies if 
        `log_to` is provided on class instantiation.
        """
        if self._logger:
            self._logger.log(msg, level)
        else:
            if self.log_to_console:
                sys.stdout.write(msg)
        return None


    def clear_log_history(self) -> None:
        """Clear all logs in log file"""
        if self._logger:
            self._logger.clear_logs()
        return None
    

    def start_new_log(self, log_filepath: str) -> None:
        """Start a new log history in a new file"""
        if self._logger:
            old_logger = self._logger
            self._logger = Logger(name=self._logger._logger.name, log_filepath=log_filepath)
            self._logger.to_console = old_logger.to_console
        return None
    
