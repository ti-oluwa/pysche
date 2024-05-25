from typing import Any, Callable, List, Optional, Union
import functools

from .baseschedule import ScheduleType
from .tasks import ScheduledTask
from .taskmanager import TaskManager
from .schedulegroups import ScheduleGroup
from ._utils import underscore_string


def taskify(
    schedule: Union[ScheduleType, ScheduleGroup],
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
    Function decorator. 
    
    Converts decorated function to a function that will return a scheduled task when called.
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
    decorator = schedule(
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
    return decorator



def taskify_decorator_factory(manager: TaskManager, /) -> Callable[[Callable], Callable[[Callable], Callable[..., ScheduledTask]]]:
    """
    Factory function for creating a `taskify` decorator for a given manager. This is useful when you want to create multiple
    tasks that are all managed by the same manager.

    :param manager: The manager to create the task decorator for.

    Example:
    ```python
    import pysche

    s = psyche.schedules
    manager = pysche.TaskManager(name="my_manager")
    taskify = pysche.taskify_decorator_factory(manager)

    @taskify(s.run_afterevery(seconds=10), ...)
    def function_one():
        pass
        
    
    @taskify(s.run_at("20:00:00"), ...)
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
    decorator_for_manager = functools.partial(taskify, manager=manager)
    decorator = functools.wraps(taskify)(decorator_for_manager)
    
    manager_name = underscore_string(manager.name)
    decorator.__name__ = f"{taskify.__name__}_decorator_for_{manager_name}"
    decorator.__qualname__ = f"{taskify.__qualname__}_decorator_for_{manager_name}"
    return decorator


  

def onerror(callback: Callable) -> Callable[..., Callable[..., Union[ScheduledTask, Any]]]:
    """
    Adds an error callback to the task returned by the "taskified" function.
    The callback will be called when any error is encountered during the task's execution.

    :param callback: The callback function to call when an error is encountered.
    The callback should accept two arguments: the task that encountered the error and the error itself.

    Example:
    ```python
    import pysche

    manager = pysche.TaskManager()
    s = pysche.schedules

    def error_callback(task, error):
        print(f"Task {task} errored with exception: {error}")

    @pysche.onerror(error_callback)
    @manager.taskify(s.run_afterevery(seconds=10))
    def function():
        raise Exception("An error occurred")

    def main():
        with manager.execute_till_idle():
            function()
    
    if __name__ == "__main__":
        main()
    ```
    """
    def decorator(taskified: Callable[..., ScheduledTask]) -> Callable[..., ScheduledTask]:
        @functools.wraps(taskified)
        def wrapper(*args, **kwargs) -> ScheduledTask:
            task = taskified(*args, **kwargs)
            if not isinstance(task, ScheduledTask):
                raise ValueError(
                    "The decorated function must return a ScheduledTask instance."
                    "It seems the function has not been marked as a task with the @task decorator."
                )
            task.add_error_callback(callback)
            return task

        return wrapper
    
    return decorator
