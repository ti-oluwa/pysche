from typing import Callable, List, Optional, Union
import functools

from .baseschedule import ScheduleType
from .tasks import ScheduledTask
from .taskmanager import TaskManager
from .schedulegroups import ScheduleGroup
from ._utils import underscore_string


def task(
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
        schedule: Union[ScheduleType, ScheduleGroup],
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
