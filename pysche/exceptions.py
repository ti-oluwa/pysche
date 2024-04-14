
class TaskError(Exception):
    """Base class for all exceptions raised by `Task`."""
    pass


class TaskExecutionError(TaskError):
    """Raised when an error occurs while executing a `Task`."""
    pass


class TaskDuplicationError(TaskError):
    """Raised when maximum number of duplicate task for a `TaskManger` is exceeded"""
    pass


class UnregisteredTask(TaskError):
    """Raised when an action is performed on a task by a manager that it was not registered with."""
    pass


class CancelTask(TaskError):
    """
    Raised to request that a task be cancelled.

    This exception can be raised from within a task to request that the task be cancelled.
    Example:
    ```python
    import pysche
    from pysche.schedules import RunAfterEvery

    manager = pysche.TaskManager()
    run_after_every_5_seconds = RunAfterEvery(seconds=5) 

    @run_after_every_5_seconds(manager=manager)
    def my_func():
        # do something
        ...
        if some_condition:
            # Cancel the task
            raise pysche.CancelTask
        else:
            # do something else
            ...
    ```
    """
    # Why use an exception instead of a custom return value?
    # Using an exception to request cancellation allows the task to be cancelled from any depth of the call stack.
    # This is useful when the task is a coroutine that is called from another coroutine.
    # If a custom return value was used, the return value would have to be propagated through the call stack
    # to the top level coroutine, which would be cumbersome and error-prone.
    pass


class InsufficientArguments(ValueError):
    """Raised when a schedule group is created with insufficient schedules."""
    pass
