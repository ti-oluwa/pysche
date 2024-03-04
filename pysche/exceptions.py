
class TaskError(Exception):
    """Base class for all exceptions raised by `Task`."""
    pass


class TaskExecutionError(TaskError):
    """Raised when an error occurs while executing a `Task`."""
    pass


class TaskDuplicationError(TaskError):
    """Raised when maximum number of duplicate task for a `TaskManger` is exceeded"""
    pass


class UnregisteredTask(Exception):
    """Raised when an action is performed on a task by a manager that it was not registered with."""
    pass

