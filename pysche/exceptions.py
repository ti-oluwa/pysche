
class UnregisteredTask(Exception):
    """Raised when an action is performed on a task by a manager that it was not registered with."""
    pass


class TaskDuplicationError(Exception):
    """Raised when maximum number of duplicate task for a `TaskManger` is exceeded"""
    pass
