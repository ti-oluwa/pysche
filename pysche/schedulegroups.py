from __future__ import annotations
from collections import deque
from typing import Any, Callable, Coroutine, Iterator, NoReturn, Union, List
import asyncio

from .abc import AbstractBaseSchedule
from .baseschedule import ScheduleType
from .descriptors import SetOnceDescriptor
from . import _utils
from .exceptions import InsufficientArguments


class ScheduleGroup(AbstractBaseSchedule):
    """
    A group of schedules that will be due if any of the schedules in the group is due.

    It allows a single task to be scheduled to run at multiple times(on different schedules)
    without having to create multiple tasks doing the same thing but on different schedules.
    """

    schedules = SetOnceDescriptor(
        tuple, validators=[_utils.validate_schedules_iterable]
    )
    """A tuple containing the schedules in the group."""

    def __init__(self, *schedules: ScheduleType) -> None:
        """
        Create a schedule group

        :param schedules: The schedules to group. Duplicate schedules will be removed.
        """
        schedules = tuple(set(schedules))
        if len(schedules) < 2:
            raise InsufficientArguments(
                "At least two dissimilar schedules are required to create a schedule group."
            )
        self.schedules = schedules
        return None

    def is_due(self) -> bool:
        """If any of the schedules in the group is due, the group is due."""
        return any(schedule.is_due() for schedule in self.schedules)

    def make_schedule_func_for_task(
        self, scheduledtask
    ) -> Callable[..., Coroutine[Any, Any, NoReturn]]:
        from .tasks import ScheduledTask

        task: ScheduledTask = scheduledtask
        task._exc = None
        # Use a deque to manage record of created futures for thread-safety
        created_futures: deque[asyncio.Future] = deque()

        # Use a generator to ensure creation of schedule functions is lazy
        def schedule_funcs():
            """Yield schedule functions for each schedule in the group."""
            for schedule in self.schedules:
                yield schedule.make_schedule_func_for_task(task)

        def create_future_for_schedule_func(
            task: ScheduledTask,
            schedule_func: Callable[..., Coroutine],
            *args,
            **kwargs,
        ) -> asyncio.Future:
            """
            Make futures that can run concurrently in the background
            for each schedule function, and pass argument and keyword arguments
            passed to this function to each schedule function
            """
            # The task manager flag that allows task to run is set to False.
            # Do not bother creating a future
            if not task.manager._continue:
                return
            future = task.manager._run_in_workthread(
                schedule_func, False, *args, **kwargs
            )

            def _done_callback(future: asyncio.Future) -> None:
                """
                Handles exception propagation, adding the result to the task and rescheduling the
                a new schedule function if the current one completes successfully.
                """
                if not task.manager._continue:
                    return

                try:
                    result = future.result()
                except BaseException as exc:
                    task._exc = exc
                else:
                    # If the future completed successfully,
                    # add the result to the task, and
                    task._add_result(result)
                    # reschedule the schedule function
                    create_future_for_schedule_func(
                        task, schedule_func, *args, **kwargs
                    )
                return

            # Add a callback to the future, this callback handles exception
            # propagation, adding the result to the task and rescheduling the
            # a new schedule function if the current one completes successfully
            future.add_done_callback(_done_callback)
            created_futures.append(future)
            return future

        async def schedulegroup_func(*args, **kwargs):
            for schedule_func in schedule_funcs():
                create_future_for_schedule_func(task, schedule_func, *args, **kwargs)

            try:
                # Wait indefinitely, until an exception occurs or the task is stopped
                while task._exc is None:
                    await asyncio.sleep(0.0001)
                    continue
                else:
                    # If an exception occurred, raise it
                    # this allows any exceptions that occurred in the schedule functions
                    # to be propagated properly to the task where it will be handled
                    raise task._exc
            except BaseException as exc:
                # If any exception occurs, cancel all the created futures
                # and await them properly to avoid warning thrown by asyncio
                for future in created_futures.copy():
                    future.cancel()
                # Raise the Exception after doing necessary cleanup
                raise exc

        schedulegroup_func.__qualname__ = (
            f"schedulegroup_func_for_{_utils.underscore_string(task.name)}"
        )
        return schedulegroup_func

    def as_string(self) -> str:
        """Returns a string representation of the schedule group."""
        return f"{type(self).__name__}<({', '.join(map(lambda s: str(s), self.schedules))})>"

    def __iter__(self) -> Iterator[ScheduleType]:
        return iter(self.schedules)

    def __contains__(self, schedule: ScheduleType) -> bool:
        return schedule in self.schedules

    def __eq__(self, other: Union[ScheduleGroup, object]) -> bool:
        if not isinstance(other, ScheduleGroup):
            raise NotImplementedError(
                f"Cannot compare {type(self).__name__} and {type(other).__name__}"
            )
        return self.schedules == other.schedules

    def __hash__(self) -> int:
        return hash(self.schedules)

    def __add__(self, other: Union[ScheduleType, ScheduleGroup]) -> ScheduleGroup:
        """Adds a new schedule to the group."""
        try:
            if isinstance(other, ScheduleGroup):
                return type(self)(*self.schedules, *other.schedules)
            return type(self)(*self.schedules, other)
        except ValueError as exc:
            raise ValueError(f"Cannot add {self} and {other}") from exc

    __radd__ = __add__
    __iadd__ = __add__

    def __sub__(self, other: ScheduleType) -> ScheduleGroup:
        """Removes a schedule from the group."""
        try:
            return type(self)(
                *filter(lambda schedule: schedule != other, self.schedules)
            )
        except InsufficientArguments:
            raise ValueError("Subtraction resulted in an invalid schedule group.")

    __isub__ = __sub__

    def __describe__(self) -> str:
        schedules: List[ScheduleType] = list(self.schedules)
        first_schedule: ScheduleType = schedules.pop(0)
        last_schedule: ScheduleType = schedules.pop(-1)

        pre_desc = _utils._strip_text(first_schedule.__describe__())
        if schedules:
            mid_desc = ", ".join(
                _utils._strip_text(schedule.__describe__().lower(), "task will run")
                for schedule in schedules
            )
        else:
            mid_desc = ""
        post_desc = f"{_utils._strip_text(last_schedule.__describe__().lower(), "task will run")}"

        if not mid_desc:
            return f"{pre_desc} and {post_desc}."
        return f"{pre_desc}, {mid_desc}, and {post_desc}."


def group_schedules(*schedules: ScheduleType) -> ScheduleGroup:
    """
    Helper function for creating a schedule group.

    :param schedules: The schedules to group
    :return: A schedule group containing the provided schedules

    A schedule group is a group of schedules that will be due if any of the schedules in the group is due.

    It allows a single task to be scheduled to run at multiple times without having to create multiple
    tasks doing the same thing but on different schedules.
    The task will run on each schedule independently as the schedules with block each other.
    This means that if a number of schedules are due at the same time the task will run multiple times

    For example:
    ```python
    import pysche

    s = pysche.schedules
    manager = pysche.TaskManager()

    run_on_mondays_at_12pm = s.run_on_weekday(0).at("12:00:00")
    run_on_thursdays_at_3am = s.run_on_weekday(3).at("03:00:00")
    run_on_mondays_at_12pm_and_on_thursdays_at_3am = pysche.group_schedules(run_on_mondays_at_12pm, run_on_thursdays_at_3am)

    @manager.taskify(run_on_mondays_at_12pm_and_on_thursdays_at_3am)
    def do_something():
        ...
    ```
    """
    return ScheduleGroup(*schedules)
