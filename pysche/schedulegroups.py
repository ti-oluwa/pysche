from __future__ import annotations
import datetime
from typing import Iterable, Union

from .abc import AbstractBaseSchedule
from .baseschedule import ScheduleType
from .descriptors import SetOnceDescriptor
from ._utils import validate_schedules_iterable



class ScheduleGroup(AbstractBaseSchedule):
    """
    A group of schedules that will be due if any of the schedules in the group is due.

    It allows a single task to be scheduled to run at multiple times without having to create multiple
    tasks doing the same thing but on different schedules.
    """

    schedules = SetOnceDescriptor(tuple, validators=[validate_schedules_iterable])
    """A tuple containing the schedules in the group."""
    
    def __init__(self, *schedules: ScheduleType) -> None:
        """
        Create a schedule group

        :param schedules: The schedules to group. Duplicate schedules will be removed.
        """
        if len(schedules) < 2:
            raise ValueError("At least two schedules are required to create a schedule group.")
        self.schedules = tuple(set(schedules))
        return None
    
    # Overrides the default wait_duration attribute
    @property
    def wait_duration(self) -> datetime.timedelta:
        """
        The time until the next schedule in the group is due.

        This is useful for determining how long to wait before checking 
        if any of the schedules in the group are due.
        """
        wait_durations = sorted([schedule.wait_duration for schedule in self.schedules])
        smallest_wait_duration = wait_durations[0]
        return smallest_wait_duration
    
    def is_due(self) -> bool:
        # If any of the schedules in the group is due, the group is due
        return any(schedule.is_due() for schedule in self.schedules)
    

    def as_string(self) -> str:
        return f"{self.__class__.__name__}({', '.join(str(schedule) for schedule in self.schedules)})"


    def __iter__(self) -> Iterable[ScheduleType]:
        return iter(self.schedules)
    

    def __eq__(self, other: Union[ScheduleGroup, object]) -> bool:
        if not isinstance(other, ScheduleGroup):
            return False
        return self.schedules == other.schedules
    

    def __hash__(self) -> int:
        return hash(self.schedules)
        


def group_schedules(*schedules: ScheduleType) -> ScheduleGroup:
    """
    Helper function for creating a schedule group.

    :param schedules: The schedules to group
    :return: A schedule group containing the provided schedules

    A schedule group is a group of schedules that will be due if any of the schedules in the group is due.

    It allows a single task to be scheduled to run at multiple times without having to create multiple
    tasks doing the same thing but on different schedules.

    For example:
    ```python
    import pysche

    s = pysche.schedules
    manager = pysche.TaskManager()

    run_on_mondays_at_12pm = s.run_on_weekday(0).at("12:00:00")
    run_on_thursdays_at_3am = s.run_on_weekday(3).at("03:00:00")
    run_on_mondays_at_12pm_and_on_thursdays_at_3am = pysche.group_schedules(run_on_mondays_at_12pm, run_on_thursdays_at_3am)


    @manager.newtask(run_on_mondays_at_12pm_and_on_thursdays_at_3am)
    def do_something():
        ...
    ```
    """
    return ScheduleGroup(*schedules)
