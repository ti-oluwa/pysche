import unittest
import asyncio
from unittest.mock import Mock, create_autospec
from typing import Any, Callable, Coroutine, NoReturn

from pysche.schedulegroups import ScheduleGroup, group_schedules
from pysche.baseschedule import Schedule
from pysche.exceptions import InsufficientArguments


class MockSchedule(Schedule):
    created_count = 0

    def __init__(self, due=False):
        self._due = due
        self._index = MockSchedule.created_count
        MockSchedule.created_count += 1

    def is_due(self) -> bool:
        return self._due

    def make_schedule_func_for_task(
        self, task
    ) -> Callable[..., Coroutine[Any, Any, NoReturn]]:
        async def mock_schedule_func(*args, **kwargs):
            return

        return mock_schedule_func

    def as_string(self):
        return f"MockSchedule{self._index}"

    def __describe__(self) -> str:
        return "Task will run at 12:00 PM"


class TestScheduleGroup(unittest.TestCase):
    def test_initialization(self):
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule_group = ScheduleGroup(schedule1, schedule2)

        self.assertEqual(len(schedule_group.schedules), 2)
        self.assertIn(schedule1, schedule_group.schedules)
        self.assertIn(schedule2, schedule_group.schedules)

    def test_initialization_with_insufficient_arguments(self):
        schedule1 = MockSchedule()
        with self.assertRaises(InsufficientArguments):
            ScheduleGroup(schedule1)

    def test_is_due(self):
        schedule1 = MockSchedule(due=False)
        schedule2 = MockSchedule(due=True)
        schedule_group = ScheduleGroup(schedule1, schedule2)

        self.assertTrue(schedule_group.is_due())

        schedule1._due = True
        schedule2._due = False
        self.assertTrue(schedule_group.is_due())

        schedule1._due = False
        self.assertFalse(schedule_group.is_due())

    def test_make_schedule_func_for_task(self):
        mock_task = Mock()
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule_group = ScheduleGroup(schedule1, schedule2)
        schedule_func = schedule_group.make_schedule_func_for_task(mock_task)

        self.assertTrue(asyncio.iscoroutinefunction(schedule_func))

    def test_as_string(self):
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule_group = ScheduleGroup(schedule1, schedule2)

        expected_str = (
            f"ScheduleGroup<({schedule1.as_string()}, {schedule2.as_string()})>"
        )
        self.assertEqual(schedule_group.as_string(), expected_str)

    def test_iter(self):
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule_group = ScheduleGroup(schedule1, schedule2)

        schedules = list(iter(schedule_group))
        self.assertEqual(len(schedules), 2)
        self.assertIn(schedule1, schedules)
        self.assertIn(schedule2, schedules)

    def test_contains(self):
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule_group = ScheduleGroup(schedule1, schedule2)

        self.assertIn(schedule1, schedule_group)
        self.assertIn(schedule2, schedule_group)

    def test_equality(self):
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule_group1 = ScheduleGroup(schedule1, schedule2)
        schedule_group2 = ScheduleGroup(schedule1, schedule2)
        schedule_group3 = ScheduleGroup(schedule1, MockSchedule(due=True))

        self.assertEqual(schedule_group1, schedule_group2)
        self.assertNotEqual(schedule_group1, schedule_group3)

    def test_add_schedule(self):
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule3 = MockSchedule()
        schedule_group = ScheduleGroup(schedule1, schedule2)
        new_schedule_group = schedule_group + schedule3

        self.assertIn(schedule3, new_schedule_group.schedules)
        self.assertEqual(len(new_schedule_group.schedules), 3)

    def test_add_schedule_group(self):
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule3 = MockSchedule()
        schedule4 = MockSchedule()
        schedule_group1 = ScheduleGroup(schedule1, schedule2)
        schedule_group2 = ScheduleGroup(schedule3, schedule4)

        new_schedule_group = schedule_group1 + schedule_group2
        self.assertEqual(len(new_schedule_group.schedules), 4)
        self.assertIn(schedule3, new_schedule_group.schedules)

    def test_subtract_schedule(self):
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule3 = MockSchedule()
        schedule_group = ScheduleGroup(schedule1, schedule2, schedule3)
        new_schedule_group = schedule_group - schedule2

        self.assertNotIn(schedule2, new_schedule_group.schedules)
        self.assertEqual(len(new_schedule_group.schedules), 2)

    def test_subtract_schedule_insufficient_arguments(self):
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule_group = ScheduleGroup(schedule1, schedule2)

        with self.assertRaises(ValueError):
            schedule_group - schedule2

    def test_describe(self):
        schedule1 = create_autospec(Schedule, instance=True)
        schedule2 = create_autospec(Schedule, instance=True)
        schedule1.__describe__.return_value = "Task will run at 12:00 PM"
        schedule2.__describe__.return_value = "Task will run at 01:00 PM"
        schedule_group = ScheduleGroup(schedule1, schedule2)

        expected_description = "Task will run at 12:00 PM and at 01:00 PM."
        self.assertEqual(schedule_group.__describe__(), expected_description)

    def test_group_schedules(self):
        schedule1 = MockSchedule()
        schedule2 = MockSchedule()
        schedule_group = group_schedules(schedule1, schedule2)

        self.assertIsInstance(schedule_group, ScheduleGroup)
        self.assertEqual(len(schedule_group.schedules), 2)
        self.assertIn(schedule1, schedule_group.schedules)
        self.assertIn(schedule2, schedule_group.schedules)


if __name__ == "__main__":
    unittest.main()
