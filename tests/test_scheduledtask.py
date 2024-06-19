import time
import os
import asyncio
import datetime
import unittest

from pysche.schedules import run_afterevery
from pysche.tasks import ScheduledTask, TaskResult
from pysche.taskmanager import TaskManager
from pysche.exceptions import TaskExecutionError
from tests.mock_functions import (
    count_to_ten,
    raise_exception,
    print_helloworld,
    error,
    error_callback,
)


class TestScheduledTask(unittest.TestCase):
    def setUp(self) -> None:
        self.log_path = "./tests/fixtures/logs.log"
        self.manager = TaskManager(
            name="testmanager", log_to_console=False, log_file=self.log_path
        )
        self.manager.start()
        return None

    def tearDown(self) -> None:
        self.manager.stop()
        return None

    def test_init(self):
        task1 = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=5),
            manager=self.manager,
            name="count_to_ten_every_5_seconds",
            start_immediately=False,
        )
        task2 = ScheduledTask(
            print_helloworld,
            schedule=run_afterevery(seconds=5),
            manager=self.manager,
            start_immediately=True,
        )

        self.assertIsInstance(task1, ScheduledTask)
        self.assertEqual(task1.name, "count_to_ten_every_5_seconds")
        self.assertEqual(task1.schedule, run_afterevery(seconds=5))
        self.assertEqual(task1.manager, self.manager)
        self.assertFalse(task1.is_active)

        self.assertEqual(task2.name, print_helloworld.__name__)
        self.assertTrue(task2.is_active)

        self.assertFalse(task1 == task2)
        self.assertTrue(task1 == task1)

    def test_add_tag(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=5),
            manager=self.manager,
            tags=["tag1", "tag2"],
        )

        task.add_tag("tag3")
        self.assertEqual(task.tags, ["tag1", "tag2", "tag3"])

    def test_remove_tag(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=5),
            manager=self.manager,
            tags=["tag1", "tag2"],
        )

        task.remove_tag("tag3")
        self.assertEqual(task.tags, ["tag1", "tag2"])

        task.remove_tag("tag2")
        self.assertEqual(task.tags, ["tag1"])

    def test_log(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=5),
            manager=self.manager,
            tags=["tag1", "tag2"],
        )

        try:
            task.log("This is a log message")
        except Exception as exc:
            self.fail(f"Unexpected exception: {exc}")

    def test_call(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
        )

        task_coroutine = task()
        self.assertTrue(asyncio.iscoroutine(task_coroutine))
        try:
            asyncio.run(task_coroutine)
            task.join()
        except Exception as exc:
            self.fail(f"Unexpected exception: {exc}")

    def test_equality(self):
        task1 = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
        )
        task2 = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
        )

        self.assertFalse(task1 == task2)
        self.assertTrue(task1 == task1)

    def test_start(self):
        task = ScheduledTask(
            print_helloworld,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=False,
        )
        self.assertFalse(task.is_active)

        with self.assertRaises(TaskExecutionError):
            task._is_active = True
            try:
                task.start()
            finally:
                task._is_active = False

        with self.assertRaises(TaskExecutionError):
            task._failed = True
            try:
                task.start()
            finally:
                task._failed = False

        task.start()
        self.assertTrue(task.is_active)

    def test_join(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        self.assertTrue(task.is_active)
        task.join()
        self.assertFalse(task.is_active)

    def test_pause(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )
        self.assertTrue(task.is_active)

        with self.assertRaises(TaskExecutionError):
            task._is_active = False
            try:
                task.pause()
            finally:
                task._is_active = True

        with self.assertRaises(TaskExecutionError):
            task._failed = True
            try:
                task.pause()
            finally:
                task._failed = False

        self.assertFalse(task.is_paused)

        task.pause()
        self.assertTrue(task.is_active)
        self.assertTrue(task.is_paused)

    def test_resume(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        self.assertTrue(task.is_active)
        self.assertFalse(task.is_paused)

        task.pause()
        self.assertTrue(task.is_active)
        self.assertTrue(task.is_paused)

        task.resume()
        self.assertTrue(task.is_active)
        self.assertFalse(task.is_paused)

    def test_stop(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
        )

        self.assertTrue(task.is_active)
        self.assertFalse(task.cancelled)

        task.stop()
        self.assertFalse(task.is_active)
        self.assertTrue(task.cancelled)

    def test_pause_after(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        self.assertFalse(task.is_paused)
        pause_task = task.pause_after(2)
        pause_task.join()
        self.assertTrue(task.is_paused)

    def test_pause_until(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        two_seconds_from_now = (
            datetime.datetime.now() + datetime.timedelta(seconds=2)
        ).strftime("%H:%M:%S")
        pause_task = task.pause_until(two_seconds_from_now)

        self.assertTrue(task.is_paused)
        pause_task.join()
        self.assertFalse(task.is_paused)

    def test_pause_for(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        self.assertFalse(task.is_paused)
        pause_task = task.pause_for(2)
        self.assertTrue(task.is_paused)
        pause_task.join()
        self.assertFalse(task.is_paused)

    def test_pause_at(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        two_seconds_from_now = (
            datetime.datetime.now() + datetime.timedelta(seconds=2)
        ).strftime("%H:%M:%S")
        pause_task = task.pause_at(two_seconds_from_now)

        self.assertFalse(task.is_paused)
        pause_task.join()
        self.assertTrue(task.is_paused)

    def test_pause_on(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        two_seconds_from_now_dt = (
            datetime.datetime.now() + datetime.timedelta(seconds=2)
        ).strftime("%Y-%m-%d %H:%M:%S")
        pause_task = task.pause_on(two_seconds_from_now_dt)

        self.assertFalse(task.is_paused)
        pause_task.join()
        self.assertTrue(task.is_paused)

    def test_stop_after(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        stop_task = task.stop_after(2)
        self.assertFalse(task.cancelled)
        stop_task.join()
        self.assertTrue(task.cancelled)

    def test_stop_at(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        two_seconds_from_now = (
            datetime.datetime.now() + datetime.timedelta(seconds=2)
        ).strftime("%H:%M:%S")
        stop_task = task.stop_at(two_seconds_from_now)

        self.assertFalse(task.cancelled)
        stop_task.join()
        self.assertTrue(task.cancelled)

    def test_stop_on(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        two_seconds_from_now_dt = (
            datetime.datetime.now() + datetime.timedelta(seconds=2)
        ).strftime("%Y-%m-%d %H:%M:%S")
        stop_task = task.stop_on(two_seconds_from_now_dt)

        self.assertFalse(task.cancelled)
        stop_task.join()
        self.assertTrue(task.cancelled)

    def test_add_result(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
            save_results=True,
        )

        # Results are added automatically in the background
        # Using the `_add_result` method
        time.sleep(2)
        self.assertTrue(len(task.resultset) == 2)
        for result in task.resultset:
            self.assertEqual(result.value, None)

    def test_get_results(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
            save_results=True,
        )

        time.sleep(2)
        results = task.get_results()
        self.assertIsInstance(results, list)
        self.assertTrue(len(results) == 2)
        for result in results:
            self.assertIsInstance(result, TaskResult)

    def test_add_error_callback(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True,
        )

        task.add_error_callback(error_callback)
        time.sleep(2)
        self.assertTrue(error)


if __name__ == "__main__":
    unittest.main()
