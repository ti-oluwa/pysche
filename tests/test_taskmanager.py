from collections import deque
import unittest
import asyncio
import threading
import os
import datetime

from pysche.taskmanager import TaskManager
from pysche.schedules import run_afterevery
from pysche.tasks import ScheduledTask
from pysche.exceptions import UnregisteredTask
from tests.mock_functions import (
    print_current_time,
    print_helloworld,
    raise_exception,
)


class TestTaskManager(unittest.TestCase):
    def setUp(self) -> None:
        self.log_path = "./tests/fixtures/logs.log"
        return None

    def tearDown(self) -> None:
        return None

    def test_init(self):
        manager = TaskManager(
            name="TestTaskManager",
            log_file=self.log_path,
            max_duplicates=1,
            log_to_console=False,
        )

        self.assertEqual(manager.name, "TestTaskManager")
        self.assertTrue(os.path.exists(self.log_path))
        self.assertEqual(manager.max_duplicates, 1)
        self.assertIsInstance(manager._tasks, deque)
        self.assertIsInstance(manager._futures, deque)
        self.assertIsNone(manager._workthread)
        self.assertIsInstance(manager._loop, asyncio.AbstractEventLoop)
        self.assertFalse(manager._continue)

        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        self.assertEqual(
            manager.name, f"{type(manager).__name__.lower()}{str(id(manager))[-6:]}"
        )

    def test_start(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        task = ScheduledTask(
            func=print_helloworld, schedule=run_afterevery(seconds=10), manager=manager
        )

        self.assertFalse(manager._continue)
        manager.start()
        self.assertRaises(RuntimeError, manager.start)
        self.assertTrue(manager._continue)
        self.assertTrue(manager._loop.is_running())
        self.assertIsNotNone(manager._loop)
        self.assertIsNotNone(manager._workthread)
        self.assertIsInstance(manager._workthread, threading.Thread)
        self.assertTrue(manager._workthread.is_alive())
        self.assertTrue(manager.is_occupied)
        self.assertTrue(task.is_active)

    def test_equality(self):
        manager1 = TaskManager(log_file=self.log_path, log_to_console=False)
        manager2 = TaskManager(log_file=self.log_path, log_to_console=False)

        self.assertFalse(manager1 == manager2)
        self.assertTrue(manager1 == manager1)
        self.assertTrue(manager2 == manager2)

    def test_stop(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        task = ScheduledTask(
            func=print_helloworld, schedule=run_afterevery(seconds=10), manager=manager
        )

        self.assertFalse(manager._continue)
        manager.start()
        self.assertTrue(manager._continue)
        self.assertTrue(manager._loop.is_running())
        self.assertTrue(manager._workthread.is_alive())
        self.assertTrue(manager.is_occupied)
        self.assertTrue(task.is_active)

        manager.stop()
        self.assertRaises(RuntimeError, manager.stop)
        self.assertFalse(manager._continue)
        self.assertFalse(manager._loop.is_running())
        self.assertIsNone(manager._workthread)
        self.assertFalse(manager.is_occupied)
        self.assertFalse(task.is_active)

    def test_get_futures(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        task = ScheduledTask(
            func=print_helloworld, schedule=run_afterevery(seconds=10), manager=manager
        )

        self.assertIsInstance(manager._get_futures(task.name), list)
        self.assertNotEqual(manager._get_futures(task.name), [])
        self.assertEqual(manager._get_futures("asdf"), [])
        self.assertEqual(manager._get_futures(task.name)[0].id, task.id)

    def test_get_future(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        task = ScheduledTask(
            func=print_helloworld, schedule=run_afterevery(seconds=10), manager=manager
        )

        self.assertIsInstance(manager._get_future(task.id), asyncio.Future)
        self.assertIsNone(manager._get_future("non-existent-id"))
        self.assertEqual(manager._get_future(task.id).id, task.id)

    def test_get_task(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        task = ScheduledTask(
            func=print_helloworld, schedule=run_afterevery(seconds=10), manager=manager
        )

        self.assertIsInstance(manager.get_task(task.id), ScheduledTask)
        self.assertIsNone(manager.get_task("non-existent-id"))
        self.assertTrue(manager.get_task(task.id).id == task.id)

    def test_get_tasks(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        task = ScheduledTask(
            func=print_helloworld, schedule=run_afterevery(seconds=10), manager=manager
        )

        task.add_tag("test-tag")
        self.assertIsInstance(manager.get_tasks(task.name), list)
        self.assertNotEqual(manager.get_tasks(task.name), [])
        self.assertEqual(manager.get_tasks("asdf"), [])
        self.assertEqual(manager.get_tasks(task.name)[0].id, task.id)
        self.assertEqual(manager.get_tasks("test-tag")[0], task)

    def test_log(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)

        manager.log("Hello World")
        with open(self.log_path, "r") as f:
            self.assertTrue("Hello World" in f.read())

    def test_is_managing(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        task = ScheduledTask(
            func=print_helloworld, schedule=run_afterevery(seconds=10), manager=manager
        )

        self.assertTrue(manager.is_managing(task.id))
        self.assertFalse(manager.is_managing("non-existent-id"))

    def test_run_after(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        manager.start()
        task = manager.run_after(3, print_current_time)

        self.assertEqual(task.name, "run_print_current_time_after_3s")
        self.assertNotEqual(manager.get_tasks("run_print_current_time_after_3s"), [])
        self.assertEqual(task, manager.get_tasks("run_print_current_time_after_3s")[0])
        self.assertTrue(task.is_active)

        task.join()
        self.assertFalse(task.is_active)
        self.assertEqual(task.status.lower(), "stopped")

    def test_run_on(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        manager.start()

        with self.assertRaises(TypeError):
            manager.run_on(3, print_current_time)

        now_plus_4_seconds_dt = (
            datetime.datetime.now() + datetime.timedelta(seconds=4)
        ).strftime("%Y-%m-%d %H:%M:%S")
        task = manager.run_on(
            now_plus_4_seconds_dt, print_current_time, task_name="run_4s_from_now_dt"
        )

        self.assertEqual(task.name, "run_4s_from_now_dt")
        self.assertNotEqual(manager.get_tasks("run_4s_from_now_dt"), [])
        self.assertEqual(task, manager.get_tasks("run_4s_from_now_dt")[0])
        self.assertTrue(task.is_active)

        task.join()
        self.assertFalse(task.is_active)
        self.assertEqual(task.status.lower(), "stopped")

    def test_run_at(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        manager.start()

        with self.assertRaises(TypeError):
            manager.run_at(3, print_helloworld)

        now_plus_4_seconds_time = (
            datetime.datetime.now() + datetime.timedelta(seconds=4)
        ).strftime("%H:%M:%S")
        task = manager.run_at(
            now_plus_4_seconds_time, print_helloworld, task_name="run_4s_from_now_time"
        )

        self.assertEqual(task.name, "run_4s_from_now_time")
        self.assertNotEqual(manager.get_tasks("run_4s_from_now_time"), [])
        self.assertEqual(task, manager.get_tasks("run_4s_from_now_time")[0])
        self.assertTrue(task.is_active)

        task.join()
        self.assertFalse(task.is_active)
        self.assertEqual(task.status.lower(), "stopped")

    def test_stop_after(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        manager.start()

        task = manager.run_after(2, print_current_time)
        stop_task = manager.stop_after(3)

        self.assertEqual(stop_task.name, f"stop_{manager.name}_after_3seconds")
        self.assertNotEqual(
            manager.get_tasks(f"stop_{manager.name}_after_3seconds"), []
        )
        self.assertTrue(
            stop_task == manager.get_tasks(f"stop_{manager.name}_after_3seconds")[0]
        )
        self.assertTrue(task.is_active)

        stop_task.join()
        self.assertFalse(task.is_active)
        self.assertEqual(task.status.lower(), "stopped")

    def test_stop_at(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        manager.start()

        task = manager.run_after(2, print_current_time)
        now_plus_3_seconds_time = (
            datetime.datetime.now() + datetime.timedelta(seconds=3)
        ).strftime("%H:%M:%S")
        stop_task = manager.stop_at(now_plus_3_seconds_time)

        self.assertNotEqual(manager.get_tasks(stop_task.name), [])
        self.assertTrue(task.is_active)

        stop_task.join()
        self.assertFalse(task.is_active)
        self.assertEqual(task.status.lower(), "stopped")

    def test_stop_on(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        manager.start()

        task = manager.run_after(2, print_current_time)
        now_plus_3_seconds_dt = (
            datetime.datetime.now() + datetime.timedelta(seconds=3)
        ).strftime("%Y-%m-%d %H:%M:%S")
        stop_task = manager.stop_on(now_plus_3_seconds_dt)

        self.assertNotEqual(manager.get_tasks(stop_task.name), [])
        self.assertTrue(task.is_active)

        stop_task.join()
        self.assertFalse(task.is_active)
        self.assertEqual(task.status.lower(), "stopped")

    def test_stop_task(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        manager.start()

        task = manager.run_after(2, print_current_time)
        self.assertTrue(task.is_active)
        with self.assertRaises(UnregisteredTask):
            manager.stop_task("non-existent-id")

        manager.stop_task(task.id)
        self.assertFalse(task.is_active)
        self.assertTrue(task.stopped)
        self.assertEqual(task.status.lower(), "stopped")

    def test_taskify(self):
        manager = TaskManager(log_file=self.log_path, log_to_console=False)
        manager.start()

        func = manager.taskify(run_afterevery(seconds=3))(raise_exception)
        task = func()

        self.assertTrue(task.is_active)
        task.join()
        self.assertFalse(task.is_active)


if __name__ == "__main__":
    unittest.main()
