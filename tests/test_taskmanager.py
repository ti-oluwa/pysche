import unittest
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
import os

from pysche.manager import TaskManager
from pysche.schedules.bases import RunAfterEvery
from pysche.task import ScheduledTask
from tests.mock import *


class TestTaskManager(unittest.TestCase):

    def setUp(self) -> None:
        self.log_path = "./tests/fixtures/logs.log"
        return super().setUp()
    
    def tearDown(self) -> None:
        return super().tearDown()


    def test_init(self):
        manager = TaskManager(name="TestTaskManager", log_to=self.log_path, max_duplicates=1)
        self.assertEqual(manager.name, "TestTaskManager")
        self.assertTrue(os.path.exists(self.log_path))
        self.assertEqual(manager.max_duplicates, 1)
        self.assertEqual(manager.tasks, [])
        self.assertEqual(manager._futures, [])
        self.assertIsNone(manager.thread)
        self.assertIsInstance(manager._loop, asyncio.AbstractEventLoop)
        self.assertIsInstance(manager._executor, ThreadPoolExecutor)
        self.assertFalse(manager._continue)
        manager = TaskManager()
        self.assertIsNone(manager._logger)
        self.assertEqual(manager.name, f"{manager.__class__.__name__.lower()}_{str(id(manager))[-6:]}")
        del manager


    def test_start(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=RunAfterEvery(seconds=10),
            manager=manager
        )
        self.assertFalse(manager._continue)
        manager.start()
        self.assertRaises(RuntimeWarning, manager.start)
        self.assertTrue(manager._continue)
        self.assertTrue(manager._loop.is_running())
        self.assertIsNotNone(manager._loop)
        self.assertIsNotNone(manager._executor)
        self.assertIsNotNone(manager.thread)
        self.assertIsInstance(manager.thread, threading.Thread)
        self.assertTrue(manager.thread.is_alive())
        self.assertTrue(manager.is_busy)
        self.assertTrue(task.is_running)
        del manager


    def test_stop(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=RunAfterEvery(seconds=10),
            manager=manager
        )
        self.assertFalse(manager._continue)
        manager.start()
        self.assertTrue(manager._continue)
        self.assertTrue(manager._loop.is_running())
        self.assertTrue(manager.thread.is_alive())
        self.assertTrue(manager.is_busy)
        self.assertTrue(task.is_running)
        manager.stop()
        self.assertRaises(RuntimeWarning, manager.stop)
        self.assertFalse(manager._continue)
        self.assertFalse(manager._loop.is_running())
        self.assertIsNone(manager.thread)
        self.assertFalse(manager.is_busy)
        self.assertFalse(task.is_running)
        del manager


    def test_get_futures(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=RunAfterEvery(seconds=10),
            manager=manager
        )
        self.assertIsInstance(manager._get_futures(task.name), list)
        self.assertTrue(manager._get_futures(task.name) != [])
        self.assertTrue(manager._get_futures("fdghjk") == [])
        self.assertTrue(manager._get_futures(task.name)[0].id == task.id)
        del manager


    def test_get_future(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=RunAfterEvery(seconds=10),
            manager=manager
        )
        self.assertIsInstance(manager._get_future(task.id), asyncio.Task)
        self.assertIsNone(manager._get_future("non-existent-id"))
        self.assertTrue(manager._get_future(task.id).id == task.id)
        del manager


    def test_get_task(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=RunAfterEvery(seconds=10),
            manager=manager
        )
        self.assertIsInstance(manager.get_task(task.id), ScheduledTask)
        self.assertIsNone(manager.get_task("non-existent-id"))
        self.assertTrue(manager.get_task(task.id).id == task.id)
        del manager


    def test_get_tasks(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=RunAfterEvery(seconds=10),
            manager=manager
        )
        self.assertIsInstance(manager.get_tasks(task.name), list)
        self.assertTrue(manager.get_tasks(task.name) != [])
        self.assertTrue(manager.get_tasks("fdghjk") == [])
        self.assertTrue(manager.get_tasks(task.name)[0].id == task.id)
        del manager


    def test_make_asyncable(self):
        manager = TaskManager()
        self.assertTrue(asyncio.iscoroutinefunction(manager._make_asyncable(print_helloworld)))
        self.assertRaises(TypeError, manager._make_asyncable, fn=1)
        self.assertRaises(TypeError, manager._make_asyncable, fn="Hello World")
        self.assertEqual(manager._make_asyncable(print_helloworld).__name__, print_helloworld.__name__)
        del manager


    def test_log(self):
        manager = TaskManager(log_to=self.log_path)
        manager.log("Hello World")
        with open(self.log_path, "r") as f:
            self.assertTrue("Hello World" in f.read())

        manager = TaskManager()
        try:
            manager.log("Hello World")
        except Exception as e:
            self.fail(f"Exception raised: {e}")
        del manager


    def test_clear_log_history(self):
        manager = TaskManager(log_to=self.log_path)
        manager.log("Hello World")
        with open(self.log_path, "r") as f:
            self.assertTrue("Hello World" in f.read())

        manager.clear_log_history()
        with open(self.log_path, "r") as f:
            self.assertFalse("Hello World" in f.read())
        del manager


    def test_start_new_log(self):
        manager = TaskManager(log_to=self.log_path)
        self.assertEqual(manager._logger.filename, os.path.abspath(self.log_path))
        manager.start_new_log("./tests/fixtures/logs2.log")
        self.assertTrue(os.path.exists("./tests/fixtures/logs2.log"))
        self.assertEqual(manager._logger.filename, os.path.abspath("./tests/fixtures/logs2.log"))
        del manager


    def test_is_managing(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=RunAfterEvery(seconds=10),
            manager=manager
        )
        self.assertTrue(manager.is_managing(task.id))
        self.assertFalse(manager.is_managing("non-existent-id"))
        del manager


    def test_run_after(self):
        manager = TaskManager()
        manager.start()
        manager.run_after(3, print_current_time)
        task = manager.get_tasks("print_current_time")[0]
        self.assertIsNotNone(task)
        self.assertFalse(task.is_running)
        time.sleep(4)
        print(manager.get_tasks("print_current_time"))
        self.assertTrue(manager.get_tasks("print_current_time") == [])
        del manager
