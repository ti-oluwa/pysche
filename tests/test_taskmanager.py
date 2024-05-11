from collections import deque
import unittest
import asyncio
import threading
from concurrent.futures import Future, ThreadPoolExecutor
import os
import datetime

from pysche.pysche.taskmanager import TaskManager
from pysche.schedules import run_afterevery
from pysche.tasks import ScheduledTask
from pysche.exceptions import UnregisteredTask
from tests.mock import print_current_time, print_helloworld, raise_exception



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
        self.assertIsInstance(manager._tasks, deque)
        self.assertIsInstance(manager._futures, deque)
        self.assertIsNone(manager._workthread)
        self.assertIsInstance(manager._loop, asyncio.AbstractEventLoop)
        self.assertIsInstance(manager._executor, ThreadPoolExecutor)
        self.assertFalse(manager._continue)
        manager = TaskManager()
        self.assertEqual(manager.name, f"{manager.__class__.__name__.lower()}{str(id(manager))[-6:]}")
        del manager


    def test_start(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=run_afterevery(seconds=10),
            manager=manager
        )
        self.assertFalse(manager._continue)
        manager.start()
        self.assertRaises(RuntimeError, manager.start)
        self.assertTrue(manager._continue)
        self.assertTrue(manager._loop.is_running())
        self.assertIsNotNone(manager._loop)
        self.assertIsNotNone(manager._executor)
        self.assertIsNotNone(manager._workthread)
        self.assertIsInstance(manager._workthread, threading.Thread)
        self.assertTrue(manager._workthread.is_alive())
        self.assertTrue(manager.is_occupied)
        self.assertTrue(task.is_active)
        del manager


    def test_equality(self):
        manager1 = TaskManager()
        manager2 = TaskManager()
        self.assertFalse(manager1 == manager2)
        self.assertTrue(manager1 == manager1)
        self.assertTrue(manager2 == manager2)
        del manager1, manager2
    

    def test_stop(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=run_afterevery(seconds=10),
            manager=manager
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
        del manager


    def test_get_futures(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=run_afterevery(seconds=10),
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
            schedule=run_afterevery(seconds=10),
            manager=manager
        )
        self.assertIsInstance(manager._get_future(task.id), Future)
        self.assertIsNone(manager._get_future("non-existent-id"))
        self.assertTrue(manager._get_future(task.id).id == task.id)
        del manager


    def test_get_task(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=run_afterevery(seconds=10),
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
            schedule=run_afterevery(seconds=10),
            manager=manager
        )
        task.add_tag("test-tag")
        self.assertIsInstance(manager.get_tasks(task.name), list)
        self.assertTrue(manager.get_tasks(task.name) != [])
        self.assertTrue(manager.get_tasks("fdghjk") == [])
        self.assertTrue(manager.get_tasks(task.name)[0].id == task.id)
        self.assertTrue(manager.get_tasks("test-tag")[0] == task)
        del manager


    def test_make_asyncable(self):
        manager = TaskManager()
        self.assertTrue(asyncio.iscoroutinefunction(manager._make_asyncable(print_helloworld)))
        self.assertRaises(TypeError, manager._make_asyncable, fn=1)
        self.assertRaises(TypeError, manager._make_asyncable, fn="Hello World")

        async_version = manager._make_asyncable(print_helloworld)
        self.assertEqual(async_version.__name__, print_helloworld.__name__)
        del manager


    def test_log(self):
        manager = TaskManager(log_to=self.log_path)
        manager.log("Hello World")
        with open(self.log_path, "r") as f:
            self.assertTrue("Hello World" in f.read())
        del manager


    def test_is_managing(self):
        manager = TaskManager()
        task = ScheduledTask(
            func=print_helloworld,
            schedule=run_afterevery(seconds=10),
            manager=manager
        )
        self.assertTrue(manager.is_managing(task.id))
        self.assertFalse(manager.is_managing("non-existent-id"))
        del manager


    def test_run_after(self):
        manager = TaskManager()
        manager.start()
        task = manager.run_after(3, print_current_time)
        self.assertTrue(task.name == "run_print_current_time_after_3s")
        self.assertTrue(manager.get_tasks("run_print_current_time_after_3s") != [])
        self.assertTrue(task == manager.get_tasks("run_print_current_time_after_3s")[0])
        self.assertTrue(task.is_active)
        task.join()
        self.assertFalse(task.is_active)
        self.assertTrue(task.status.lower() == "cancelled")
        del manager

    
    def test_run_on(self):
        manager = TaskManager()
        manager.start()
        with self.assertRaises(TypeError):
            manager.run_on(3, print_current_time)

        now_plus_4_seconds_dt = (datetime.datetime.now() + datetime.timedelta(seconds=4)).strftime("%Y-%m-%d %H:%M:%S")
        task = manager.run_on(now_plus_4_seconds_dt, print_current_time, task_name="run_4s_from_now_dt")
        self.assertTrue(task.name == "run_4s_from_now_dt")
        self.assertTrue(manager.get_tasks("run_4s_from_now_dt") != [])
        self.assertTrue(task == manager.get_tasks("run_4s_from_now_dt")[0])
        self.assertTrue(task.is_active)
        task.join()
        self.assertFalse(task.is_active)
        self.assertTrue(task.status.lower() == "cancelled")
        del manager

    
    def test_run_at(self):
        manager = TaskManager()
        manager.start()
        with self.assertRaises(TypeError):
            manager.run_at(3, print_helloworld)

        now_plus_4_seconds_time = (datetime.datetime.now() + datetime.timedelta(seconds=4)).strftime("%H:%M:%S")
        task = manager.run_at(now_plus_4_seconds_time, print_helloworld, task_name="run_4s_from_now_time")
        self.assertTrue(task.name == "run_4s_from_now_time")
        self.assertTrue(manager.get_tasks("run_4s_from_now_time") != [])
        self.assertTrue(task == manager.get_tasks("run_4s_from_now_time")[0])
        self.assertTrue(task.is_active)
        task.join()
        self.assertFalse(task.is_active)
        self.assertTrue(task.status.lower() == "cancelled")
        del manager


    def test_stop_after(self):
        manager = TaskManager()
        manager.start()
        task = manager.run_after(2, print_current_time)
        stop_task = manager.stop_after(3)
        self.assertTrue(stop_task.name == f"stop_{manager.name}_after_3seconds")
        self.assertTrue(manager.get_tasks(f"stop_{manager.name}_after_3seconds") != [])
        self.assertTrue(stop_task == manager.get_tasks(f"stop_{manager.name}_after_3seconds")[0])

        self.assertTrue(task.is_active)
        stop_task.join()
        self.assertFalse(task.is_active)
        self.assertTrue(task.status.lower() == "cancelled")
        del manager

    
    def test_stop_at(self):
        manager = TaskManager()
        manager.start()
        task = manager.run_after(2, print_current_time)
        now_plus_3_seconds_time = (datetime.datetime.now() + datetime.timedelta(seconds=3)).strftime("%H:%M:%S")
        stop_task = manager.stop_at(now_plus_3_seconds_time)
        self.assertTrue(manager.get_tasks(stop_task.name) != [])
        
        self.assertTrue(task.is_active)
        stop_task.join()
        self.assertFalse(task.is_active)
        self.assertTrue(task.status.lower() == "cancelled")
        del manager


    def test_stop_on(self):
        manager = TaskManager()
        manager.start()
        task = manager.run_after(2, print_current_time)
        now_plus_3_seconds_dt = (datetime.datetime.now() + datetime.timedelta(seconds=3)).strftime("%Y-%m-%d %H:%M:%S")
        stop_task = manager.stop_on(now_plus_3_seconds_dt)
        self.assertTrue(manager.get_tasks(stop_task.name) != [])
        
        self.assertTrue(task.is_active)
        stop_task.join()
        self.assertFalse(task.is_active)
        self.assertTrue(task.status.lower() == "cancelled")
        del manager


    def test_cancel_task(self):
        manager = TaskManager()
        manager.start()
        task = manager.run_after(2, print_current_time)
        self.assertTrue(task.is_active)
        with self.assertRaises(UnregisteredTask):
            manager.cancel_task("non-existent-id")

        manager.cancel_task(task.id)
        self.assertFalse(task.is_active)
        self.assertTrue(task.cancelled)
        self.assertTrue(task.status.lower() == "cancelled")
        del manager

    
    def test_get_running_tasks(self):
        manager = TaskManager()
        manager.start()
        task1 = manager.run_after(2, print_current_time)
        task2 = manager.run_after(2, print_helloworld)
        self.assertTrue(len(manager.get_running_tasks()) == 2)
        self.assertTrue(task1, task2 in manager.get_running_tasks())
        del manager


    def test_get_paused_tasks(self):
        manager = TaskManager()
        manager.start()
        task1 = manager.run_after(2, print_current_time)
        task2 = manager.run_after(2, print_helloworld)
        task1.pause()
        self.assertTrue(len(manager.get_paused_tasks()) == 1)
        self.assertTrue(task1 in manager.get_paused_tasks())
        self.assertFalse(task2 in manager.get_paused_tasks())
        del manager

    
    def test_get_failed_tasks(self):
        manager = TaskManager()
        manager.start()
        task1 = manager.run_after(2, raise_exception)
        task2 = manager.run_after(2, print_helloworld)
        task1.join()
        self.assertTrue(len(manager.get_failed_tasks()) == 1)
        self.assertTrue(task1 in manager.get_failed_tasks())
        self.assertFalse(task2 in manager.get_failed_tasks())
        del manager


    def test_newtask(self):
        manager = TaskManager()
        manager.start()
        func = manager.newtask(run_afterevery(seconds=3))(raise_exception)
        task = func()
        self.assertTrue(task.is_active)
        task.join()
        self.assertFalse(task.is_active)
        del manager



if __name__ == "__main__":
    unittest.main()
