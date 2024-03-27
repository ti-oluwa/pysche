import asyncio
import datetime
import unittest

from pysche.schedules import run_afterevery
from pysche.tasks import ScheduledTask, TaskCallback, CallbackTrigger
from pysche.manager import TaskManager
from pysche.exceptions import TaskExecutionError
from tests.mock import count_to_ten, raise_exception, print_helloworld, generic_callback



class TestScheduledTask(unittest.TestCase):
    
    def setUp(self) -> None:
        self.manager = TaskManager("testmanager")
        self.manager.start()
        return super().setUp()


    def test_init(self):
        task1 = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=5),
            manager=self.manager,
            name="count_to_ten_every_5_seconds",
            start_immediately=False
        )
        task2 = ScheduledTask(
            print_helloworld,
            schedule=run_afterevery(seconds=5),
            manager=self.manager,
            start_immediately=True
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
        del task1, task2


    def test_add_tag(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=5),
            manager=self.manager,
            tags=["tag1", "tag2"],
        )
        task.add_tag("tag3")
        self.assertEqual(task.tags, ["tag1", "tag2", "tag3"])
        del task

    
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
        del task
        

    def test_log(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=5),
            manager=self.manager,
            tags=["tag1", "tag2"],
        )
        try:
            task.log("This is a log message")
        except Exception as e:
            self.fail(f"Unexpected exception: {e}")

    
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
        del task


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
        del task1, task2


    def test_start(self):
        task = ScheduledTask(
            print_helloworld,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=False
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
        del task


    def test_join(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        self.assertTrue(task.is_active)
        task.join()
        self.assertFalse(task.is_active)
        del task


    def test_pause(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        self.assertTrue(task.is_active)

        with self.assertRaises(TaskExecutionError):
            task._is_paused = True
            try:
                task.pause()
            finally:
                task._is_paused = False

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
        del task

    
    def test_resume(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        self.assertTrue(task.is_active)
        self.assertFalse(task.is_paused)
        task.pause()
        self.assertTrue(task.is_active)
        self.assertTrue(task.is_paused)
        task.resume()
        self.assertTrue(task.is_active)
        self.assertFalse(task.is_paused)
        del task


    def test_cancel(self):
        task = ScheduledTask(
            raise_exception,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
        )
        self.assertTrue(task.is_active)
        self.assertFalse(task.cancelled)
        task.cancel()
        self.assertFalse(task.is_active)
        self.assertTrue(task.cancelled)


    def test_pause_after(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        self.assertFalse(task.is_paused)
        pause_task = task.pause_after(2)
        pause_task.join()
        self.assertTrue(task.is_paused)
        del task

    
    def test_pause_until(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        two_seconds_from_now_time = (datetime.datetime.now() + datetime.timedelta(seconds=2)).strftime("%H:%M:%S")
        pause_task = task.pause_until(two_seconds_from_now_time)
        self.assertTrue(task.is_paused)
        pause_task.join()
        self.assertFalse(task.is_paused)
        del task


    def test_pause_for(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        self.assertFalse(task.is_paused)
        pause_task = task.pause_for(2)
        self.assertTrue(task.is_paused)
        pause_task.join()
        self.assertFalse(task.is_paused)
        del task

    
    def test_pause_at(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        two_seconds_from_now_time = (datetime.datetime.now() + datetime.timedelta(seconds=2)).strftime("%H:%M:%S")
        pause_task = task.pause_at(two_seconds_from_now_time)
        self.assertFalse(task.is_paused)
        pause_task.join()
        self.assertTrue(task.is_paused)
        del task


    def test_pause_on(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        two_seconds_from_now_dt = (datetime.datetime.now() + datetime.timedelta(seconds=2)).strftime("%Y-%m-%d %H:%M:%S")
        pause_task = task.pause_on(two_seconds_from_now_dt)
        self.assertFalse(task.is_paused)
        pause_task.join()
        self.assertTrue(task.is_paused)
        del task


    def test_cancel_after(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        cancel_task = task.cancel_after(2)
        self.assertFalse(task.cancelled)
        cancel_task.join()
        self.assertTrue(task.cancelled)
        del task

    
    def test_cancel_at(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        two_seconds_from_now_time = (datetime.datetime.now() + datetime.timedelta(seconds=2)).strftime("%H:%M:%S")
        cancel_task = task.cancel_at(two_seconds_from_now_time)
        self.assertFalse(task.cancelled)
        cancel_task.join()
        self.assertTrue(task.cancelled)
        del task

    
    def test_cancel_on(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        two_seconds_from_now_dt = (datetime.datetime.now() + datetime.timedelta(seconds=2)).strftime("%Y-%m-%d %H:%M:%S")
        cancel_task = task.cancel_on(two_seconds_from_now_dt)
        self.assertFalse(task.cancelled)
        cancel_task.join()
        self.assertTrue(task.cancelled)
        del task


    def test_add_callback(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )

        task.add_callback(generic_callback)
        self.assertTrue(len(task.callbacks) == 1)
        self.assertIsInstance(task.callbacks[0], TaskCallback)
        self.assertTrue(task.callbacks[0].task == task)
        self.assertTrue(task.callbacks[0].func == generic_callback)
        self.assertTrue(task.callbacks[0].trigger == CallbackTrigger.ERROR)

        with self.assertRaises(ValueError):
            task.add_callback(generic_callback, "invalid_trigger")


    def test_get_callbacks(self):
        task = ScheduledTask(
            count_to_ten,
            schedule=run_afterevery(seconds=1),
            manager=self.manager,
            start_immediately=True
        )
        self.assertTrue(task.get_callbacks(CallbackTrigger.ERROR) == [])
        self.assertTrue(task.get_callbacks(CallbackTrigger.PAUSED) == [])
        self.assertTrue(task.get_callbacks(CallbackTrigger.CANCELLED) == [])

        task.add_callback(generic_callback, CallbackTrigger.ERROR)
        task.add_callback(generic_callback, CallbackTrigger.PAUSED)
        task.add_callback(generic_callback, CallbackTrigger.CANCELLED)
        task.add_callback(generic_callback, CallbackTrigger.PAUSED)

        self.assertEqual(len(task.get_callbacks(CallbackTrigger.ERROR)), 1)
        self.assertEqual(len(task.get_callbacks(CallbackTrigger.PAUSED)), 2)
        self.assertEqual(len(task.get_callbacks(CallbackTrigger.CANCELLED)), 1)

        with self.assertRaises(ValueError):
            task.get_callbacks("invalid_trigger")




if __name__ == "__main__":
    unittest.main()
