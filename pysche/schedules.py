import datetime
from typing import Any, Optional, List, Dict, Union

try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

from .taskmanager import TaskManager
from .baseschedule import Schedule, ScheduleType
from ._utils import (
    parse_datetime,
    MinMaxValidator as minmax,
    construct_datetime_from_time,
    parse_time,
    get_datetime_now,
    ensure_value_is_null,
    weekday_to_str,
    str_to_weekday,
    month_to_str,
    str_to_month,
)
from .descriptors import SetOnceDescriptor, null


month_validator = minmax(1, 12)
dayofmonth_validator = minmax(1, 31)
weekday_validator = minmax(0, 6)


# ------------- BASIC SCHEDULES ----------- #


class run_at(Schedule):
    """Task will run at the specified time, everyday"""

    time = SetOnceDescriptor(datetime.time)
    """Time in the day when the task will run."""

    def __init__(self, time: str, **kwargs) -> None:
        """
        Create a schedule that will be due at the specified time, everyday.

        :param time: The time to run the task. The time must be in the format, "HH:MM" or "HH:MM:SS".

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_at_12_30pm_daily = s.run_at("12:30:00", tz="Africa/Lagos")

        @manager.taskify(run_at_12_30pm_daily, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(**kwargs)
        self.time = parse_time(time=time, tzinfo=self.tz)
        self.wait_duration = self.get_next_occurrence(self.time)
        return None

    def get_next_occurrence(self, time: datetime.time) -> datetime.timedelta:
        """
        Calculates the timedelta to the next occurrence of the specified time in the given datetime.
        If the time is in the future, the timedelta will be the difference between the current time and the specified time.
        """
        # Since we cannot compare a time to a datetime, we need to convert the time to a datetime
        # The date part of the datetime will be the today's date and the time part will be the specified time.
        datetime_from_time = construct_datetime_from_time(time)
        current_datetime = get_datetime_now(tzinfo=self.tz)

        # If the datetime_from_time is greater than the current_datetime, then given time has not occurred today
        # E.g:
        # time = 12:00:00
        # datetime_from_time = 2021-10-10 12:00:00
        # current_datetime = 2021-10-10 11:00:00
        # 'time' (12:00:00) has not occurred today since the current time is still 11:00:00
        # Hence, the timedelta will be the difference between the specified time and the current time.
        if datetime_from_time > current_datetime:
            return datetime_from_time - current_datetime

        # If the datetime_from_time is less than the current_datetime, then given time has occurred today
        # The next occurrence of time will be the next day
        # E.g:
        # time = 12:00:00
        # datetime_from_time = 2021-10-10 12:00:00
        # current_datetime = 2021-10-10 13:00:00
        # 'time' (12:00:00) has occurred today since the current time is already 13:00:00
        # Hence, the timedelta will be the difference between one day from now, and the
        # difference between the current time and the specified time.
        return datetime.timedelta(days=1) - (current_datetime - datetime_from_time)

    def is_due(self) -> bool:
        is_due = True
        if self.parent:
            is_due = self.parent.is_due() and is_due

        if is_due:
            self.wait_duration = self.get_next_occurrence(self.time)
        return is_due

    def __describe__(self) -> str:
        return f"Task will run at {self.time.strftime('%H:%M:%S %z')} everyday."


class run_afterevery(Schedule):
    """Task will run after the specified interval, repeatedly"""

    wait_duration = SetOnceDescriptor(datetime.timedelta, default=None)

    def __init__(
        self,
        *,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        **kwargs,
    ) -> None:
        """
        Create a schedule that will be due after the specified interval, repeatedly.

        :param weeks: The number of weeks.
        :param days: The number of days.
        :param hours: The number of hours.
        :param minutes: The number of minutes.
        :param seconds: The number of seconds.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules

        @manager.taskify(s.run_afterevery(seconds=5), **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        self.wait_duration = datetime.timedelta(
            days=days,
            seconds=seconds,
            minutes=minutes,
            hours=hours,
            weeks=weeks,
        )
        if self.wait_duration.total_seconds() <= 0:
            raise ValueError("At least one of the arguments must be greater than 0")
        super().__init__(**kwargs)

    def is_due(self) -> bool:
        if self.parent:
            return self.parent.is_due()
        return True

    def destructure_wait_duration(self) -> Dict[str, int]:
        """Convert the wait_duration to a dictionary of weeks, days, hours, minutes, and seconds."""
        return {
            "weeks": self.wait_duration.days // 7,
            "days": self.wait_duration.days % 7,
            "hours": self.wait_duration.seconds // 3600,
            "minutes": self.wait_duration.seconds % 3600 // 60,
            "seconds": self.wait_duration.seconds % 60,
        }

    def as_string(self) -> str:
        # Convert the wait_duration to a dictionary
        wd_dict = self.destructure_wait_duration()
        # Remove any zero values
        wd_dict = dict(filter(lambda item: item[1] > 0, wd_dict.items()))
        # Convert the items to a string of format "key=value"
        wd_list = [f"{k}={v}" for k, v in wd_dict.items()]
        # Join the items with a comma

        if self.tz and (not self.parent or self.tz != self.parent.tz):
            # if this is a standalone schedule, or this is the first schedule in a schedule clause,
            # or the tzinfo of this schedule is different from its parent's, add the tzinfo attribute
            wd_list.append(f"tz='{self.tz}'")
        return f"{self.__class__.__name__.lower()}({', '.join(wd_list)})"

    def __describe__(self) -> str:
        # Convert the wait_duration to a dictionary
        wd_dict = self.destructure_wait_duration()
        # Remove any zero values
        wd_dict = dict(filter(lambda item: item[1] > 0, wd_dict.items()))
        # Convert the items to a string of format "value key"
        wd_list = [f"{v} {k}" for k, v in wd_dict.items()]
        return f"Task will run after every {', '.join(wd_list)}."


class AtMixin:
    """Allows chaining of the 'run_at' schedule to other schedules."""

    def at(self: ScheduleType, time: str, **kwargs) -> run_at:
        """
        Task will run at the specified time, everyday.

        :param time: The time to run the task. The time must be in the format, "HH:MM:SS".
        """
        return run_at(time=time, parent=self, **kwargs)


class AfterEveryMixin:
    """Allows chaining of the 'run_afterevery' schedule to other schedules."""

    def afterevery(
        self: ScheduleType,
        *,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
    ) -> run_afterevery:
        """
        Task will run after specified interval, repeatedly.

        :param weeks: The number of weeks.
        :param days: The number of days.
        :param hours: The number of hours.
        :param minutes: The number of minutes.
        :param seconds: The number of seconds.
        """
        return run_afterevery(
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            parent=self,
        )


# ------------ TIME PERIOD BASED SCHEDULES -------------- #


class BaseTimePeriodSchedule(AfterEveryMixin, Schedule):
    """
    A time period schedule is a schedule that will only be due at a specific time
    or within a specified time frame or period. This kind of schedule has no wait period/duration, 
    meaning it cannot be used solely to create a task. It has to be
    chained with a schedule that has its wait period/duration defined to form a useable schedule clause.

    Example:
    ```python
    import pysche

    manager = pysche.TaskManager()
    s = pysche.schedules

    run_on_4th_november_from_2_30_to_2_35_afterevery_5s = s.run_in_month(11)\
                                                            .on_dayofmonth(4)\
                                                            .within("14:30:00", "14:35:00")\
                                                            .afterevery(seconds=5)
    ```
    """

    wait_duration = SetOnceDescriptor(null, validators=[ensure_value_is_null])
    """Time period schedules have no wait duration. Accessing this attribute will raise an error."""

    def __call__(
        self,
        *,
        manager: TaskManager,
        name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        execute_then_wait: bool = False,
        save_results: bool = False,
        resultset_size: Optional[int] = None,
        stop_on_error: bool = False,
        max_retries: int = 0,
        start_immediately: bool = True,
    ):
        try:
            self.wait_duration
        except AttributeError:
            raise ValueError(
                f"The '{type(self).__name__}' schedule cannot be used solely to create a task."
                " It has to be chained with a schedule that has it's wait duration specified to form a useable schedule clause."
            )
        return super().__call__(
            manager=manager,
            name=name,
            tags=tags,
            execute_then_wait=execute_then_wait,
            save_results=save_results,
            resultset_size=resultset_size,
            stop_on_error=stop_on_error,
            max_retries=max_retries,
            start_immediately=start_immediately,
        )


class DHMSAfterEveryMixin(AfterEveryMixin):
    """Overrides the "afterevery" method to allow only days, hours, minutes, and seconds arguments."""

    def afterevery(
        self: ScheduleType,
        *,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
    ) -> run_afterevery:
        return super().afterevery(
            days=days, hours=hours, minutes=minutes, seconds=seconds
        )


class HMSAfterEveryMixin(AfterEveryMixin):
    """Overrides the "afterevery" method to allow only hours, minutes, and seconds arguments."""

    def afterevery(
        self: ScheduleType, *, hours: int = 0, minutes: int = 0, seconds: int = 0
    ) -> run_afterevery:
        return super().afterevery(hours=hours, minutes=minutes, seconds=seconds)


class MSAfterEveryMixin(AfterEveryMixin):
    """Overrides the "afterevery" method to allow only minutes and seconds arguments."""

    def afterevery(
        self: ScheduleType, *, minutes: int = 0, seconds: int = 0
    ) -> run_afterevery:
        return super().afterevery(minutes=minutes, seconds=seconds)


class run_within(MSAfterEveryMixin, BaseTimePeriodSchedule):
    """
    Task will only run within the specified time frame, everyday.

    This special schedule phrase is meant to be chained with the `afterevery` phrase.
    """

    start = SetOnceDescriptor(datetime.time)
    """The time to start running the task."""
    end = SetOnceDescriptor(datetime.time)
    """The time to stop running the task."""

    def __init__(self, start: str, end: str, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified time frame, everyday.

        :param start: The time to start running the task. The time must be in the format, "HH:MM" or "HH:MM:SS".
        :param end: The time to stop running the task. The time must be in the format, "HH:MM" or "HH:MM:SS".

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_from_12_30_to_13_30 = s.run_within("12:30:00", "13:30:00", tz="Africa/Lagos")

        @run_from_12_30_to_13_30.afterevery(seconds=5)(manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(**kwargs)
        self.start = parse_time(time=start, tzinfo=self.tz)
        self.end = parse_time(time=end, tzinfo=self.tz)
        if self.start == self.end:
            raise ValueError("'start' time cannot be the same as 'end' time")
        return None

    def is_due(self) -> bool:
        def _construct_mock_datetime_with_time(
            time: str, tzinfo: Optional[zoneinfo.ZoneInfo] = None
        ) -> datetime.datetime:
            mock_dt_str = f"2000-01-01 {time}"
            return parse_datetime(mock_dt_str, tzinfo)

        time_now_str_in_machine_tz = get_datetime_now().strftime("%H:%M:%S")
        mock_dt_with_time_now = _construct_mock_datetime_with_time(
            time_now_str_in_machine_tz
        )
        mock_dt_with_from_time = _construct_mock_datetime_with_time(
            self.start.strftime("%H:%M:%S"), self.tz
        )
        mock_dt_with_to_time = _construct_mock_datetime_with_time(
            self.end.strftime("%H:%M:%S"), self.tz
        )

        if self.start < self.end:
            # E.g; If self.start='04:00:00' and self.end='08:00:00', then we can check if the time (x),
            # lies between the range (x: '04:00:00' <= x <= '08:00:00')
            is_due = (mock_dt_with_time_now >= mock_dt_with_from_time) and (
                mock_dt_with_time_now <= mock_dt_with_to_time
            )
        else:
            # E.g; If self.start='14:00:00' and self.end='00:00:00', then we can check if the time (x),
            # satisfies any of the two conditions;
            # -> x >= self.start
            # -> x <= self.end
            is_due = (mock_dt_with_time_now >= mock_dt_with_from_time) or (
                mock_dt_with_time_now <= mock_dt_with_to_time
            )

        if self.parent:
            return self.parent.is_due() and is_due
        return is_due

    def __describe__(self) -> str:
        return f"Task will run from {self.start.strftime('%H:%M:%S')} to {self.end.strftime('%H:%M:%S %z')} everyday."


class WithinMixin:
    """Allows chaining of the 'run_within' schedule to other schedules."""

    def within(self: ScheduleType, start: str, end: str) -> run_within:
        """
        Task will only run within the specified time frame, everyday.

        :param start: The time to start running the task. The time must be in the format, "HH:MM" or "HH:MM:SS".
        :param end: The time to stop running the task. The time must be in the format, "HH:MM" or "HH:MM:SS".
        """
        return run_within(start=start, end=end, parent=self)


class TimePeriodSchedule(WithinMixin, AtMixin, BaseTimePeriodSchedule):
    __doc__ = BaseTimePeriodSchedule.__doc__
    pass


class run_on_weekday(HMSAfterEveryMixin, TimePeriodSchedule):
    """Task will run on the specified day of the week, every week."""

    weekday = SetOnceDescriptor(int, validators=[weekday_validator])
    """The day of the week (0-6) on which the task will run. 0 is Monday and 6 is Sunday."""

    def __init__(self, weekday: Union[int, str], **kwargs) -> None:
        """
        Create a schedule that will be due on the specified day of the week, every week.

        :param weekday: The day of the week (0-6). 0 is Monday and 6 is Sunday.
        You can also pass the name of the weekday as a string. E.g; "Monday", "Tuesday", etc.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_on_mondays = s.run_on_weekday(weekday=0)

        @manager.taskify(run_on_mondays.afterevery(minutes=10), **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(**kwargs)
        if isinstance(weekday, str):
            weekday = str_to_weekday(weekday)
        self.weekday = weekday
        return None

    def is_due(self) -> bool:
        weekday_now = get_datetime_now(tzinfo=self.tz).weekday()
        is_due = weekday_now == self.weekday
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due

    def __describe__(self) -> str:
        return f"Task will run on {weekday_to_str(self.weekday)}s."


class run_on_dayofmonth(HMSAfterEveryMixin, TimePeriodSchedule):
    """Task will run on the specified day of the month, every month."""

    day = SetOnceDescriptor(int, validators=[dayofmonth_validator])
    """The day of the month (1-31) on which the task will run."""

    def __init__(self, day: int, **kwargs):
        """
        Create a schedule that will be due on the specified day of the month, every month.

        :param day: The day of the month (1-31).

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_on_1st_of_every_month = s.run_on_dayofmonth(day=1)

        @run_on_1st_of_every_month.at("14:21:00")(manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(**kwargs)
        self.day = day
        return None

    def is_due(self) -> bool:
        today = get_datetime_now(tzinfo=self.tz).day
        is_due = today == self.day
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due

    def __describe__(self) -> str:
        return f"Task will run on the {self.day} of every month."


class OnWeekDayMixin:
    """Allows chaining of the "run_on_weekday" schedule to other schedules."""

    def on_weekday(self: ScheduleType, weekday: Union[int, str]) -> run_on_weekday:
        """
        Task will run on the specified day of the week, every week.

        :param weekday: The day of the week (0-6). 0 is Monday and 6 is Sunday.
        You can also pass the name of the weekday as a string. E.g; "Monday", "Tuesday", etc.
        """
        return run_on_weekday(weekday, parent=self)


class OnDayOfMonthMixin:
    """Allows chaining of the "run_on_dayofmonth" schedule to other schedules."""

    def on_dayofmonth(self: ScheduleType, day: int) -> run_on_dayofmonth:
        """
        Task will run on the specified day of the month, every month.

        :param day: The day of the month (1-31).
        """
        return run_on_dayofmonth(day, parent=self)


class OnDayMixin(OnWeekDayMixin, OnDayOfMonthMixin):
    """Combines `OnWeekDayMixin` and `OnDayOfMonthMixin`."""

    pass


class RunWithinSchedule(TimePeriodSchedule):
    """Base class for all "run_within_..." type time period schedule."""

    start = SetOnceDescriptor()
    """The start of the time period."""
    end = SetOnceDescriptor()
    """The end of the time period."""

    def __init__(self, start: Any, end: Any, **kwargs) -> None:
        super().__init__(**kwargs)
        self.start = start
        self.end = end
        return None


class run_within_weekday(HMSAfterEveryMixin, RunWithinSchedule):
    """Task will only run within the specified days of the week, every week."""

    start = SetOnceDescriptor(int, validators=[weekday_validator])
    """The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday."""
    end = SetOnceDescriptor(int, validators=[weekday_validator])
    """The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday."""

    def __init__(self, start: Union[int, str], end: Union[int, str], **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified days of the week, every week.

        :param start: The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday.
        :param end: The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday.

        You can also pass the name of the weekdays as a string. E.g; "Monday", "Tuesday", etc.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_from_monday_to_friday = s.run_within_weekday(0, 4)

        @run_from_monday_to_friday.at("11:00:00")(manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if start == end:
            raise ValueError("'start' and 'end' cannot be the same")
        if isinstance(start, str):
            start = str_to_weekday(start)
        if isinstance(end, str):
            end = str_to_weekday(end)
        return super().__init__(start, end, **kwargs)

    def is_due(self) -> bool:
        weekday_now = get_datetime_now(tzinfo=self.tz).weekday()
        if self.start < self.end:
            # E.g; If self.start=0 and self.end=5, then we can check if the weekday (x),
            # lies between the range (x: 0 <= x <= 5)
            is_due = weekday_now >= self.start and weekday_now <= self.end
        else:
            # E.g; If self.start=6 and self.end=3, then we can check if the weekday (x),
            # satisfies any of the two conditions;
            # -> x >= self.start
            # -> x <= self.end
            is_due = weekday_now >= self.start or weekday_now <= self.end

        if self.parent:
            return self.parent.is_due() and is_due
        return is_due

    def __describe__(self) -> str:
        return f"Task will run from {weekday_to_str(self.start)} to {weekday_to_str(self.end)}."


class run_within_dayofmonth(HMSAfterEveryMixin, RunWithinSchedule):
    """Task will only run within the specified days of the month, every month."""

    start = SetOnceDescriptor(int, validators=[dayofmonth_validator])
    """The day of the month (1-31) from which the task will run."""
    end = SetOnceDescriptor(int, validators=[dayofmonth_validator])
    """The day of the month (1-31) until which the task will run."""

    def __init__(self, start: int, end: int, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified days of the month, every month.

        :param start: The day of the month (1-31) from which the task will run.
        :param end: The day of the month (1-31) until which the task will run.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_from_1st_to_5th_of_every_month = s.run_within_dayofmonth(1, 5)

        @manager.taskify(run_from_1st_to_5th_of_every_month.at("00:00:00"), **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if start == end:
            raise ValueError("'start' and 'end' cannot be the same")
        return super().__init__(start, end, **kwargs)

    def is_due(self) -> bool:
        today = get_datetime_now(tzinfo=self.tz).day
        if self.start < self.end:
            # E.g; If self.start=4 and self.end=6, then we can check if the day (x),
            # lies between the range (x: 4 <= x <= 6)
            is_due = today >= self.start and today <= self.end
        else:
            # E.g; If self.start=11 and self.end=3, then we can check if the day (x),
            # satisfies any of the two conditions;
            # -> x >= self.start
            # -> x <= self.end
            is_due = today >= self.start or today <= self.end

        if self.parent:
            return self.parent.is_due() and is_due
        return is_due

    def __describe__(self) -> str:
        return f"Task will run from {self.start} to {self.end} of every month."


class WithinDayOfMonthMixin:
    """Allows chaining of the "run_within_dayofmonth" schedule to other schedules"""

    def within_dayofmonth(
        self: ScheduleType, start: int, end: int
    ) -> run_within_dayofmonth:
        """
        Task will only run within the specified days of the month, every month.

        :param start: The day of the month (1-31) from which the task will run.
        :param end: The day of the month (1-31) until which the task will run.
        """
        return run_within_dayofmonth(start=start, end=end, parent=self)


class WithinWeekDayMixin:
    """Allows chaining of the "run_within_weekday" schedule to other schedules."""

    def within_weekday(
        self: ScheduleType, start: Union[int, str], end: Union[int, str]
    ) -> run_within_weekday:
        """
        Task will only run within the specified days of the week, every week.

        :param start: The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday.
        :param end: The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday.

        You can also pass the name of the weekday as a string. E.g; "Monday", "Tuesday", etc.
        """
        return run_within_weekday(start=start, end=end, parent=self)


class WithinDayMixin(WithinDayOfMonthMixin, WithinWeekDayMixin):
    """Combines `WithinDayOfMonthMixin` and `WithinWeekDayMixin`."""

    pass


class run_in_month(DHMSAfterEveryMixin, WithinDayMixin, OnDayMixin, TimePeriodSchedule):
    """Task will run in specified month of the year, every year"""

    month = SetOnceDescriptor(int, validators=[month_validator])
    """The month of the year (1-12) in which the task will run. 1 is January and 12 is December."""

    def __init__(self, month: Union[int, str], **kwargs) -> None:
        """
        Create a schedule that will be due in a specific month of the year, every year.

        :param month: The month of the year (1-12). 1 is January and 12 is December.
        You can also pass the name of the month as a string. E.g; "January", "February", etc.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_every_january = s.run_in_month(month=1)

        @pysche.task(
            schedule=run_every_january.within_weekday(2, 6).at("06:00:00"),
            manager=manager,
            **kwargs
        )
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(**kwargs)
        if isinstance(month, str):
            month = str_to_month(month)
        self.month = month
        return None

    def is_due(self) -> bool:
        month_now = get_datetime_now(tzinfo=self.tz).month
        is_due = month_now == self.month
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due

    def __describe__(self) -> str:
        return f"Task will run in {month_to_str(self.month)}."


class InMonthMixin:
    """Allows chaining of the "in_month" schedule to other schedules."""

    def in_month(self: ScheduleType, month: Union[int, str]) -> run_in_month:
        """
        Task will run in specified month of the year, every year.

        :param month: The month of the year (1-12). 1 is January and 12 is December.
        You can also pass the name of the month as a string. E.g; "January", "February", etc.
        """
        return run_in_month(month, parent=self)


class run_within_month(
    DHMSAfterEveryMixin, OnDayMixin, WithinDayMixin, RunWithinSchedule
):
    """Task will only run within the specified months of the year, every year."""

    start = SetOnceDescriptor(int, validators=[month_validator])
    """The month of the year (1-12) from which the task will run. 1 is January and 12 is December."""
    end = SetOnceDescriptor(int, validators=[month_validator])
    """The month of the year (1-12) until which the task will run. 1 is January and 12 is December."""

    def __init__(self, start: Union[int, str], end: Union[int, str], **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified months of the year, every year.

        :param start: The month of the year (1-12) from which the task will run. 1 is January and 12 is December.
        :param end: The month of the year (1-12) until which the task will run. 1 is January and 12 is December.

        You can also pass the name of the month as a string. E.g; "January", "February", etc.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_from_january_to_march = s.run_within_month(1, 3)

        @run_from_january_to_march.within_dayofmonth(12, 28)
        .afterevery(minutes=5, seconds=20)(manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if start == end:
            raise ValueError("'start' and 'end' cannot be the same")
        if isinstance(start, str):
            start = str_to_month(start)
        if isinstance(end, str):
            end = str_to_month(end)
        return super().__init__(start, end, **kwargs)

    def is_due(self) -> bool:
        month_now = get_datetime_now(tzinfo=self.tz).month
        if self.start < self.end:
            # E.g; If self.start=4 and self.end=8, then we can check if the month (x),
            # lies between the range (x: 4 <= x <= 8)
            is_due: bool = month_now >= self.start and month_now <= self.end
        else:
            # E.g; If self.start=11 and self.end=3, then we can check if the month (x),
            # satisfies any of the two conditions;
            # -> x >= self.start
            # -> x <= self.end
            is_due: bool = month_now >= self.start or month_now <= self.end

        if self.parent:
            return self.parent.is_due() and is_due
        return is_due

    def __describe__(self) -> str:
        return f"Task will run from {month_to_str(self.start)} to {month_to_str(self.end)}."


class WithinMonthMixin:
    """Allows chaining of the "run_within_month" schedule to other schedules."""

    def within_month(
        self: ScheduleType, start: Union[int, str], end: Union[int, str]
    ) -> run_within_month:
        """
        Task will only run within the specified months of the year, every year.

        :param start: The month of the year (1-12) from which the task will run. 1 is January and 12 is December.
        :param end: The month of the year (1-12) until which the task will run. 1 is January and 12 is December.

        You can also pass the name of the month as a string. E.g; "January", "February", etc.
        """
        return run_within_month(start=start, end=end, parent=self)


class run_in_year(
    WithinMonthMixin, WithinDayMixin, InMonthMixin, OnDayMixin, TimePeriodSchedule
):
    """Task will run in specified year"""

    year = SetOnceDescriptor(int)
    """The year in which the task will run."""

    def __init__(self, year: Union[int, str], **kwargs) -> None:
        """
        Create a schedule that will be due in the specified year.

        :param year: The year.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_in_2021 = s.run_in_year(2021)

        @run_in_2021.within_month(1, 6)
        .within_weekday(3, 5).at("03:43:00")(manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        # run_in_year cannot have a parent. It is the highest schedule from which you can start chaining
        kwargs.pop("parent", None)
        super().__init__(**kwargs)
        self.year = int(year)
        return None

    def is_due(self) -> bool:
        year_now = get_datetime_now(tzinfo=self.tz).year
        return year_now == self.year

    def __describe__(self) -> str:
        return f"Task will run in {self.year}."


class run_within_datetime(
    InMonthMixin, OnDayMixin, WithinMonthMixin, WithinDayMixin, RunWithinSchedule
):
    """Task will only run within the specified date and time range."""

    start = SetOnceDescriptor(str)
    """The date and time from which the task will run."""
    end = SetOnceDescriptor(str)
    """The date and time until which the task will run."""

    def __init__(self, start: str, end: str, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified date and time.

        :param start: The date and time from which the task will run.
        :param end: The date and time until which the task will run.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_from_2021_01_01_00_00_to_2022_01_02_00_05 = s.run_within_datetime("2021-01-01 00:00:00", "2022-01-02 00:05:00")

        @run_from_2021_01_01_00_00_to_2022_01_02_00_05
        .within("08:00:00", "15:00:00")(manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(start, end, **kwargs)
        self.start = parse_datetime(dt=start, tzinfo=self.tz)
        self.end = parse_datetime(dt=end, tzinfo=self.tz)

        # For datetimes self.start must always be greater than self.end
        if self.start > self.end:
            raise ValueError("'start' cannot be greater than 'end'")
        return None

    def is_due(self) -> bool:
        now = get_datetime_now(tzinfo=self.tz)
        is_due = now >= self.start and now <= self.end
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due

    def __describe__(self) -> str:
        return f"Task will run from {self.start.strftime('%Y-%m-%d %H:%M:%S')} to {self.end.strftime('%Y-%m-%d %H:%M:%S %z')}."
