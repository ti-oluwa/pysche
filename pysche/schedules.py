import datetime
from typing import Any, Optional, List
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

from .manager import TaskManager
from .bases import Schedule, ScheduleType
from ._utils import (
    parse_datetime, MinMaxValidator as minmax,
    construct_datetime_from_time, parse_time, 
    get_datetime_now, ensure_value_is_null
)
from .descriptors import SetOnceDescriptor, AttributeDescriptor


month_validator = minmax(1, 12)
dayofmonth_validator = minmax(1, 31)
weekday_validator = minmax(0, 6)


# ------------- BASIC SCHEDULES ----------- #

class run_at(Schedule):
    """Task will run at the specified time, everyday"""

    wait_duration = AttributeDescriptor(datetime.timedelta, default=None)
    """The timedelta to the next occurrence of the specified time."""
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

        @manager.newtask(run_at_12_30pm_daily, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(**kwargs)
        self.time = parse_time(time=time, tzinfo=self.tz)
        self.wait_duration = self.get_next_occurrence_of_time(self.time)
        return None


    def get_next_occurrence_of_time(self, time: datetime.time) -> datetime.timedelta:
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
            self.wait_duration = self.get_next_occurrence_of_time(self.time)
        return is_due
    
        


class run_afterevery(Schedule):
    """Task will run after the specified interval, repeatedly"""

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

        @manager.newtask(s.run_afterevery(seconds=5), **kwargs)
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
    

    def as_string(self) -> str:
        # Convert the wait_duration to a dictionary
        attrs_dict = {
            "weeks": self.wait_duration.days//7,
            "days": self.wait_duration.days%7,
            "hours": self.wait_duration.seconds//3600,
            "minutes": self.wait_duration.seconds%3600//60,
            "seconds": self.wait_duration.seconds%60,
        }
        # Remove any zero values
        attrs_dict = dict(filter(lambda item: item[1] > 0, attrs_dict.items()))
        # Convert the attributes to a string of format "attr=value"
        attrs_list = [f"{k}={v}" for k, v in attrs_dict.items()]
        # Join the attributes with a comma

        if self.tz and (not self.parent or self.tz != self.parent.tz):
            # if this is a standalone schedule, or this is the first schedule in a schedule clause, 
            # or the tzinfo of this schedule is different from its parent's, add the tzinfo attribute
            attrs_list.append(f"tz='{self.tz}'")
        return f"{self.__class__.__name__.lower()}({', '.join(attrs_list)})"
        
        


class AtMixin:
    """Allows chaining of the 'at' schedule to other schedules."""

    def at(self: ScheduleType, time: str, **kwargs) -> run_at:
        """
        Task will run at the specified time, everyday.

        :param time: The time to run the task. The time must be in the format, "HH:MM:SS".
        """
        return run_at(time=time, parent=self, **kwargs)



class AfterEveryMixin:
    """Allows chaining of the 'afterevery' schedule to other schedules."""

    def afterevery(
        self: ScheduleType,
        *,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0
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
            parent=self
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

    run_on_4th_november_from_2_30_to_2_35_afterevery_5s = s.run_in_month(11).on_day_of_month(4).from__to("14:30:00", "14:35:00").afterevery(seconds=5)
    ```
    """

    wait_duration = SetOnceDescriptor(validators=[ensure_value_is_null])
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
        start_immediately: bool = True
    ):
        try:
            self.wait_duration
        except KeyError:
            raise ValueError(
                f"The '{self.__class__.__name__}' schedule cannot be used solely to create a task."
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
            start_immediately=start_immediately
        )



class DHMSAfterEveryMixin(AfterEveryMixin):
    """Overrides the "afterevery" method to allow only days, hours, minutes, and seconds arguments."""

    def afterevery(
        self: ScheduleType, 
        *, 
        days: int = 0,
        hours: int = 0, 
        minutes: int = 0, 
        seconds: int = 0
    ) -> run_afterevery:
        return super().afterevery(
            days=days,
            hours=hours, 
            minutes=minutes, 
            seconds=seconds
        )



class HMSAfterEveryMixin(AfterEveryMixin):
    """Overrides the "afterevery" method to allow only hours, minutes, and seconds arguments."""

    def afterevery(
        self: ScheduleType, 
        *, 
        hours: int = 0, 
        minutes: int = 0, 
        seconds: int = 0
    ) -> run_afterevery:
        return super().afterevery(
            hours=hours, 
            minutes=minutes, 
            seconds=seconds
        )
    


class MSAfterEveryMixin(AfterEveryMixin):
    """Overrides the "afterevery" method to allow only minutes and seconds arguments."""

    def afterevery(
        self: ScheduleType, 
        *, 
        minutes: int = 0, 
        seconds: int = 0
    ) -> run_afterevery:
        return super().afterevery(
            minutes=minutes, 
            seconds=seconds
        )
    


class run_from__to(MSAfterEveryMixin, BaseTimePeriodSchedule):
    """
    Task will only run within the specified time frame, everyday.

    This special schedule phrase is meant to be chained with the `afterevery` phrase.
    """

    def __init__(self, _from: str, _to: str, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified time frame, everyday.

        :param _from: The time to start running the task. The time must be in the format, "HH:MM" or "HH:MM:SS".
        :param _to: The time to stop running the task. The time must be in the format, "HH:MM" or "HH:MM:SS".

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_from_12_30_to_13_30 = s.run_from__to("12:30:00", "13:30:00", tz="Africa/Lagos")

        @run_from_12_30_to_13_30.afterevery(seconds=5)(manager, **kwargs)
        def func():
            print("Hello world!")
        
        func()
        ```
        """
        super().__init__(**kwargs)
        self._from = parse_time(time=_from, tzinfo=self.tz)
        self._to = parse_time(time=_to, tzinfo=self.tz)
        if self._from == self._to:
            raise ValueError("'_from' time cannot be the same as '_to' time")
        return None
    

    def is_due(self) -> bool:
        def _construct_mock_datetime_with_time(time: str, tzinfo: Optional[zoneinfo.ZoneInfo] = None) -> datetime.datetime:
            mock_dt_str = f"2000-01-01 {time}"
            return parse_datetime(mock_dt_str, tzinfo)
        
        time_now_str_in_machine_tz = get_datetime_now().strftime("%H:%M:%S")
        mock_dt_with_time_now = _construct_mock_datetime_with_time(time_now_str_in_machine_tz)
        mock_dt_with_from_time = _construct_mock_datetime_with_time(self._from.strftime("%H:%M:%S"), self.tz)
        mock_dt_with_to_time = _construct_mock_datetime_with_time(self._to.strftime("%H:%M:%S"), self.tz)

        if self._from < self._to:
            # E.g; If self._from='04:00:00' and self._to='08:00:00', then we can check if the time (x),
            # lies between the range (x: '04:00:00' <= x <= '08:00:00')
            is_due = (mock_dt_with_time_now >= mock_dt_with_from_time) and (mock_dt_with_time_now <= mock_dt_with_to_time)
        else:
            # E.g; If self._from='14:00:00' and self._to='00:00:00', then we can check if the time (x),
            # satisfies any of the two conditions; 
            # -> x >= self._from
            # -> x <= self._to
            is_due = (mock_dt_with_time_now >= mock_dt_with_from_time) or (mock_dt_with_time_now <= mock_dt_with_to_time)

        if self.parent:
            return self.parent.is_due() and is_due
        return is_due



class From__ToMixin:
    """Allows chaining of the 'from__to' schedule to other schedules."""

    def from__to(self: ScheduleType, _from: str, _to: str) -> run_from__to:
        """
        Task will only run within the specified time frame, everyday.

        :param _from: The time to start running the task. The time must be in the format, "HH:MM" or "HH:MM:SS".
        :param _to: The time to stop running the task. The time must be in the format, "HH:MM" or "HH:MM:SS".
        """
        return run_from__to(_from=_from, _to=_to, parent=self)



class TimePeriodSchedule(From__ToMixin, AtMixin, BaseTimePeriodSchedule):
    __doc__ = BaseTimePeriodSchedule.__doc__
    pass



class run_on_weekday(HMSAfterEveryMixin, TimePeriodSchedule):
    """Task will run on the specified day of the week, every week."""

    weekday = SetOnceDescriptor(int, validators=[weekday_validator])
    """The day of the week (0-6) on which the task will run. 0 is Monday and 6 is Sunday."""

    def __init__(self, weekday: int, **kwargs) -> None:
        """
        Create a schedule that will be due on the specified day of the week, every week.

        :param weekday: The day of the week (0-6). 0 is Monday and 6 is Sunday.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_on_mondays = s.run_on_weekday(weekday=0)

        @manager.newtask(run_on_mondays.afterevery(minutes=10), **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(**kwargs)
        self.weekday = weekday
        return None

    
    def is_due(self) -> bool:
        weekday_now = get_datetime_now(tzinfo=self.tz).weekday()
        is_due = weekday_now == self.weekday
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due



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



class OnWeekDayMixin:
    """Allows chaining of the "on_weekday" schedule to other schedules."""

    def on_weekday(self: ScheduleType, weekday: int) -> run_on_weekday:
        """
        Task will run on the specified day of the week, every week.

        :param weekday: The day of the week (0-6). 0 is Monday and 6 is Sunday.
        """
        return run_on_weekday(weekday, parent=self)
    


class OnDayOfMonthMixin:
    """Allows chaining of the "on_dayofmonth" schedule to other schedules."""

    def on_dayofmonth(self: ScheduleType, day: int) -> run_on_dayofmonth:
        """
        Task will run on the specified day of the month, every month.

        :param day: The day of the month (1-31).
        """
        return run_on_dayofmonth(day, parent=self)



class OnDayMixin(OnWeekDayMixin, OnDayOfMonthMixin):
    """Combines on `OnWeekDayMixin` and `OnDayOfMonthMixin`."""
    pass




class RunFromSchedule(TimePeriodSchedule):
    """Base class for all "RunFrom...__to" type time period schedule."""

    _from = SetOnceDescriptor()
    """The start of the time period."""
    _to = SetOnceDescriptor()
    """The end of the time period."""

    def __init__(self, _from: Any, _to: Any, **kwargs) -> None:
        super().__init__(**kwargs)
        self._from = _from
        self._to = _to
        return None




class run_from_weekday__to(HMSAfterEveryMixin, RunFromSchedule):
    """Task will only run within the specified days of the week, every week."""

    _from = SetOnceDescriptor(int, validators=[weekday_validator])
    """The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday."""
    _to = SetOnceDescriptor(int, validators=[weekday_validator])
    """The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday."""

    def __init__(self, _from: int, _to: int, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified days of the week, every week.

        :param _from: The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday.
        :param _to: The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday.
        
        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_from_monday_to_friday = s.run_from_weekday__to(0, 4)

        @run_from_monday_to_friday.at("11:00:00")(manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if _from == _to:
            raise ValueError("'_from' and '_to' cannot be the same")
        return super().__init__(_from, _to, **kwargs)

    
    def is_due(self) -> bool:
        weekday_now = get_datetime_now(tzinfo=self.tz).weekday()
        if self._from < self._to:
            # E.g; If self._from=0 and self._to=5, then we can check if the weekday (x),
            # lies between the range (x: 0 <= x <= 5)
            is_due = weekday_now >= self._from and weekday_now <= self._to
        else:
            # E.g; If self._from=6 and self._to=3, then we can check if the weekday (x),
            # satisfies any of the two conditions; 
            # -> x >= self._from
            # -> x <= self._to
            is_due = weekday_now >= self._from or weekday_now <= self._to

        if self.parent:
            return self.parent.is_due() and is_due
        return is_due



class run_from_dayofmonth__to(HMSAfterEveryMixin, RunFromSchedule):
    """Task will only run within the specified days of the month, every month."""

    _from = SetOnceDescriptor(int, validators=[dayofmonth_validator])
    """The day of the month (1-31) from which the task will run."""
    _to = SetOnceDescriptor(int, validators=[dayofmonth_validator])
    """The day of the month (1-31) until which the task will run."""

    def __init__(self, _from: int, _to: int, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified days of the month, every month.

        :param _from: The day of the month (1-31) from which the task will run.
        :param _to: The day of the month (1-31) until which the task will run.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_from_1st_to_5th_of_every_month = s.run_from_dayofmonth__to(1, 5)

        @manager.newtask(run_from_1st_to_5th_of_every_month.at("00:00:00"), **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if _from == _to:
            raise ValueError("'_from' and '_to' cannot be the same")
        return super().__init__(_from, _to, **kwargs)             

    
    def is_due(self) -> bool:
        today = get_datetime_now(tzinfo=self.tz).day
        if self._from < self._to:
            # E.g; If self._from=4 and self._to=6, then we can check if the day (x),
            # lies between the range (x: 4 <= x <= 6)
            is_due = today >= self._from and today <= self._to
        else:
            # E.g; If self._from=11 and self._to=3, then we can check if the day (x),
            # satisfies any of the two conditions; 
            # -> x >= self._from
            # -> x <= self._to
            is_due = today >= self._from or today <= self._to

        if self.parent:
            return self.parent.is_due() and is_due
        return is_due



class FromDayOfMonth__ToMixin:
    """Allows chaining of the "from_dayofmonth__to" schedule to other schedules"""

    def from_dayofmonth__to(self: ScheduleType, _from: int, _to: int) -> run_from_dayofmonth__to:
        """
        Task will only run within the specified days of the month, every month.

        :param _from: The day of the month (1-31) from which the task will run.
        :param _to: The day of the month (1-31) until which the task will run.
        """
        return run_from_dayofmonth__to(_from=_from, _to=_to, parent=self)



class FromWeekDay__ToMixin:
    """Allows chaining of the "from_weekday__to" schedule to other schedules."""

    def from_weekday__to(self: ScheduleType, _from: int, _to: int) -> run_from_weekday__to:
        """
        Task will only run within the specified days of the week, every week.

        :param _from: The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday.
        :param _to: The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday.
        """
        return run_from_weekday__to(_from=_from, _to=_to, parent=self)



class FromDay__ToMixin(FromDayOfMonth__ToMixin, FromWeekDay__ToMixin):
    """Combines `FromDayOfMonth__ToMixin` and `FromWeekDay__ToMixin`."""
    pass




class run_in_month(DHMSAfterEveryMixin, FromDay__ToMixin, OnDayMixin, TimePeriodSchedule):
    """Task will run in specified month of the year, every year"""

    month = SetOnceDescriptor(int, validators=[month_validator])
    """The month of the year (1-12) in which the task will run. 1 is January and 12 is December."""

    def __init__(self, month: int, **kwargs) -> None:
        """
        Create a schedule that will be due in a specific month of the year, every year.

        :param month: The month of the year (1-12). 1 is January and 12 is December.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_every_january = s.run_in_month(month=1)

        @pysche.task(
            schedule=run_every_january.from_weekday__to(2, 6).at("06:00:00"), 
            manager=manager,
            **kwargs
        )
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(**kwargs)
        self.month = month
        return None

    
    def is_due(self) -> bool:
        month_now = get_datetime_now(tzinfo=self.tz).month
        is_due = month_now == self.month
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due



class InMonthMixin:
    """Allows chaining of the "in_month" schedule to other schedules."""

    def in_month(self: ScheduleType, month: int) -> run_in_month:
        """
        Task will run in specified month of the year, every year.

        :param month: The month of the year (1-12). 1 is January and 12 is December.
        """
        return run_in_month(month, parent=self)




class run_from_month__to(DHMSAfterEveryMixin, OnDayMixin, FromDay__ToMixin, RunFromSchedule):
    """Task will only run within the specified months of the year, every year."""

    _from = SetOnceDescriptor(int, validators=[month_validator])
    """The month of the year (1-12) from which the task will run. 1 is January and 12 is December."""
    _to = SetOnceDescriptor(int, validators=[month_validator])
    """The month of the year (1-12) until which the task will run. 1 is January and 12 is December."""

    def __init__(self, _from: int, _to: int, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified months of the year, every year.

        :param _from: The month of the year (1-12) from which the task will run. 1 is January and 12 is December.
        :param _to: The month of the year (1-12) until which the task will run. 1 is January and 12 is December.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_from_january_to_march = s.run_from_month__to(1, 3)

        @run_from_january_to_march.from_dayofmonth__to(12, 28)
        .afterevery(minutes=5, seconds=20)(manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if _from == _to:
            raise ValueError("'_from' and '_to' cannot be the same")
        return super().__init__(_from, _to, **kwargs)

    
    def is_due(self) -> bool:
        month_now = get_datetime_now(tzinfo=self.tz).month
        if self._from < self._to:
            # E.g; If self._from=4 and self._to=8, then we can check if the month (x),
            # lies between the range (x: 4 <= x <= 8)
            is_due: bool = month_now >= self._from and month_now <= self._to
        else:
            # E.g; If self._from=11 and self._to=3, then we can check if the month (x),
            # satisfies any of the two conditions; 
            # -> x >= self._from
            # -> x <= self._to
            is_due: bool = month_now >= self._from or month_now <= self._to

        if self.parent:
            return self.parent.is_due() and is_due
        return is_due

    

class FromMonth__ToMixin:
    """Allows chaining of the "from_month__to" schedule to other schedules."""

    def from_month__to(self: ScheduleType, _from: int, _to: int) -> run_from_month__to:
        """
        Task will only run within the specified months of the year, every year.

        :param _from: The month of the year (1-12) from which the task will run. 1 is January and 12 is December.
        :param _to: The month of the year (1-12) until which the task will run. 1 is January and 12 is December.
        """
        return run_from_month__to(_from=_from, _to=_to, parent=self)
  


class run_in_year(FromMonth__ToMixin, FromDay__ToMixin, InMonthMixin, OnDayMixin, TimePeriodSchedule):
    """Task will run in specified year"""

    year = SetOnceDescriptor(int)
    """The year in which the task will run."""

    def __init__(self, year: int, **kwargs) -> None:
        """
        Create a schedule that will be due in the specified year.

        :param year: The year.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_in_2021 = s.run_in_year(2021)

        @run_in_2021.from_month__to(1, 6)
        .from_weekday__to(3, 5).at("03:43:00")(manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        # run_in_year cannot have a parent. It is the highest schedule from which you can start chaining
        kwargs.pop("parent", None)
        super().__init__(**kwargs)
        self.year = year
        return None

    
    def is_due(self) -> bool:
        year_now = get_datetime_now(tzinfo=self.tz).year
        return year_now == self.year



class run_from_datetime__to(InMonthMixin, OnDayMixin, FromMonth__ToMixin, FromDay__ToMixin, RunFromSchedule):
    """Task will only run within the specified date and time range."""

    _from = SetOnceDescriptor(str)
    """The date and time from which the task will run."""
    _to = SetOnceDescriptor(str)
    """The date and time until which the task will run."""

    def __init__(self, _from: str, _to: str, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified date and time.

        :param _from: The date and time from which the task will run.
        :param _to: The date and time until which the task will run.

        Example:
        ```
        import pysche

        manager = pysche.TaskManager()
        s = pysche.schedules
        run_from_2021_01_01_00_00_to_2022_01_02_00_05 = s.run_from_datetime__to("2021-01-01 00:00:00", "2022-01-02 00:05:00")

        @run_from_2021_01_01_00_00_to_2022_01_02_00_05
        .from__to("08:00:00", "15:00:00")(manager, **kwargs)
        def func():
            print("Hello world!")
        
        func()
        ```
        """
        super().__init__(_from, _to, **kwargs)
        self._from = parse_datetime(dt=_from, tzinfo=self.tz)
        self._to = parse_datetime(dt=_to, tzinfo=self.tz)

        # For datetimes self._from must always be greater than self._to
        if self._from > self._to:
            raise ValueError("'_from' cannot be greater than '_to'")
        return None
    

    def is_due(self) -> bool:
        now = get_datetime_now(tzinfo=self.tz)
        is_due = now >= self._from and now <= self._to
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due


