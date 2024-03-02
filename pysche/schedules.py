import datetime
from typing import Any

from .manager import TaskManager
from .bases import Schedule
from .utils import (
    parse_datetime, MinMaxValidator as minmax,
    construct_datetime_from_time, parse_time, get_datetime_now
)
from .descriptors import SetOnceDescriptor, AttributeDescriptor


month_validator = minmax(1, 12)
dayofmonth_validator = minmax(1, 31)
weekday_validator = minmax(0, 6)



class RunAt(Schedule):
    """Task will run at the specified time, everyday"""
    timedelta = AttributeDescriptor(attr_type=datetime.timedelta, default=None)
    time = SetOnceDescriptor(attr_type=datetime.time)

    def __init__(self, time: str = None, **kwargs) -> None:
        """
        Create a schedule that will be due at the specified time, everyday.

        :param time: The time to run the task. The time must be in the format, "HH:MM:SS".
        
        Example:
        ```
        run_at_12_30pm_daily = RunAt(time="12:30:00", tz="Africa/Lagos")
        @run_at_12_30pm_daily(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        super().__init__(**kwargs)
        self.time = parse_time(time=time, tzinfo=self.tz)
        self.timedelta = self.get_next_occurrence_of_time(self.time)
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
            self.timedelta = self.get_next_occurrence_of_time(self.time)
        return is_due
    
        


class RunAfterEvery(Schedule):
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
        run_afterevery_5_seconds = RunAfterEvery(seconds=5)
        @run_afterevery_5_seconds(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        self.timedelta = datetime.timedelta(
            days=days,
            seconds=seconds,
            minutes=minutes,
            hours=hours,
            weeks=weeks,
        )
        if self.timedelta.total_seconds() <= 0:
            raise ValueError("At least one of the arguments must be greater than 0")
        super().__init__(**kwargs)

        
    def is_due(self) -> bool:
        if self.parent:
            return self.parent.is_due()
        return True
    


class AtMixin:
    """Allows chaining of the 'at' schedule to other schedules."""
    def at(self: Schedule, time: str, **kwargs) -> RunAt:
        """
        Task will run at the specified time, everyday.

        :param time: The time to run the task. The time must be in the format, "HH:MM:SS".
        """
        return RunAt(time=time, parent=self, **kwargs)



class AfterEveryMixin:
    """Allows chaining of the 'afterevery' schedule to other schedules."""
    def afterevery(
        self: Schedule,
        *,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
    ) -> RunAfterEvery:
        """
        Task will run after specified interval, repeatedly.

        :param weeks: The number of weeks.
        :param days: The number of days.
        :param hours: The number of hours.
        :param minutes: The number of minutes.
        :param seconds: The number of seconds.
        """
        return RunAfterEvery(
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            parent=self,
        )


class RunFrom__To(AfterEveryMixin, Schedule):
    """
    Task will only run within the specified time frame, everyday.

    This special schedule phrase is meant to be chained with the `afterevery` phrase.
    """
    def __init__(self, *, _from: str, _to: str, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified time frame, everyday.

        :param _from: The time to start running the task. The time must be in the format, "HH:MM:SS".
        :param _to: The time to stop running the task. The time must be in the format, "HH:MM:SS".

        Example:
        ```
        run_from_12_30_to_13_30 = RunFrom__To(_from="12:30:00", _to="13:30:00", tz="Africa/Lagos")

        @run_from_12_30_to_13_30.afterevery(seconds=5)(manager=task_manager, **kwargs)
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
        time_now = get_datetime_now(tzinfo=self.tz).time().replace(tzinfo=self.tz) # Update naive time returned by .time() to aware time
        if self._from < self._to:
            # E.g; If self._from='04:00:00' and self._to='08:00:00', then we can check if the time (x),
            # lies between the range (x: '04:00:00' <= x <= '08:00:00')
            is_due = time_now >= self._from and time_now <= self._to
        else:
            # E.g; If self._from='14:00:00' and self._to='00:00:00', then we can check if the time (x),
            # satisfies any of the two conditions; 
            # -> x >= self._from
            # -> x <= self._to
            is_due = time_now >= self._from or time_now <= self._to

        if self.parent:
            return self.parent.is_due() and is_due
        return is_due


    def afterevery(
            self,
            *,
            minutes: int = 0,
            seconds: int = 0,
        ) -> RunAfterEvery:
        return super().afterevery(
            minutes=minutes,
            seconds=seconds, 
        )



class From__ToMixin:
    """Allows chaining of the 'from__to' schedule to other schedules."""
    def from__to(self: Schedule, *, _from: str, _to: str) -> RunFrom__To:
        """
        Task will only run within the specified time frame, everyday.

        :param _from: The time to start running the task. The time must be in the format, "HH:MM:SS".
        :param _to: The time to stop running the task. The time must be in the format, "HH:MM:SS".
        """
        return RunFrom__To(_from=_from, _to=_to, parent=self)


# A value to represent the absence of a timedelta
NO_TIMEDELTA = object()

class TimePeriodSchedule(From__ToMixin, AtMixin, AfterEveryMixin, Schedule):
    """
    A time period schedule is a schedule that will only be due at a specific time
    or within a specified time frame or period. This kind of schedule has its timedelta
    attribute set to None, meaning it cannot be used solely to create a task. It has to be
    chained with a schedule that has its timedelta defined to form a useable schedule clause.

    Example:
    ```python
    run_on_4th_november_from_2_30_to_2_35_afterevery_5s = RunInMonth(month=11).on_day_of_month(day=4).from__to(
        _from="14:30:00", _to="14:35:00"
    ).afterevery(seconds=5)
    ```
    """
    timedelta = SetOnceDescriptor(default=NO_TIMEDELTA)
    
    def __call__(
        self, 
        *, 
        manager: TaskManager, 
        name: str = None, 
        execute_then_wait: bool = False, 
        stop_on_error: bool = False, 
        max_retry: int = 0, 
        start_immediately: bool = True
    ):
        if self.timedelta is NO_TIMEDELTA:
            raise ValueError(
                f"The '{self.__class__.__name__}' schedule cannot be used solely to create a task."
                " It has to be chained with a schedule that has its timedelta defined to form a useable schedule clause."
            )
        return super().__call__(
            manager=manager, 
            name=name, 
            execute_then_wait=execute_then_wait, 
            stop_on_error=stop_on_error, 
            max_retry=max_retry, 
            start_immediately=start_immediately
        )
    




class DHMSAfterEveryMixin(AfterEveryMixin):
    """Overrides the "afterevery" method to allow only days, hours, minutes, and seconds arguments."""
    def afterevery(
        self, 
        *, 
        days: int = 0,
        hours: int = 0, 
        minutes: int = 0, 
        seconds: int = 0, 
    ) -> RunAfterEvery:
        return super().afterevery(
            days=days,
            hours=hours, 
            minutes=minutes, 
            seconds=seconds
        )



class HMSAfterEveryMixin(AfterEveryMixin):
    """Overrides the "afterevery" method to allow only hours, minutes, and seconds arguments."""
    def afterevery(
        self, 
        *, 
        hours: int = 0, 
        minutes: int = 0, 
        seconds: int = 0, 
    ) -> RunAfterEvery:
        return super().afterevery(
            hours=hours, 
            minutes=minutes, 
            seconds=seconds
        )



class RunOnWeekDay(HMSAfterEveryMixin, TimePeriodSchedule):
    """Task will run on the specified day of the week, every week."""
    weekday = SetOnceDescriptor(attr_type=int, validators=[weekday_validator])

    def __init__(self, weekday: int, **kwargs) -> None:
        """
        Create a schedule that will be due on the specified day of the week, every week.

        :param weekday: The day of the week (0-6). 0 is Monday and 6 is Sunday.

        Example:
        ```
        run_on_mondays = RunOnWeekDay(weekday=0)
        @run_on_mondays.afterevery(minutes=10)(manager=task_manager, **kwargs)
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



class RunOnDayOfMonth(HMSAfterEveryMixin, TimePeriodSchedule):
    """Task will run on the specified day of the month, every month."""
    day = SetOnceDescriptor(attr_type=int, validators=[dayofmonth_validator])

    def __init__(self, day: int, **kwargs):
        """
        Create a schedule that will be due on the specified day of the month, every month.

        :param day: The day of the month (1-31).

        Example:
        ```
        run_on_1st_of_every_month = RunOnDayOfMonth(day=1)
        @run_on_1st_of_every_month.at("14:21:00")(manager=task_manager, **kwargs)
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
    def on_weekday(self, weekday: int) -> RunOnWeekDay:
        """
        Task will run on the specified day of the week, every week.

        :param weekday: The day of the week (0-6). 0 is Monday and 6 is Sunday.
        """
        return RunOnWeekDay(weekday, parent=self)
    


class OnDayOfMonthMixin:
    """Allows chaining of the "on_dayofmonth" schedule to other schedules."""
    def on_dayofmonth(self, day: int) -> RunOnDayOfMonth:
        """
        Task will run on the specified day of the month, every month.

        :param day: The day of the month (1-31).
        """
        return RunOnDayOfMonth(day, parent=self)



class OnDayMixin(OnWeekDayMixin, OnDayOfMonthMixin):
    """Combines on `OnWeekDayMixin` and `OnDayOfMonthMixin`."""
    pass




class RunFromSchedule(TimePeriodSchedule):
    """Base class for all "RunFrom..." type time period schedule."""
    _from = SetOnceDescriptor()
    _to = SetOnceDescriptor()

    def __init__(self, _from: Any, _to: Any, **kwargs) -> None:
        super().__init__(**kwargs)
        self._from = _from
        self._to = _to
        return None




class RunFromWeekDay__To(HMSAfterEveryMixin, RunFromSchedule):
    """Task will only run within the specified days of the week, every week."""
    _from = SetOnceDescriptor(attr_type=int, validators=[weekday_validator])
    _to = SetOnceDescriptor(attr_type=int, validators=[weekday_validator])

    def __init__(self, *, _from: int, _to: int, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified days of the week, every week.

        :param _from: The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday.
        :param _to: The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday.
        
        Example:
        ```
        run_from_monday_to_friday = RunFromWeekDay__To(_from=0, _to=4)
        @run_from_monday_to_friday.at("11:00:00")(manager=task_manager, **kwargs)
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



class RunFromDayOfMonth__To(HMSAfterEveryMixin, RunFromSchedule):
    """Task will only run within the specified days of the month, every month."""
    _from = SetOnceDescriptor(attr_type=int, validators=[dayofmonth_validator])
    _to = SetOnceDescriptor(attr_type=int, validators=[dayofmonth_validator])

    def __init__(self, _from: int, _to: int, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified days of the month, every month.

        :param _from: The day of the month (1-31) from which the task will run.
        :param _to: The day of the month (1-31) until which the task will run.

        Example:
        ```
        run_from_1st_to_5th_of_every_month = RunFromDayOfMonth__To(_from=1, _to=5)
        @run_from_1st_to_5th_of_every_month.at("00:00:00")(manager=task_manager, **kwargs)
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
    def from_dayofmonth__to(self, _from: int, _to: int) -> RunFromDayOfMonth__To:
        """
        Task will only run within the specified days of the month, every month.

        :param _from: The day of the month (1-31) from which the task will run.
        :param _to: The day of the month (1-31) until which the task will run.
        """
        return RunFromDayOfMonth__To(_from=_from, _to=_to, parent=self)



class FromWeekDay__ToMixin:
    """Allows chaining of the "from_weekday__to" schedule to other schedules."""
    def from_weekday__to(self, _from: int, _to: int) -> RunFromWeekDay__To:
        """
        Task will only run within the specified days of the week, every week.

        :param _from: The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday.
        :param _to: The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday.
        """
        return RunFromWeekDay__To(_from=_from, _to=_to, parent=self)



class FromDay__ToMixin(FromDayOfMonth__ToMixin, FromWeekDay__ToMixin):
    """Combines `FromDayOfMonth__ToMixin` and `FromWeekDay__ToMixin`."""
    pass




class RunInMonth(DHMSAfterEveryMixin, FromDay__ToMixin, OnDayMixin, TimePeriodSchedule):
    """Task will run in specified month of the year, every year"""
    month = SetOnceDescriptor(attr_type=int, validators=[month_validator])

    def __init__(self, month: int, **kwargs) -> None:
        """
        Create a schedule that will be due in a specific month of the year, every year.

        :param month: The month of the year (1-12). 1 is January and 12 is December.

        Example:
        ```
        run_every_january = RunInMonth(month=1)
        @run_every_january.from_weekday__to(
            _from=2
            _to=6
        )
        .at("06:00:00")(
            manager=task_manager, **kwargs
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
    def in_month(self, month: int) -> RunInMonth:
        """
        Task will run in specified month of the year, every year.

        :param month: The month of the year (1-12). 1 is January and 12 is December.
        """
        return RunInMonth(month, parent=self)




class RunFromMonth__To(DHMSAfterEveryMixin, OnDayMixin, FromDay__ToMixin, RunFromSchedule):
    """Task will only run within the specified months of the year, every year."""
    _from = SetOnceDescriptor(attr_type=int, validators=[month_validator])
    _to = SetOnceDescriptor(attr_type=int, validators=[month_validator])

    def __init__(self, _from: int, _to: int, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified months of the year, every year.

        :param _from: The month of the year (1-12) from which the task will run. 1 is January and 12 is December.
        :param _to: The month of the year (1-12) until which the task will run. 1 is January and 12 is December.

        Example:
        ```
        run_from_january_to_march = RunFromMonth__To(_from=1, _to=3)
        @run_from_january_to_march.from_dayofmonth__to(
            _from=12, _to=28
        )
        .afterevery(
            minutes=5, seconds=20
        )(manager=task_manager, **kwargs)
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
    def from_month__to(self, _from: int, _to: int) -> RunFromMonth__To:
        """
        Task will only run within the specified months of the year, every year.

        :param _from: The month of the year (1-12) from which the task will run. 1 is January and 12 is December.
        :param _to: The month of the year (1-12) until which the task will run. 1 is January and 12 is December.
        """
        return RunFromMonth__To(_from=_from, _to=_to, parent=self)
  


class RunInYear(FromMonth__ToMixin, FromDay__ToMixin, InMonthMixin, OnDayMixin, TimePeriodSchedule):
    """Task will run in specified year"""
    year = SetOnceDescriptor(attr_type=int)

    def __init__(self, year: int, **kwargs) -> None:
        """
        Create a schedule that will be due in the specified year.

        :param year: The year.

        Example:
        ```
        run_in_2021 = RunInYear(year=2021)
        @run_in_2021.from_month__to(
            _from=1, _to=6
        )
        .from_weekday__to(
            _from=3, _to=5
        )
        .at("03:43:00")(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        # RunInYear cannot have a parent. It is the highest schedule from which you can start chaining
        kwargs.pop("parent", None)
        super().__init__(**kwargs)
        self.year = year
        return None

    
    def is_due(self) -> bool:
        year_now = get_datetime_now(tzinfo=self.tz).year
        return year_now == self.year



class RunFromDateTime__To(InMonthMixin, OnDayMixin, FromMonth__ToMixin, FromDay__ToMixin, RunFromSchedule):
    """Task will only run within the specified date and time range."""
    _from = SetOnceDescriptor(attr_type=str)
    _to = SetOnceDescriptor(attr_type=str)

    def __init__(self, _from: str, _to: str, **kwargs) -> None:
        """
        Create a schedule that will only be due within the specified date and time.

        :param _from: The date and time from which the task will run.
        :param _to: The date and time until which the task will run.

        Example:
        ```
        run_from_2021_01_01_00_00_to_2022_01_02_00_05 = RunFromDateTime__To(_from="2021-01-01 00:00:00", _to="2022-01-02 00:05:00")
        @run_from_2021_01_01_00_00_to_2022_01_02_00_05
        .from__to(
            _from="08:00:00", _to="15:00:00"
        )(manager=task_manager, **kwargs)
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

