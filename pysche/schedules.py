import datetime
from typing import Any

from .bases import (
    BaseSchedule, _AtPhraseMixin, 
    _AfterEveryPhraseMixin, _From__ToPhraseMixin, 
    RunAfterEvery
)
from ._utils import SetOnceDescriptor, parse_datetime


class Schedule(
    _From__ToPhraseMixin, 
    _AtPhraseMixin, 
    _AfterEveryPhraseMixin, 
    BaseSchedule
):
    """
    A schedule defines how a task will be run. Schedules can be chained together to create complex schedules
    called a "schedule clause". A schedule clause is a combination of schedules that define when a task will run.
    A schedule clause must always end with a base schedule, a schedule that defines the frequency or time of the task execution.

    A schedule can have its timedelta set to None.

    Example:
    ```python
    run_on_4th_november_from_2_30_to_2_35_afterevery_5s = RunInMonth(month=11).on_day_of_month(day=4).from__to(
        _from="14:30:00", _to="14:35:00"
    ).afterevery(seconds=5)
    ```
    """
    pass

# You can make complex schedules called "schedule clauses" by chaining schedules together.



################################ DAY-BASED "RunOn..." PHRASES AND PHRASE-MIXINS ###########################################

class _HMSAfterEveryPhraseMixin(_AfterEveryPhraseMixin):
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



class RunOnWeekDay(_HMSAfterEveryPhraseMixin, Schedule):
    """Task will run on the specified day of the week, every week."""
    weekday = SetOnceDescriptor(attr_type=int, validators=[lambda x: 0 <= x <= 6])

    def __init__(self, weekday: int, **kwargs):
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
        weekday_now = datetime.datetime.now(tz=self.tz).weekday()
        is_due = weekday_now == self.weekday
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due



class RunOnDayOfMonth(_HMSAfterEveryPhraseMixin, Schedule):
    """Task will run on the specified day of the month, every month."""
    day = SetOnceDescriptor(attr_type=int, validators=[lambda x: 1 <= x <= 31])

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
        today = datetime.datetime.now(tz=self.tz).day
        is_due = today == self.day
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due



# ---------------------------- DAY-BASED "RunOn..." PHRASE-MIXINS ---------------------------- #

class _OnWeekDayPhraseMixin:
    """Allows chaining of the "on_weekday" phrase to other schedule phrases."""
    def on_weekday(self, weekday: int):
        """
        Task will run on the specified day of the week, every week.

        :param weekday: The day of the week (0-6). 0 is Monday and 6 is Sunday.
        """
        return RunOnWeekDay(weekday, parent=self)
    


class _OnDayOfMonthPhraseMixin:
    """Allows chaining of the "on_dayofmonth" phrase to other schedule phrases."""
    def on_dayofmonth(self, day: int):
        """
        Task will run on the specified day of the month, every month.

        :param day: The day of the month (1-31).
        """
        return RunOnDayOfMonth(day, parent=self)



class _OnDayPhraseMixin(_OnWeekDayPhraseMixin, _OnDayOfMonthPhraseMixin):
    pass


# ---------------------------- DAY-BASED "RunOn..." PHRASE-MIXINS ---------------------------- #


################################ END DAY-BASED "RunOn..." PHRASES AND PHRASE-MIXINS ###########################################







###################################### DAY_BASED "RunFrom...__To" PHRASES AND PHRASE-MIXINS ###########################################

class RunFromSchedule(Schedule):
    """Base class for all "RunFrom..." schedule phrases."""
    _from = SetOnceDescriptor(attr_type=Any)
    _to = SetOnceDescriptor(attr_type=Any)

    def __init__(self, _from: Any, _to: Any, **kwargs):
        super().__init__(**kwargs)
        self._from = _from
        self._to = _to
        return None




class RunFromWeekDay__To(_HMSAfterEveryPhraseMixin, RunFromSchedule):
    """Task will only run within the specified days of the week, every week."""
    _from = SetOnceDescriptor(attr_type=int, validators=[lambda x: 0 <= x <= 6])
    _to = SetOnceDescriptor(attr_type=int, validators=[lambda x: 0 <= x <= 6])

    def __init__(self, *, _from: int, _to: int, **kwargs):
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
        super().__init__(_from, _to, **kwargs)

    
    def is_due(self) -> bool:
        weekday_now = datetime.datetime.now(tz=self.tz).weekday()
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



class RunFromDayOfMonth__To(_HMSAfterEveryPhraseMixin, RunFromSchedule):
    """Task will only run within the specified days of the month, every month."""
    _from = SetOnceDescriptor(attr_type=int, validators=[lambda x: 1 <= x <= 31])
    _to = SetOnceDescriptor(attr_type=int, validators=[lambda x: 1 <= x <= 31])

    def __init__(self, _from: int, _to: int, **kwargs):
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
        super().__init__(_from, _to, **kwargs)             

    
    def is_due(self) -> bool:
        today = datetime.datetime.now(tz=self.tz).day
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




# ---------------------------- DAY-BASED "RunFrom...__To" PHRASE-MIXINS ---------------------------- #

class _FromDayOfMonth__ToPhraseMixin:
    """Allows chaining of the "from_dayofmonth__to" phrase to other schedule phrases"""
    def from_dayofmonth__to(self, _from: int, _to: int):
        """
        Task will only run within the specified days of the month, every month.

        :param _from: The day of the month (1-31) from which the task will run.
        :param _to: The day of the month (1-31) until which the task will run.
        """
        return RunFromDayOfMonth__To(_from=_from, _to=_to, parent=self)



class _FromWeekDay__ToPhraseMixin:
    """Allows chaining of the "from_weekday__to" phrase to other schedule phrases."""
    def from_weekday__to(self, _from: int, _to: int):
        """
        Task will only run within the specified days of the week, every week.

        :param _from: The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday.
        :param _to: The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday.
        """
        return RunFromWeekDay__To(_from=_from, _to=_to, parent=self)



class _FromDay__ToPhraseMixin(_FromDayOfMonth__ToPhraseMixin, _FromWeekDay__ToPhraseMixin):
    pass


# ---------------------------- END DAY_BASED "RunFrom...__To" PHRASE-MIXINS ---------------------------- #


###################################### END DAY-BASED "RunFrom...__To" PHRASES AND PHRASE-MIXINS ###########################################







###################################### MONTH-BASED PHRASES AND PHRASE-MIXINS ###########################################

class _DHMSAfterEveryPhraseMixin(_AfterEveryPhraseMixin):
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
    


class RunInMonth(
    _DHMSAfterEveryPhraseMixin, 
    _FromDay__ToPhraseMixin, 
    _OnDayPhraseMixin, 
    Schedule
):
    """Task will run in specified month of the year, every year"""
    month = SetOnceDescriptor(attr_type=int, validators=[lambda x: 1 <= x <= 12])

    def __init__(self, month: int, **kwargs):
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
        month_now = datetime.datetime.now(tz=self.tz).month
        is_due = month_now == self.month
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due



class _InMonthPhraseMixin:
    """Allows chaining of the "in_month" phrase to other schedule phrases."""
    def in_month(self, month: int):
        """
        Task will run in specified month of the year, every year.

        :param month: The month of the year (1-12). 1 is January and 12 is December.
        """
        return RunInMonth(month, parent=self)




class RunFromMonth__To(
    _DHMSAfterEveryPhraseMixin, 
    _OnDayPhraseMixin, 
    _FromDay__ToPhraseMixin, 
    RunFromSchedule
):
    """Task will only run within the specified months of the year, every year."""
    _from = SetOnceDescriptor(attr_type=int, validators=[lambda x: 1 <= x <= 12])
    _to = SetOnceDescriptor(attr_type=int, validators=[lambda x: 1 <= x <= 12])

    def __init__(self, _from: int, _to: int, **kwargs):
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
        super().__init__(_from, _to, **kwargs)

    
    def is_due(self) -> bool:
        month_now = datetime.datetime.now(tz=self.tz).month
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



class _FromMonth__ToPhraseMixin:
    """Allows chaining of the "from_month__to" phrase to other schedule phrases."""
    def from_month__to(self, _from: int, _to: int):
        """
        Task will only run within the specified months of the year, every year.

        :param _from: The month of the year (1-12) from which the task will run. 1 is January and 12 is December.
        :param _to: The month of the year (1-12) until which the task will run. 1 is January and 12 is December.
        """
        return RunFromMonth__To(_from=_from, _to=_to, parent=self)
  

###################################### END MONTH-BASED PHRASES AND PHRASE-MIXINS ###########################################







###################################### YEAR-BASED PHRASES AND PHRASE-MIXINS ###########################################


class RunInYear(
    _FromMonth__ToPhraseMixin, 
    _FromDay__ToPhraseMixin, 
    _InMonthPhraseMixin, 
    _OnDayPhraseMixin, 
    Schedule
):
    """Task will run in specified year"""
    year = SetOnceDescriptor(attr_type=int)

    def __init__(self, year: int, **kwargs):
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
        year_now = datetime.datetime.now(tz=self.tz).year
        return year_now == self.year



class RunFromDateTime__To(
    _InMonthPhraseMixin, 
    _OnDayPhraseMixin, 
    _FromMonth__ToPhraseMixin, 
    _FromDay__ToPhraseMixin, 
    RunFromSchedule
):
    """Task will only run within the specified date and time range."""
    _from = SetOnceDescriptor(attr_type=str)
    _to = SetOnceDescriptor(attr_type=str)

    def __init__(self, _from: str, _to: str, **kwargs):
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
        now = datetime.datetime.now(tz=self.tz)
        is_due = now >= self._from and now <= self._to
        if self.parent:
            return self.parent.is_due() and is_due
        return is_due


###################################### END YEAR-BASED PHRASES AND PHRASE-MIXINS ###########################################

