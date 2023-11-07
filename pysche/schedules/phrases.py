import datetime
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo
from typing import Any

from .bases import (
    BaseSchedulePhrase, 
    _AtPhraseMixin, 
    _AfterEveryPhraseMixin, 
    _From__ToPhraseMixin, 
)



class SchedulePhrase(_From__ToPhraseMixin, _AtPhraseMixin, _AfterEveryPhraseMixin, BaseSchedulePhrase):
    """
    A schedule phrase is a schedule that can be chained with other schedule phrases to create a complex schedule
    called a "schedule clause".

    Example:
    ```python
    run_on_nov_4_from_2_30_to_2_35_afterevery_5s = RunInMonth(month=11).on_day_of_month(day=4).from__to(from_hour=2, from_minute=30, to_hour=2, to_minute=35).afterevery(seconds=5)
    ```
    """
    pass

# You can make complex schedules called "schedule clauses" by chaining schedule phrases together.



################################ DAY-BASED "RunOn..." PHRASES AND PHRASE-MIXINS ###########################################

class RunOnWeekDay(SchedulePhrase):
    """Task will run on the specified day of the week, every week."""
    def __init__(self, weekday: int, **kwargs):
        """
        Create a schedule that will be due on the specified day of the week, every week.

        :param weekday: The day of the week (0-6). 0 is Monday and 6 is Sunday.

        Example:
        ```
        run_on_mondays = RunOnWeekDay(weekday=0)
        @run_on_mondays(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if weekday < 0 or weekday > 6:
            raise ValueError("'weekday' must be between 0 and 6")
        self.weekday = weekday
        super().__init__(**kwargs)

    
    def is_due(self) -> bool:
        weekday_now = datetime.datetime.now().weekday()
        if self.parent:
            return self.parent.is_due() and weekday_now == self.weekday
        return weekday_now == self.weekday


    def afterevery(
            self, 
            *, 
            hours: int = 0, 
            minutes: int = 0, 
            seconds: int = 0, 
            milliseconds: int = 0, 
            microseconds: int = 0
        ):
        return super().afterevery(
            hours=hours, 
            minutes=minutes, 
            seconds=seconds, 
            milliseconds=milliseconds, 
            microseconds=microseconds
        )



class RunOnDayOfMonth(SchedulePhrase):
    """Task will run on the specified day of the month, every month."""
    def __init__(self, day: int, **kwargs):
        """
        Create a schedule that will be due on the specified day of the month, every month.

        :param day: The day of the month (1-31).

        Example:
        ```
        run_on_1st_of_month = RunOnDayOfMonth(day=1)
        @run_on_1st_of_month(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")
         
        func()
        ```
        """
        if day < 1 or day > 31:
            raise ValueError("'day' must be between 1 and 31")
        self.day = day
        super().__init__(**kwargs)

    
    def is_due(self) -> bool:
        today = datetime.datetime.now().day
        if self.parent:
            return self.parent.is_due() and today == self.day
        return today == self.day


    def afterevery(
            self, 
            *, 
            hours: int = 0, 
            minutes: int = 0, 
            seconds: int = 0, 
            milliseconds: int = 0, 
            microseconds: int = 0
        ):
        return super().afterevery(
            hours=hours, 
            minutes=minutes, 
            seconds=seconds, 
            milliseconds=milliseconds, 
            microseconds=microseconds
        )



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

class RunFromSchedulePhrase(SchedulePhrase):
    """Base class for all "RunFrom..." schedule phrases."""
    def __init__(self, _from: Any, _to: Any, tz: str | datetime.tzinfo, **kwargs):
        super().__init__(**kwargs)
        self._from = _from
        self._to = _to
        self.tz = zoneinfo.ZoneInfo(tz) if tz and isinstance(tz, str) else tz
        if not self.tz and self.parent:
            if getattr(self.parent, "tz", None):
                self.tz = self.parent.tz
        return None




class RunFromWeekDay__To(RunFromSchedulePhrase):
    """Task will only run within the specified days of the week, every week."""
    def __init__(self, *, _from: int, _to: int, tz: str | datetime.tzinfo = None, **kwargs):
        """
        Create a schedule that will only be due within the specified days of the week, every week.

        :param _from: The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday.
        :param _to: The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday.
        :param tz: The timezone to use. If None, the local timezone will be used.
        
        Example:
        ```
        run_from_monday_to_friday = RunFromWeekDay__To(_from=0, _to=4)
        @run_from_monday_to_friday(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if _from < 0 or _from > 6:
            raise ValueError("'_from' must be between 0 and 6")
        if _to < 0 or _to > 6:
            raise ValueError("'_to' must be between 0 and 6")
        if _from == _to:
            raise ValueError("'_from' and '_to' cannot be the same")
        super().__init__(_from, _to, tz, **kwargs)
        

    
    def is_due(self):
        weekday_now = datetime.datetime.now().weekday()
        if self.parent:
            return self.parent.is_due() and weekday_now >= self._from and weekday_now <= self._to
        return weekday_now >= self._from and weekday_now <= self._to


    def afterevery(
            self,
            *,
            hours: int = 0,
            minutes: int = 0,
            seconds: int = 0,
            milliseconds: int = 0,
            microseconds: int = 0
        ):
        return super().afterevery(
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            milliseconds=milliseconds,
            microseconds=microseconds
        )



class RunFromDayOfMonth__To(RunFromSchedulePhrase):
    """Task will only run within the specified days of the month, every month."""
    def __init__(self, _from: int, _to: int, tz: str | datetime.tzinfo = None, **kwargs):
        """
        Create a schedule that will only be due within the specified days of the month, every month.

        :param _from: The day of the month (1-31) from which the task will run.
        :param _to: The day of the month (1-31) until which the task will run.
        :param tz: The timezone to use. If None, the local timezone will be used.

        Example:
        ```
        run_from_1st_to_5th_of_month = RunFromDayOfMonth__To(_from=1, _to=5)
        @run_from_1st_to_5th_of_month(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if _from < 1 or _from > 31:
            raise ValueError("'_from' must be between 1 and 31")
        if _to < 1 or _to > 31:
            raise ValueError("'_to' must be between 1 and 31")
        if _from == _to:
            raise ValueError("'_from' and '_to' cannot be the same")
        super().__init__(_from, _to, tz, **kwargs)             

    
    def is_due(self):
        today = datetime.datetime.now(tz=self.tz).day
        if self.parent:
            return self.parent.is_due() and today >= self._from and today <= self._to
        return today >= self._from and today <= self._to
    

    def afterevery(
            self,
            *,
            hours: int = 0,
            minutes: int = 0,
            seconds: int = 0,
            milliseconds: int = 0,
            microseconds: int = 0
        ):
        return super().afterevery(
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            milliseconds=milliseconds,
            microseconds=microseconds
        )




# ---------------------------- DAY-BASED "RunFrom...__To" PHRASE-MIXINS ---------------------------- #

class _FromDayOfMonth__ToPhraseMixin:
    """Allows chaining of the "from_dayofmonth__to" phrase to other schedule phrases"""
    def from_dayofmonth__to(self, _from: int, _to: int, tz: str | datetime.tzinfo = None):
        """
        Task will only run within the specified days of the month, every month.

        :param _from: The day of the month (1-31) from which the task will run.
        :param _to: The day of the month (1-31) until which the task will run.
        :param tz: The timezone to use. If None, the local timezone will be used.
        """
        return RunFromDayOfMonth__To(_from=_from, _to=_to, tz=tz, parent=self)



class _FromWeekDay__ToPhraseMixin:
    """Allows chaining of the "from_weekday__to" phrase to other schedule phrases."""
    def from_weekday__to(self, _from: int, _to: int, tz: str | datetime.tzinfo = None):
        """
        Task will only run within the specified days of the week, every week.

        :param _from: The day of the week (0-6) from which the task will run. 0 is Monday and 6 is Sunday.
        :param _to: The day of the week (0-6) until which the task will run. 0 is Monday and 6 is Sunday.
        :param tz: The timezone to use. If None, the local timezone will be used.
        """
        return RunFromWeekDay__To(_from=_from, _to=_to, tz=tz, parent=self)



class _FromDay__ToPhraseMixin(_FromDayOfMonth__ToPhraseMixin, _FromWeekDay__ToPhraseMixin):
    pass


# ---------------------------- END DAY_BASED "RunFrom...__To" PHRASE-MIXINS ---------------------------- #


###################################### END DAY-BASED "RunFrom...__To" PHRASES AND PHRASE-MIXINS ###########################################







###################################### MONTH-BASED PHRASES AND PHRASE-MIXINS ###########################################

class RunInMonth(_FromDay__ToPhraseMixin, _OnDayPhraseMixin, SchedulePhrase):
    """Task will run in specified month of the year, every year"""
    def __init__(self, month: int, **kwargs):
        """
        Create a schedule that will be due in a specific month of the year, every year.

        :param month: The month of the year (1-12). 1 is January and 12 is December.

        Example:
        ```
        run_in_january = RunInMonth(month=1)
        @run_in_january(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if month < 1 or month > 12:
            raise ValueError("'month' must be between 1 and 12")
        self.month = month
        super().__init__(**kwargs)

    
    def is_due(self) -> bool:
        month_now = datetime.datetime.now().month
        if self.parent:
            return self.parent.is_due() and month_now == self.month
        return month_now == self.month


    def afterevery(
            self, 
            *, 
            days: int = 0,
            hours: int = 0, 
            minutes: int = 0, 
            seconds: int = 0, 
            milliseconds: int = 0, 
            microseconds: int = 0
        ):
        return super().afterevery(
            days=days,
            hours=hours, 
            minutes=minutes, 
            seconds=seconds, 
            milliseconds=milliseconds, 
            microseconds=microseconds
        )



class _InMonthPhraseMixin:
    """Allows chaining of the "in_month" phrase to other schedule phrases."""
    def in_month(self, month: int):
        """
        Task will run in specified month of the year, every year.

        :param month: The month of the year (1-12). 1 is January and 12 is December.
        """
        return RunInMonth(month, parent=self)




class RunFromMonth__To(_OnDayPhraseMixin, _FromDay__ToPhraseMixin, RunFromSchedulePhrase):
    """Task will only run within the specified months of the year, every year."""
    def __init__(self, _from: int, _to: int, tz: str | datetime.tzinfo = None, **kwargs):
        """
        Create a schedule that will only be due within the specified months of the year, every year.

        :param _from: The month of the year (1-12) from which the task will run. 1 is January and 12 is December.
        :param _to: The month of the year (1-12) until which the task will run. 1 is January and 12 is December.
        :param tz: The timezone to use. If None, the local timezone will be used.

        Example:
        ```
        run_from_january_to_march = RunFromMonth__To(_from=1, _to=3)
        @run_from_january_to_march(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        if _from < 1 or _from > 12:
            raise ValueError("'_from' must be between 1 and 12")
        if _to < 1 or _to > 12:
            raise ValueError("'_to' must be between 1 and 12")
        if _from == _to:
            raise ValueError("'_from' and '_to' cannot be the same")
        super().__init__(_from, _to, tz, **kwargs)

    
    def is_due(self):
        month_now = datetime.datetime.now().month
        if self.parent:
            return self.parent.is_due() and month_now >= self._from and month_now <= self._to
        return month_now >= self._from and month_now <= self._to
    

    def afterevery(
        self,
        *,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        milliseconds: int = 0,
        microseconds: int = 0
    ):
        return super().afterevery(
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            milliseconds=milliseconds,
            microseconds=microseconds
        )



class _FromMonth__ToPhraseMixin:
    """Allows chaining of the "from_month__to" phrase to other schedule phrases."""
    def from_month__to(self, _from: int, _to: int, tz: str | datetime.tzinfo = None):
        """
        Task will only run within the specified months of the year, every year.

        :param _from: The month of the year (1-12) from which the task will run. 1 is January and 12 is December.
        :param _to: The month of the year (1-12) until which the task will run. 1 is January and 12 is December.
        """
        return RunFromMonth__To(_from=_from, _to=_to, tz=tz, parent=self)
  

###################################### END MONTH-BASED PHRASES AND PHRASE-MIXINS ###########################################







###################################### YEAR-BASED PHRASES AND PHRASE-MIXINS ###########################################


class RunInYear(
        _FromMonth__ToPhraseMixin, 
        _FromDay__ToPhraseMixin, 
        _InMonthPhraseMixin, 
        _OnDayPhraseMixin, 
        SchedulePhrase
    ):
    """Task will run in specified year"""
    def __init__(self, year: int):
        """
        Create a schedule that will be due in the specified year.

        :param year: The year.

        Example:
        ```
        run_in_2021 = RunInYear(year=2021)
        @run_in_2021(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")

        func()
        ```
        """
        self.year = year
        # RunInYear cannot have a parent. 
        # It is the highest schedule from which you can start chaining
        super().__init__()

    
    def is_due(self) -> bool:
        year_now = datetime.datetime.now().year
        return year_now == self.year



def _parse_datetime(dt: str, tzinfo: datetime.tzinfo = None) -> datetime.datetime:
    """
    Parse a datetime string in format "%Y-%m-%d %H:%M:%S" into a datetime.datetime object.
    """
    if tzinfo is None:
        tzinfo = datetime.datetime.now().astimezone().tzinfo
    return datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tzinfo)



class RunFromDateTime__To(
        _InMonthPhraseMixin, 
        _OnDayPhraseMixin, 
        _FromMonth__ToPhraseMixin, 
        _FromDay__ToPhraseMixin, 
        RunFromSchedulePhrase
    ):
    """Task will only run within the specified date and time range."""
    def __init__(self, _from: str, _to: str, tz: str | datetime.tzinfo = None):
        """
        Create a schedule that will only be due within the specified date and time.

        :param _from: The date and time from which the task will run.
        :param _to: The date and time until which the task will run.
        :param tz: The timezone to use. If None, the local timezone will be used.

        Example:
        ```
        run_from_2021_01_01_00_00_to_2022_01_01_00_05 = RunFromDateTime__To(_from="2021-01-01 00:00:00", _to="2022-01-01 00:05:00")
        @run_from_2021_01_01_00_00_to_2022_01_01_00_05(manager=task_manager, **kwargs)
        def func():
            print("Hello world!")
        
        func()
        ```
        """
        super().__init__(_from, _to, tz)
        self._from = _parse_datetime(dt=_from, tzinfo=self.tz)
        self._to = _parse_datetime(dt=_to, tzinfo=self.tz)
        if self._from > self._to:
            raise ValueError("'_from' cannot be greater than '_to'")
        return None
    

    def is_due(self):
        now = datetime.datetime.now(tz=self.tz)
        if self.parent:
            return self.parent.is_due() and now >= self._from and now <= self._to
        return now >= self._from and now <= self._to


###################################### END YEAR-BASED PHRASES AND PHRASE-MIXINS ###########################################

