# pysche

Simple API for creating schedules and efficiently managing scheduled tasks in the background.

## Features

pysche is for you if you need:

- A light-weight and easy to use API for scheduling tasks.
- Creation of schedules that run at specific times, or at regular intervals.
- Creation of schedules that only run within a specified date/time period/range.
- Creation of complex schedules by chaining simpler schedules together.
- Timezone support for schedules.
- Fine-grained control of scheduled tasks behaviors using task managers.
- Concurrent execution of tasks.
- Detailed logging of scheduled task execution process.
- Background execution of tasks.
- Proper exception handling and reporting of errors that occur during scheduled task execution.
- To pause, resume, cancel and update tasks at runtime.
- To cache result of task execution in background.

## Usage

pysche uses three basic mechanism for achieving the above listed.

- Schedules
- Task managers
- Scheduled tasks

Let's take an overview of what these mechanisms are, and the role(s) they play when scheduling tasks.

Schedules are objects that are used to define the frequency, and when a scheduled task will be executed. Task managers are objects assigned the job of handling the execution and management of a scheduled task or group of scheduled tasks. Lastly, a scheduled task is an object that runs a function/job on a specified schedule.

**Example:**

```python
import pysche

manager = pysche.TaskManager()
s = pysche.schedules

@manager.newtask(s.run_afterevery(seconds=20))
def send_message(msg: str):
    print(msg)


def main():
    manager.start()

    task = send_message("Hello friend!")
    print(task.name)

    manager.join()

if __name__ == "__main__:
    main()
```

Now that we have an overview of these mechanisms. Let's take an in-depth look at each mechanism, their nuances and how they can/should be utilized.

### Schedules [pysche.schedules]

Schedules help us define how and when tasks should run. They are two main categories of schedules and by convention schedules start with the 'run_*' prefix e.g, 'run_at'. Listed below are the two main categories available.

- Basic schedules
- Time period based schedules

#### What are Basic schedules?

Basic schedules are the simplest form of schedules. They are used to define the frequency or exact time a task should run. All schedule clauses must always end with this kind of schedule. Below are the basic schedules available.

- `pysche.schedules.run_at`
- `pysche.schedules.run_afterevery`

Note: All schedules can take an optional timezone argument on initialization. If the timezone argument is not provided, the system timezone is used.

##### pysche.schedules.run_at

This schedule is used to define the exact time a task should run. It takes in a string representing the time in the format 'HH:MM:SS' or 'HH:MM' and an optional timezone(string) argument. If the timezone argument is not provided, the system timezone is used.

```python
import pysche

s = pysche.schedules
manager = pysche.TaskManager()

# Run task at 12:00:00
run_at_12pm_everyday = s.run_at("12:00:00")

@run_at_12pm_everyday(manager=manager)
def send_message(msg: str):
    print(msg)
```

##### pysche.schedules.run_afterevery

This schedule is used to define the frequency a task should run. You can specify the frequency in seconds, minutes, hours, days, weeks, months as keyword arguments.

```python
import pysche

s = pysche.schedules
manager = pysche.TaskManager()

# Run task every 20 seconds
run_every_4min_20sec = s.run_afterevery(minutes=4, seconds=20)

@run_every_4min_20sec(manager=manager)
def send_message(msg: str):
    print(msg)
```

Now that we have an overview of basic schedules, let's take a look at time period based schedules.

#### What are Time period based schedules?

Time period based schedules are used to specify a time period within which a task should run. There are three kinds of time period based schedules available.

- run_on_* schedules
- run_from_*__to schedules
- run_in_* schedules

> Note: Time period based schedules must be chained with a basic schedule for the schedule to be useable.

##### run_on_* schedules

These schedules are used to specify the day a task should run. Below are the available run_on_* schedules.

- `pysche.schedules.run_on_weekday`
- `pysche.schedules.run_on_dayofmonth`

###### pysche.schedules.run_on_weekday

This schedule is used to specify the day of the week a task should run. It takes in an integer representing the day of the week to run the task. The integer must be between 0 and 6, where 0 is Monday and 6 is Sunday.

```python
...
# Run task on Mondays
run_on_mondays = s.run_on_weekday(0)

@run_on_mondays.at("23:15")(manager=manager)
def send_message(msg: str):
    print(msg)
```

###### pysche.schedules.run_on_dayofmonth

This schedule is used to specify the day of the month a task should run. It takes in an integer representing the day of the month to run the task. The integer must be between 1 and 31.

```python
...
run_on_12th_of_every_month = s.run_on_dayofmonth(12)

@run_on_12th_of_every_month.at("2:20")(manager=manager)
def send_message(msg: str):
    print(msg)
```

##### run_from_*__to schedules

These kind of schedules are use to specify a timeframe within which a task should run. Below are the available run_from_*__to schedules.

- `pysche.schedules.run_from__to`
- `pysche.schedules.run_from_weekday__to`
- `pysche.schedules.run_from_dayofmonth__to`
- `pysche.schedules.run_from_month__to`
- `pysche.schedules.run_from_datetime__to`

###### pysche.schedules.run_from__to

This schedule is used to specify a timeframe within which a task should run. It takes in two strings representing the start and end time in the format 'HH:MM:SS' or 'HH:MM'.

```python
...
run_from_12pm_to_2pm = s.run_from__to("12:00", "14:00")

@run_from_12pm_to_2pm.afterevery(seconds=20)(manager=manager)
def send_message(msg: str):
    print(msg)
```

###### pysche.schedules.run_from_weekday__to

This schedule is used to specify a timeframe as weekdays within which a task should run. It takes in two integers representing the start and end day of the week to run the task. The integers must be between 0 and 6, where 0 is Monday and 6 is Sunday.

```python
...
run_from_monday_to_friday = s.run_from_weekday__to(0, 4)

@run_from_monday_to_friday.afterevery(hours=2)(manager=manager)
def send_message(msg: str):
    print(msg)
```

###### pysche.schedules.run_from_dayofmonth__to

This schedule is used to specify a timeframe as days of the month within which a task should run. It takes in two integers representing the start and end day of the month to run the task. The integers must be between 1 and 31.

```python
...
run_from_1st_to_15th = s.run_from_dayofmonth__to(1, 15)

@run_from_1st_to_15th.afterevery(days=1)(manager=manager)
def send_message(msg: str):
    print(msg)
```

###### pysche.schedules.run_from_month__to

This schedule is used to specify a timeframe as months within which a task should run. It takes in two integers representing the start and end month to run the task. The integers must be between 1 and 12.

```python
...
run_from_january_to_march = s.run_from_month__to(1, 3)

@run_from_january_to_march.afterevery(weeks=1)(manager=manager)
def send_message(msg: str):
    print(msg)
```

###### pysche.schedules.run_from_datetime__to

This schedule is used to specify a timeframe as datetime within which a task should run. It takes in two strings representing the start and end datetime in the format 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD HH:MM'.

```python
...
run_from_2022_01_01_12pm_to_2022_01_01_2pm = s.run_from_datetime__to("2022-01-01 12:00", "2022-01-01 14:00")

@run_from_2022_01_01_12pm_to_2022_01_01_2pm.afterevery(seconds=20)(manager=manager)
def send_message(msg: str):
    print(msg)
```

##### run_in_* schedules

These schedules are used to specify the time period in which a task should run. Below are the available run_in_* schedules.

- `pysche.schedules.run_in_month`
- `pysche.schedules.run_in_year`

###### pysche.schedules.run_in_month

This schedule is used to specify the month in which a task should run. It takes in an integer representing the month to run the task. The integer must be between 1 and 12.

```python
...

run_in_january = s.run_in_month(1)

@run_in_january.from_weekday__to(0, 4).afterevery(hours=2)(manager=manager)
def send_message(msg: str):
    print(msg)
```

###### pysche.schedules.run_in_year

This schedule is used to specify the year in which a task should run. It takes in an integer representing the year to run the task.

```python
...
run_in_2022 = s.run_in_year(2022)

@run_in_2022.from_month__to(1, 3).afterevery(weeks=1)(manager=manager)
def send_message(msg: str):
    print(msg)
```

#### Creating complex schedules; Schedules clauses

Sometimes, you may need to create complex schedules that are not covered by the basic schedules. In such cases, you can chain schedules together to create a complex schedule or schedule clause. A schedule clause usually starts with a time period based schedule (at the beginning of the chain) and ends with a basic schedule. We've already seen implementations of this in the examples above.

There are various permutations of schedules clauses that can be created. Below are some examples of schedule clauses.

```python
import pysche

s = pysche.schedules
manager = pysche.TaskManager()

# Run task on Mondays from 12:00 to 14:00 every 12 minutes
run_on_mondays_from_12pm_to_2pm_after_every_12min = s.run_on_weekday(0).from__to("12:00", "14:00").afterevery(minutes=12)


# Run task from April to June on the 15th of every month at 12:00
run_from_april_to_june_every_15th_at_12pm = s.run_from_month__to(4, 6).on_dayofmonth(15).at("12:00")
```

**Note that a schedule clause must always end with a basic schedule.**

When schedules are chained together, the schedule preceding a schedule in the chain is called the parent schedule, and the schedule following a schedule in the chain is called the child schedule. Given the example below, `run_from_january_to_march` is the parent schedule and `afterevery(weeks=1)` is the child schedule.

```python

run_from_january_to_march_every_1week = s.run_from_month__to(1, 3).afterevery(weeks=1)
```

#### Timezone support for schedules

All schedules can take an optional timezone argument on initialization. If the timezone argument is not provided, the system timezone is used. The timezone argument must be a string representing a valid timezone.

```python
import pysche

s = pysche.schedules
manager = pysche.TaskManager()

# Run task at 12:00:00 in the 'Africa/Lagos' timezone
run_at_12pm_everyday = s.run_at("12:00:00", tz="Africa/Lagos")
```

The above example will run the task at 12:00:00 in the 'Africa/Lagos' timezone. Also, all time or datetime objects associated with this schedule will be in the 'Africa/Lagos' timezone(not the system timezone).

You cannot use multiple timezones in a schedule. If you need to run a task in different timezones, you should create separate schedules for each timezone. The schedule chain below will not work, the timezone should only be specified once, at the beginning of the schedule chain.

```python

run_at_12pm_everyday_lagos = s.run_in_month(4, tz="Africa/Lagos").afterevery(hours=24, tz="Europe/London")
```

#### Reuseability of schedules

You do not need to create a new schedule object for each task. You can either reuse the same schedule object for multiple tasks or create a base time period based schedule and chain it with different time period/basic schedules to create different schedules.

**Option 1: Reuse the same schedule object for multiple tasks**

```python
run_on_mondays_at_12pm = s.run_on_weekday(0).at("12:00")

@run_on_mondays_at_12pm(manager=manager)
def send_message(msg: str):
    print(msg)

@run_on_mondays_at_12pm(manager=manager)
def send_email(email: str):
    print(email)
```

**Option 2: Create a base time period based schedule and chain it with different time period/basic schedules**

```python
run_on_mondays = s.run_on_weekday(0)

@run_on_mondays.at("12:00")(manager=manager)
def send_message(msg: str):
    print(msg)

@run_on_mondays.at("14:00")(manager=manager)
def send_email(email: str):
    print(email)
```

#### Schedules attributes and methods

All schedules have the following attributes and methods.

- `tz` (zoneinfo.ZoneInfo): The timezone of the schedule.
- `parent` (Schedule): The parent schedule of the schedule, if any, else it returns None.
- `wait_duration` (datetime.timedelta): The duration to wait before the schedule is due. This attribute is only accessible on basic schedules.
Accessing this attribute on a time period based schedule will raise an AttributeError.

- `is_due() -> bool`: This method is used to check if the schedule is due at the current time. It returns a boolean value.
- `get_ancestors() -> List[Schedule]`: This method returns a list of all the parent schedules of the schedule arranged in order of hierarchy.
That is, in a schedule chain like `run_on_weekday(0).from__to("12:00", "14:00").afterevery(minutes=12)`, the ancestors of `afterevery(minutes=12)` are `run_on_weekday(0)` and `from__to("12:00", "14:00")`.
- `as_string() -> str`: This method returns a string representation of the schedule it is called on. Note that in a schedule clause, this returns the string representation of the basic schedule at the end of the clause.
For example, in the schedule chain;

```python

run_on_monday_from_12pm_to_2pm_every_12min = s.run_on_weekday(0).from__to("12:00", "14:00").afterevery(minutes=12)
```

Calling `as_string()` on `run_on_monday_from_12pm_to_2pm_every_12min` will return 'run_afterevery(minutes=12)' as that is the schedule it is called on. To get the string representation of the entire schedule chain, you should do

```python

str(run_on_monday_from_12pm_to_2pm_every_12min)

# Output: 'run_on_weekday(0, tz="<timezone>").from__to("12:00", "14:00").afterevery(minutes=12)'
```
