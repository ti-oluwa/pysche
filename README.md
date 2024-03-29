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
- Callback execution for specific event triggers.

## Usage

pysche uses three basic mechanism for achieving the above listed.

- Schedules
- Task managers
- Scheduled tasks

Let's take an overview of what these mechanisms are, and the role(s) they play when scheduling tasks.

Schedules are objects that are used to define the frequency, and when a scheduled task will be executed. Task managers are objects assigned the job of handling the execution and management of a scheduled task or group of scheduled tasks. Lastly, a scheduled task is an object that runs a function/job on a specified schedule.

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

<!-- ### Schedules -->
