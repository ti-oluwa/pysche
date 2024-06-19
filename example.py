from typing import Any
import httpx
import random
import rich

import pysche


manager = pysche.TaskManager()
s = pysche.schedules
run_from_2am_to_9pm = s.run_from__to("02:00", "21:00")


def get_random_rm_char_id(id_limit: int = 826) -> str:
    return str(random.randint(0, id_limit))


@manager.newtask(
    run_from_2am_to_9pm.afterevery(seconds=20), max_retries=2, save_results=True
)
def fetch_rm_character(char_id: str) -> Any | None:
    char_url = f"https://rickandmortyapi.com/api/character/{char_id}"
    with httpx.Client() as client:
        response = client.get(char_url)

        if response.status_code == 200:
            rich.print(response.json())
            return response.json()
        response.raise_for_status()
    return


@manager.newtask(run_from_2am_to_9pm.afterevery(seconds=15))
def update_rm_task_args(rm_task: pysche.ScheduledTask) -> None:
    rm_task.args = (get_random_rm_char_id(),)


def main() -> None:
    # Start task execution
    manager.start()

    rm_task = fetch_rm_character("20")
    update_rm_task_args(rm_task)

    # Wait for the scheduled tasks to finish
    manager.join()


if __name__ == "__main__":
    main()
