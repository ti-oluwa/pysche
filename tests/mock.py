import time


def print_helloworld():
    print("Hello World")


def say_hello(name: str):
    print(f"Hello {name}")


def print_current_time():
    print(time.time())


def count_to_ten():
    for i in range(10):
        print(i)


def delay_print_hello_world(seconds: int):
    time.sleep(seconds)
    print("Hello World")


def raise_exception():
    raise Exception("This is an exception")
