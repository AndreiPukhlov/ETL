import time


def time_count(func):
    def time_wrapper(*args, **kwargs):
        start = time.time()
        func(*args, **kwargs)
        end = time.time()
        result = end - start
        if result < 60:
            if result < 2:
                print(f"It took {round(result)} second")
            else:
                print(f"It took {round(result)} seconds")

        else:
            if result < 120:
                print(f"It took {round(result / 60)} minute and {round(result % 60)} seconds")
            else:
                print(f"It took {round(result / 60)} minutes and {round(result % 60)} seconds")

    return time_wrapper


def get_execution_time(func):
    start_time = time.time()
    func()
    end_time = time.time()
    importing_time = (end_time - start_time) / 60
    print(f"It took {importing_time} minutes to import this csv file")