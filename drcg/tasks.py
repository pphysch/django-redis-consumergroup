task_registry = dict()

def register(func, task_name=None):
    if task_name == None:
        task_name = func.__name__

    if task_name in task_registry:
        raise ValueError(f"task name collision for '{task_name}'")

    task_registry[task_name] = func

    return func