import os
from taskdb import TaskDB
import argparse as ap
import json


database = TaskDB("tasks.db")

if __name__ == "__main__":
    parser = ap.ArgumentParser()
    parser.add_argument('--task-file',required=True)

    args = parser.parse_args()

    if not os.path.isfile(args.task_file):
        raise FileExistsError(f"Error, the file {args.task_file} does not exist!")
    database.ensure_tasks_exist()
    database.load_tasks_from_file(args.task_file)
