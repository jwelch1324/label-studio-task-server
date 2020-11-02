from datetime import date
import sqlite3
import os
import json
from tqdm.auto import tqdm
import datetime


class TaskDB:
    def __init__(self, sqllite_path):
        self.dbpath = sqllite_path

    def create_db_schema(self):
        conn = self.getconn()
        conn.execute(
            """CREATE TABLE "tasks" (
        "ID"	INTEGER NOT NULL,
        "assigned"	TEXT NOT NULL DEFAULT 'None',
        "url"	TEXT NOT NULL,
        "accepted" INTEGER NOT NULL DEFAULT 0,
        "assigned_ts" INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY("ID" AUTOINCREMENT)
        );"""
        )
        conn.close()

    def check_for_task_table(self):
        conn = self.getconn()
        res = (
            len(
                conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='tasks'"
                ).fetchall()
            )
            > 0
        )
        conn.close()
        return res

    def add_task_url(self, url):
        conn = self.getconn()
        conn.execute(
            f"INSERT INTO tasks (assigned, url, assigned_ts) values ('None', '{url}',{int(datetime.datetime.now().timestamp())});"
        )
        conn.commit()
        conn.close()

    def ensure_tasks_exist(self):
        if not self.check_for_task_table():
            self.create_db_schema()

    def getconn(self):
        return sqlite3.connect(self.dbpath)

    def load_tasks_from_file(self, path):
        # We will consume a standard tasks.json file for label-studio and load it into the database
        if not os.path.isfile(path):
            raise FileExistsError(f"File {path} does not seem to exist!")

        with open(path, "r") as jfile:
            tasks = json.load(jfile)

        for key in tqdm(tasks.keys()):
            # Get the data entry as that is where the image url lives
            dd = tasks[key]["data"]
            if "image" in dd:
                url = dd["image"]

                self.add_task_url(url)

    def assign_task(self, task_id, user):
        conn = self.getconn()
        conn.execute(f"UPDATE tasks set assigned='{user}' where ID={task_id}")
        conn.execute(
            f"UPDATE tasks set assigned_ts='{int(datetime.datetime.now().timestamp())}' where ID={task_id}"
        )
        conn.commit()
        conn.close()

    def accept_task(self, task_id):
        conn = self.getconn()
        conn.execute(f"UPDATE tasks set accepted=1 where ID={task_id};")
        conn.commit()
        conn.close()

    def get_task_batch(self, username, batch_size: int):
        conn = self.getconn()
        # Select tasks from the database which are either unassigned or more than 10 seconds have passed without 
        # the assigned accetping the tasks
        res = conn.execute(
            f"SELECT ID, url from tasks where assigned='None' or (accepted=0 and ({int(datetime.datetime.now().timestamp())}-assigned_ts) > 10);"
        ).fetchall()
        if len(res) < batch_size:
            batch_size = len(res)

        tasks = {}

        for i in range(batch_size):
            tid, url = res[i]
            self.assign_task(tid, username)
            tasks[tid] = url

        return tasks