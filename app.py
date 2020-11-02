import flask
from flask import Response, request, redirect, jsonify
from taskdb import TaskDB


database = TaskDB("tasks.db")


def create_app():
    database.ensure_tasks_exist()
    app = flask.Flask(__name__, static_url_path="")
    return app


app = create_app()


@app.route("/api/<username>/newbatch/<int:size>", methods=["GET"])
def get_batch(username, size):
    print("Fetching new batch of tasks")
    tasks = database.get_task_batch(username, size)
    return tasks


@app.route("/api/accepttask/<int:taskid>", methods=["GET"])
def accept_task(taskid):
    print(f"Accepting Task {taskid}")
    database.accept_task(taskid)
    return jsonify({"success": True})

@app.route("/api/acceptasks",methods=['POST'])
def accept_tasks():
    if request.method == 'POST':
        taskids = request.get_json()
        for tid in taskids['tids']:
            print(f"Accepting Task {tid}")
            database.accept_task(tid)



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
