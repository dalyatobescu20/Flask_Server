"""Module for defining the routes of the web server."""
from app import webserver
from flask import request, jsonify
from app.task_runner import Task, logger
import os
import json

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    """
    Example POST endpoints
    """
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        logger.info(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405


@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    """
    Get the response for a given job_id
    """
    logger.info(f"JobID is {job_id}")
    # Check if job_id is valid
    if webserver.tasks_runner.job_id_is_valid(job_id):
        # Get the task associated with the job_id
        task = webserver.tasks_runner.get_task(job_id)
        if task.status == "done":
            # Construct the file path for the result file
            result_file_path = f"results/{job_id}.json"
            # Check if the result file exists
            if not os.path.exists(result_file_path):
                logger.error(f"Result file not found: {result_file_path}")
                return jsonify({"error": "Result file not found"}), 404
            # Attempt to read the result file
            try:
                with open(result_file_path, "r") as file:
                    result_data = json.load(file)
                response_data = {
                    "status": "done",
                    "data": result_data
                }
                return jsonify(response_data)

            except json.JSONDecodeError:
                logger.error("Error decoding JSON from result file")
                return jsonify({"error": "Error decoding JSON from result file"}), 500
            except Exception as e:
                logger.error(f"Error reading result file: {e}")
                return jsonify({"error": f"Error reading result file: {e}"}), 500
        else:
            # Task is still running
            return jsonify({"status": "running"})
    else:
        # Invalid job_id
        return jsonify({"status": "error", "reason": "Invalid job_id"}), 404


@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    """
    Handle the states_mean request
    """
    data = request.json
    logger.info(f"Got request {data}")
    question = data["question"]
    # get job_id
    job_id = f"job_id{webserver.job_counter}"
    # increment job_id
    webserver.job_counter += 1
    # create task
    task = Task(job_id, "running", webserver.data_ingestor.states_mean, question, None)
    # add task to dictionary
    webserver.tasks_runner.add_task(task, job_id)
    # return job_id
    return jsonify({"job_id": job_id})


@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    """
    Handle the state_mean request
    """
    data = request.json
    logger.info(f"Got request {data}")
    question = data["question"]
    state = data["state"]
    # get job_id
    job_id = f"job_id{webserver.job_counter}"
    # increment job_id
    webserver.job_counter += 1
    # create task
    task = Task(job_id, "running", webserver.data_ingestor.state_mean, question, state)
    # add task to dictionary
    webserver.tasks_runner.add_task(task, job_id)
    # return job_id
    return jsonify({"job_id": job_id})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    """
    Handle the best5 request
    """
    data = request.json
    logger.info(f"Got request {data}")
    question = data["question"]
    # get job_id
    job_id = f"job_id{webserver.job_counter}"
    # increment job_id
    webserver.job_counter += 1
    # create task
    task = Task(job_id, "running", webserver.data_ingestor.best5, question, None)
    # add task to dictionary
    webserver.tasks_runner.add_task(task, job_id)
    # return job_id
    return jsonify({"job_id": job_id})


@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    """
    Handle the worst5 request
    """
    data = request.json
    logger.info(f"Got request {data}")
    question = data["question"]
    # get job_id
    job_id = f"job_id{webserver.job_counter}"
    # increment job_id
    webserver.job_counter += 1
    # create task
    task = Task(job_id, "running", webserver.data_ingestor.worst5, question, None)
    # add task to dictionary
    webserver.tasks_runner.add_task(task, job_id)
    # return job_id
    return jsonify({"job_id": job_id})


@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    """
    Handle the global_mean request
    """
    data = request.json
    logger.info(f"Got request {data}")
    question = data["question"]
    # get job_id
    job_id = f"job_id{webserver.job_counter}"
    # increment job_id
    webserver.job_counter += 1
    # create task
    task = Task(job_id, "running", webserver.data_ingestor.global_mean, question, None)
    # add task to dictionary
    webserver.tasks_runner.add_task(task, job_id)
    # return job_id
    return jsonify({"job_id": job_id})


@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    """
    Handle the diff_from_mean request
    """
    data = request.json
    logger.info(f"Got request {data}")
    question = data["question"]
    # get job_id
    job_id = f"job_id{webserver.job_counter}"
    # increment job_id
    webserver.job_counter += 1
    # create task
    task = Task(job_id, "running", webserver.data_ingestor.diff_from_mean, question, None)
    # add task to dictionary
    webserver.tasks_runner.add_task(task, job_id)
    # return job_id
    return jsonify({"job_id": job_id})


@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    """
    Handle the state_diff_from_mean request
    """
    data = request.json
    logger.info(f"Got request {data}")
    question = data["question"]
    state = data["state"]
    # get job_id
    job_id = f"job_id{webserver.job_counter}"
    # increment job_id
    webserver.job_counter += 1
    # create task
    task = Task(job_id, "running", webserver.data_ingestor.state_diff_from_mean, question, state)
    # add task to dictionary
    webserver.tasks_runner.add_task(task, job_id)
    # return job_id
    return jsonify({"job_id": job_id})


@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    """
    Handle the mean_by_category request
    """
    data = request.json
    logger.info(f"Got request {data}")
    question = data["question"]
    # get job_id
    job_id = f"job_id{webserver.job_counter}"
    # increment job_id
    webserver.job_counter += 1
    # create task
    task = Task(job_id, "running", webserver.data_ingestor.mean_by_category, question, None)
    # add task to dictionary
    webserver.tasks_runner.add_task(task, job_id)
    # return job_id
    return jsonify({"job_id": job_id})


@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    """
    Handle the state_mean_by_category request
    """
    data = request.json
    logger.info(f"Got request {data}")
    question = data["question"]
    state = data["state"]
    # get job_id
    job_id = f"job_id{webserver.job_counter}"
    # increment job_id
    webserver.job_counter += 1
    # create task
    task = Task(job_id, "running", webserver.data_ingestor.state_mean_by_category, question, state)
    # add task to dictionary
    webserver.tasks_runner.add_task(task, job_id)
    # return job_id
    return jsonify({"job_id": job_id})


@webserver.route('/api/jobs', methods=['GET'])
def get_jobs():
    """
    Get the status of all jobs
    """
    jobs = []
    for job_id in webserver.tasks_runner.job_id_to_task:
        job = webserver.tasks_runner.job_id_to_task[job_id]
        jobs.append({
            "job_id": job.job_id,
            "status": job.status
        })
    return jsonify({"status": "done", "data": jobs})


@webserver.route('/api/num_jobs', methods=['GET'])
def get_num_jobs():
    """
    Get the number of jobs
    """
    nr_jobs = len(webserver.tasks_runner.job_id_to_task)
    return jsonify({"status": "done", "data": nr_jobs})


@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    """"
    Graceful shutdown of the webserver
    """
    logger.info("Graceful shutdown")
    webserver.tasks_runner.graceful_shutdown()
    return jsonify({
        "message": "Shutdown complete."
    })


# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    """
    Display the defined routes
    """
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg


def get_defined_routes():
    """
    Get the defined routes
    """
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
