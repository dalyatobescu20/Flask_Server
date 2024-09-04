"""Module for defining and managing task runners."""
import json
import multiprocessing
import os
from queue import Queue, Empty
from threading import Thread, Event, Semaphore
import pandas as pd
import logging.handlers

logger = logging.getLogger(__name__)


def get_thread_count():
    num_threads = int(os.getenv('TP_NUM_OF_THREADS', multiprocessing.cpu_count()))
    return min(num_threads, multiprocessing.cpu_count())


class ThreadPool:
    def __init__(self):
        self.num_threads = get_thread_count()
        self.task_runners = []
        self.job_queue = Queue()
        self.shutdown_event = Event()
        self.job_id_to_task = {}
        self.semaphore = Semaphore(0)
        self.create_task_runners()

    def create_task_runners(self):
        """
        Create and start the task runners.
        """
        for _ in range(self.num_threads):
            task_runner = TaskRunner()
            task_runner.set_job_queue(self.job_queue)
            task_runner.set_shutdown_event(self.shutdown_event)
            task_runner.set_dictionary(self.job_id_to_task)
            task_runner.set_semaphore(self.semaphore)
            thread = Thread(target=task_runner.run)
            self.task_runners.append(thread)
            thread.start()

    def add_task(self, job, job_id):
        """
        Add a task to the job queue.
        """
        if not self.shutdown_event.is_set():
            self.job_queue.put((job, job_id))
            self.job_id_to_task[job_id] = job
            self.semaphore.release()
        else:
            logger.error("Cannot add tasks while shutting down.")
            raise RuntimeError("ThreadPool is shutting down.")

    def graceful_shutdown(self):
        """
        Gracefully shutdown the ThreadPool.
        """
        self.shutdown_event.set()
        for _ in range(self.num_threads):
            self.job_queue.put(None)
        for thread in self.task_runners:
            self.semaphore.release()
            thread.join()
        logger.info("All threads have been stopped.")
        logger.info("Shutdown complete.")

    def job_id_is_valid(self, job_id):
        """
        Check if a job_id is valid.
        """
        return job_id in self.job_id_to_task

    def get_task(self, job_id):
        """
        Get the task associated with a job_id.
        """
        return self.job_id_to_task[job_id]


class TaskRunner(Thread):
    def __init__(self):
        """
        Initialize a TaskRunner object.
        """
        super().__init__()
        self.semaphore = None
        self.dictionary = None
        self.job_queue = None
        self.shutdown_event = None

    def set_job_queue(self, job_queue):
        self.job_queue = job_queue

    def set_shutdown_event(self, shutdown_event):
        """
        Set the shutdown event for the TaskRunner.
        """
        self.shutdown_event = shutdown_event

    def set_dictionary(self, dictionary):
        """
        Set the dictionary for the TaskRunner.
        """
        self.dictionary = dictionary

    def set_semaphore(self, semaphore):
        """
        Set the semaphore for the TaskRunner.
        """
        self.semaphore = semaphore

    def run(self):
        """
        Run the TaskRunner.
        """
        if self.shutdown_event.is_set():
            return
        while True:
            if self.shutdown_event.is_set():
                break
            self.semaphore.acquire()
            job, job_id = self.job_queue.get()
            try:
                result = job.do_task()
                status = write_to_file(job_id, result)
                if status == "Done":
                    job.change_task_status("done")
            finally:
                self.job_queue.task_done()


def write_to_file(job_id, result):
    """
    Write the result to a file.
    """
    folder_name = "results"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    file_path = os.path.join(folder_name, f"{job_id}.json")

    if isinstance(result, pd.Series):
        result_dict = result.to_dict()
    elif isinstance(result, dict):
        result_dict = result
    else:
        logger.error("Unsupported result type. Expected Pandas Series or dictionary.")
        return None

    with open(file_path, "w") as file:
        try:
            json.dump(result_dict, file)
        except Exception as e:
            logger.error(f"Error writing to file: {e}", exc_info=True)
            return None

    return "Done"


def convert_to_serializable(obj):
    """
    Convert an object to a serializable format.
    """
    if isinstance(obj, pd.Series):
        return obj.to_dict()
    elif isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_serializable(item) for item in obj]
    elif isinstance(obj, (int, float, str, bool)) or obj is None:
        return obj
    else:
        return str(obj)


def flatten_nested_dict(nested_dict):
    """
    Flatten a nested dictionary.
    """
    flat_dict = {}
    for state, inner_dict in nested_dict.items():
        for key, value in inner_dict.items():
            if state == key:
                flat_dict[state] = value
    return flat_dict


class Task:
    """Represents a task to be executed by a TaskRunner."""

    def __init__(self, job_id, status, function_for_task, question, state):
        self.job_id = job_id
        self.status = status
        self.function_for_task = function_for_task
        self.question = question
        self.state = state

    def change_task_status(self, status):
        """
        Change the status of the task.

        Args:
            status (str): The new status of the task.
        """
        self.status = status

    def do_task(self):
        """
        Execute the assigned task and return the result.

        Returns:
            Any: The result of executing the task.
        """
        if self.state is None:
            result = self.function_for_task(self.question)
        else:
            result = self.function_for_task(self.state, self.question)
        return result
