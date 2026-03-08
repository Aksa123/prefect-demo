from prefect.tasks import Task
from prefect.client.schemas.objects import TaskRun
from prefect.client.schemas.objects import State
from prefect.logging import get_run_logger
from loggers import logger
from settings import db_source_conn, db_destination_conn


def failure_handler(task: Task, task_run: TaskRun, state: State):
    logger_pref = get_run_logger()
    msg = f"""Task <<{task.name}>> failed during task run on {task_run.start_time.strftime('%Y-%m-%d %H:%M:%S')}. Message: {state.message} """
    logger_pref.error(msg)
    logger.error(msg, exc_info=False)
    db_source_conn.close()
    db_destination_conn.close()
    logger.warning('Databases closed upon failure.')
    # Send a Slack message / email

def completion_handler(task: Task, task_run: TaskRun, state: State):
    msg = f"Flow <<{task.name}>> completed!."
    logger.info(msg)