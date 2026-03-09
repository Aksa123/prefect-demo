from prefect.tasks import Task
from prefect.client.schemas.objects import TaskRun
from prefect.client.schemas.objects import State
from prefect.logging import get_run_logger
from code.loggers import logger
from code.utils import close_databases



def failure_handler(task: Task, task_run: TaskRun, state: State):
    msg = f"""Task <<{task.name}>> failed during task run on {task_run.start_time.strftime('%Y-%m-%d %H:%M:%S')}. Message: {state.message} """
    logger.error(msg, exc_info=False)
    close_databases()
    logger.warning('Databases closed upon failure.')
    # Send a Slack message / email


def completion_handler(task: Task, task_run: TaskRun, state: State):
    msg = f"Flow <<{task.name}>> completed!."
    logger.info(msg)