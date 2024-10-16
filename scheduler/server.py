import logging
import traceback

from pytz import utc
from apscheduler.schedulers.background import BackgroundScheduler

from command import DEFAULT_COMMAND_SERVER_BACKLOG
from command.server import start_command_server

_logger = logging.getLogger(__name__)

class SchedulerDefintion:
    def __init__(self, scheduler_type, jobstores, executors, job_defaults):
        self.scheduler_type = scheduler_type
        self.jobstores = jobstores
        self.executors = executors
        self.job_defaults = job_defaults

    def create_scheduler(self):
        if self.scheduler_type == "background":
            targetcls = BackgroundScheduler
        else:
            raise ValueError("scheduler_type is not 'background'")
        return targetcls(
            jobstores=self.jobstores,
            executors=self.executors,
            job_defaults=self.job_defaults,
            timezone=utc
        )

def spawn_scheduler_server(
    scheduler_definition: SchedulerDefintion,
    command_server_address: str,
    command_server_backlog: int = DEFAULT_COMMAND_SERVER_BACKLOG,
    command_server_authkey: bytes = b""
):
    try:
        scheduler = scheduler_definition.create_scheduler()
        #scheduler.add_job(job, trigger="interval", seconds=5)
        scheduler.start()

        start_command_server(
            command_server_address,
            command_server_backlog,
            command_server_authkey
        )
    except KeyboardInterrupt as exc:
        _logger.info(
            "scheduler server received SIGINT, terminating the scheduler "
            "server"
        )
    except Exception as exc:
        traceback.print_exc()

