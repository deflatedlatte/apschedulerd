import time
import os
import os.path
import stat
import random
import logging
import traceback
from multiprocessing import Process, Queue, Pipe
from multiprocessing.connection import Listener, Client, Connection, wait
from threading import Thread, Lock, Event

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

import message
import message.simple
from scheduler.server import SchedulerDefintion, spawn_scheduler_server

logging.basicConfig(level=logging.INFO)

_logger = logging.getLogger(__name__)

class WaitingProcess:
    def __init__(self, process: Process):
        self.process = process

    def fileno(self):
        return self.process.sentinel

def main():
    _logger.info("starting main()")
    try:
        running_processes = []

        # TODO: make this configurable
        scheduler_definition = SchedulerDefintion(
            scheduler_type="background",
            jobstores = {
                'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
            },
            executors = {
                'default': ProcessPoolExecutor(5)
            },
            job_defaults = {
                'coalesce': False,
                'max_instances': 3
            },
        )

        # spawn scheduler server
        _logger.info("spawning a scheduler server")
        p_sched = Process(
            target=spawn_scheduler_server,
            name="scheduler_server",
            args=(
                scheduler_definition,
                "tcp://localhost:4000",
            )
        )
        p_sched.start()
        running_processes.append(WaitingProcess(p_sched))

        # spawn message servers
        _logger.info("spawning message servers")
        p = Process(
            target=message.simple.create_and_start_server,
            name="message_server",
            args=(
                "tcp://localhost:3000",
                "tcp://localhost:4000"
            )
        )
        p.start()
        running_processes.append(WaitingProcess(p))

        while True:
            ready = wait(running_processes)
            for p in ready:
                _logger.info("{} terminated".format(p.process.name))
                running_processes.remove(p)

    except KeyboardInterrupt as exc:
        _logger.info(
            "main process received SIGINT, terminating the scheduler server"
        )

if __name__ == "__main__":
    main()
