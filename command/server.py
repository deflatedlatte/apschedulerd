import logging
from multiprocessing import Pipe
from multiprocessing.connection import Listener, Client, Connection, wait
from threading import Thread, Lock, Event

from command import (
    DEFAULT_COMMAND_SERVER_BACKLOG,
    parse_command_server_address,
)
from command.client import send_kill_message_to_command_server

_logger = logging.getLogger(__name__)

class ListenerInitializationFailedError(Exception):
    pass

def command_server_accept_loop(
    command_server_address: str,
    command_server_backlog: int,
    command_server_authkey: bytes,
    pending_connections: list,
    pending_connections_lock: Lock,
    pending_connections_event: Event,
    event_pipe: Connection,
):
    """Keep listening for incoming connections to the command server.

    This function can only be spawned as a Thread, not a Process.
    """

    try:
        listener = Listener(
            **parse_command_server_address(
                command_server_address,
                command_server_backlog,
                command_server_authkey
            )
        )
        while True:
            conn = listener.accept()
            init_message = conn.recv()
            if init_message == "hi":
                conn.send("ok")
            elif init_message == "die":
                conn.send("bleh")
                conn.close()
                break
            try:
                pending_connections_lock.acquire()
                pending_connections.append(conn)
                pending_connections_event.set()
            finally:
                pending_connections_lock.release()
    except Exception as exc:
        raise ListenerInitializationFailedError from exc
    finally:
        event_pipe.close()
        listener.close()

def spawn_command_server_listener(
    command_server_address: str,
    command_server_backlog: int,
    command_server_authkey: bytes,
    pending_connections: list,
    pending_connections_lock: Lock,
    pending_connections_event: Event,
):
    pipe1, pipe2 = Pipe()
    listener_thread = Thread(
        target=command_server_accept_loop,
        args=(
            command_server_address,
            command_server_backlog,
            command_server_authkey,
            pending_connections,
            pending_connections_lock,
            pending_connections_event,
            pipe1,
        )
    )
    listener_thread.start()
    return listener_thread, pipe2

def start_command_server(
    command_server_address: str,
    command_server_backlog: int = DEFAULT_COMMAND_SERVER_BACKLOG,
    command_server_authkey: bytes = b"",
):
    pending_connections = []
    pending_connections_lock = Lock()
    pending_connections_event = Event()
    listener_thread, event_pipe = spawn_command_server_listener(
        command_server_address,
        command_server_backlog,
        command_server_authkey,
        pending_connections,
        pending_connections_lock,
        pending_connections_event,
    )
    active_connections = [event_pipe]
    try:
        while True:
            if not active_connections:
                # This should prevent accidentally using CPU resources when
                # active_connections becomes empty for some unexpected reason.
                pending_connections_event.wait()

            if pending_connections_event.is_set():
                pending_connections_lock.acquire()
                _logger.info(
                    "{} new pending connections will now become active".format(
                        len(pending_connections)
                    )
                )
                for conn in pending_connections:
                    active_connections.append(conn)
                pending_connections.clear()
                pending_connections_event.clear()
                pending_connections_lock.release()

            ready_connections = wait(active_connections)
            for conn in ready_connections:
                try:
                    val = conn.recv()
                    retval = 0
                except (EOFError, ConnectionResetError) as exc:
                    if conn == event_pipe:
                        # event_pipe is closed, something happened to the listener
                        # TODO: maybe restart the listener then?
                        return -1
                    conn.close()
                    active_connections.remove(conn)
                except Exception as exc:
                    if conn == event_pipe:
                        return -1
                    conn.close()
                    active_connections.remove(conn)
    finally:
        send_kill_message_to_command_server(
            command_server_address,
            command_server_authkey
        )
        listener_thread.join()

