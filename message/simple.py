import logging
import socket
import selectors
import time
import struct
import json
from threading import Thread, Event
from queue import Queue, Empty

import message
from command import parse_command_server_address

_logger = logging.getLogger(__name__)

class SelectorKeyData:
    def __init__(self, key_type: str, info=None):
        self.type = key_type
        self.info = info

class ConnectionInfo:
    def __init__(self, connection_id: int, timeref: float):
        self.buffer = bytearray()
        self.state = 0
        self.id = connection_id
        self.content_length = None
        self.conn_start_timeref = timeref
        self.last_data_recv_timeref = timeref
        self.read_iteration = 0

class SimpleMessageServer:
    backlog = 4096
    socket_read_unit = 4096
    content_length_limit = 16777216

    def __init__(
        self,
        message_server_address: str,
        command_server_address: str,
        command_server_authkey: bytes = b""
    ):
        self.message_server_address = message_server_address
        self.command_server_address = command_server_address
        self.command_server_authkey = command_server_authkey
        self.listen_socket = None
        self.selector = None
        self.connection_count = 0
        self.connections = None
        self.payload_queue = None

    def _accept(self, sock: socket.socket, mask: int):
        conn, addr = sock.accept()
        conn.setblocking(False)
        self.connections[self.connection_count+1] = (conn, addr)
        self.selector.register(
            conn,
            selectors.EVENT_READ,
            SelectorKeyData(
                "c",
                ConnectionInfo(self.connection_count+1, time.monotonic())
            )
        )
        self.connection_count += 1

    def _read(self, conn: socket.socket, mask: int, aux: ConnectionInfo):
        data = conn.recv(self.socket_read_unit)
        if data:
            aux.last_data_recv_timeref = time.monotonic()
            aux.read_iteration += 1
            _logger.debug(
                "connection id {} - read iteration = {} ({:.2f} seconds since"
                " connection)".format(
                    aux.id,
                    aux.read_iteration,
                    aux.last_data_recv_timeref - aux.conn_start_timeref
                )
            )
            aux.buffer.extend(data)
        else:
            _logger.debug(
                "connection id {} - no data returned, setting state to "
                "2".format(aux.id)
            )
            aux.state = 2
        if aux.state == 0:
            if len(aux.buffer) >= 4:
                _logger.debug(
                    "connection id {} - reading content_length".format(aux.id)
                )
                content_length = struct.unpack_from(">I", aux.buffer, 0)[0]
                aux.content_length = content_length
                _logger.debug(
                    "connection id {} - content_length = {}".format(
                        aux.id, content_length
                    )
                )
                if content_length > self.content_length_limit:
                    _logger.debug(
                        "connection id {} - content_length went over "
                        "content_length_limit {}, setting state to 2".format(
                            aux.id, self.content_length_limit
                        )
                    )
                    aux.state = 2
                else:
                    aux.state = 1
        if aux.state == 1:
            buflen = len(aux.buffer)
            _logger.debug(
                "connection id {} - current buffer length = {}, "
                "content_length = {}".format(
                    aux.id, buflen, aux.content_length
                )
            )
            if buflen-4 > self.content_length_limit:
                _logger.debug(
                    "connection id {} - data length went over "
                    "content_length_limit {}, setting state to 2".format(
                        aux.id, self.content_length_limit
                    )
                )
                aux.state = 2
            elif buflen-4 > aux.content_length:
                _logger.debug(
                    "connection id {} - data length went over content_length "
                    "{}, setting state to 2".format(
                        aux.id, aux.content_length
                    )
                )
                aux.state = 2
            elif buflen-4 == aux.content_length:
                _logger.debug(
                    "connection id {} - content_length satisfied, reading "
                    "json object".format(aux.id)
                )
                try:
                    obj = json.loads(aux.buffer[4:].decode(encoding="utf-8"))
                    self.payload_queue.put((aux.id, obj))
                    self.selector.unregister(conn)
                    _logger.debug(
                        "connection id {} - json object = {}".format(
                            aux.id, obj
                        )
                    )
                except UnicodeError as exc:
                    _logger.debug(
                        "connection id {} - failed to read json object "
                        "(UnicodeError)".format(aux.id)
                    )
                except json.JSONDecodeError as exc:
                    _logger.debug(
                        "connection id {} - failed to read json object "
                        "(JSONDecodeError)".format(aux.id)
                    )
        if aux.state == 2:
            _logger.debug(
                "connection id {} - unregistering and closing the "
                "connection".format(aux.id)
            )
            self.selector.unregister(conn)
            conn.close()
            _logger.debug(
                "connection id {} - unregistered and closed".format(aux.id)
            )

    def _process_result_queue(self, process_termination_flag: Event):
        while True:
            try:
                conn_id, payload = self.payload_queue.get(block=True, timeout=1)
                # TODO: send to command server
                conn, addr = self.connections.pop(conn_id)
                conn.send(json.dumps(payload).encode())
                conn.close()
            except Empty as exc:
                pass
            if process_termination_flag.is_set():
                break

    def serve(self):
        _logger.info("starting simple message server")
        selector = selectors.DefaultSelector()

        # TODO: implement parse_message_server_address() separately
        address_dict = parse_command_server_address(self.message_server_address)

        if address_dict.get("family") == "AF_UNIX":
            listen_socket = socket.socket(socket.AF_UNIX)
        elif address_dict.get("family") == "AF_INET":
            listen_socket = socket.socket(socket.AF_INET)
        else:
            raise ValueError("address family not AF_UNIX or AF_INET")
        listen_socket.bind(address_dict["address"])
        listen_socket.listen(self.backlog)
        listen_socket.setblocking(False)

        selector.register(
            listen_socket,
            selectors.EVENT_READ,
            SelectorKeyData("l")
        )
        self.listen_socket = listen_socket
        self.selector = selector
        self.connections = {}
        self.payload_queue = Queue()
        process_termination_flag = Event()
        process_termination_flag.clear()
        queue_processor = Thread(
            target=self._process_result_queue,
            args=(process_termination_flag,)
        )
        queue_processor.start()
        try:
            while True:
                events = selector.select()
                for key, mask in events:
                    key_data = key.data
                    if key_data.type == "l":
                        self._accept(key.fileobj, mask)
                    elif key_data.type == "c":
                        self._read(key.fileobj, mask, key_data.info)
                    else:
                        _logger.warning(
                            "warning: key of unknown type '{}' is in the "
                            "selector (fileobj: {})".format(
                                key_data.type,
                                key.fileobj
                            )
                        )
        finally:
            process_termination_flag.set()
            queue_processor.join()

def create_and_start_server(
    message_server_address: str,
    command_server_address: str,
    command_server_authkey: bytes = b""
):
    try:
        s = SimpleMessageServer(
            message_server_address,
            command_server_address,
            command_server_authkey
        )
        s.serve()
    except KeyboardInterrupt as exc:
        _logger.info(
            "simple message server received SIGINT, terminating the server"
        )

