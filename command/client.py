import logging

from multiprocessing.connection import Client

from command import (
    DEFAULT_COMMAND_SERVER_BACKLOG,
    parse_command_server_address,
)

_logger = logging.getLogger(__name__)

def send_kill_message_to_command_server(
    command_server_address: str,
    command_server_authkey: bytes = b"",
    kill_command_timeout: int = 5,
):
    try:
        address_dict = parse_command_server_address(
            command_server_address,
            DEFAULT_COMMAND_SERVER_BACKLOG,
            command_server_authkey
        )
        _logger.info("trying to connect to the command server's listener")
        client = Client(
            address_dict["address"],
            address_dict["family"],
            address_dict.get("authkey")
        )
        _logger.info(
            "successfully connected to the command server's listener, "
            "sending 'die'"
        )
        client.send("die")
        _logger.info(
            "waiting for a response from the command server's listener"
        )
        if client.poll(kill_command_timeout):
            response = client.recv()
            if response == "bleh":
                _logger.info(
                    "command server's listener returned 'bleh' (i.e., "
                    "terminated)"
                )
                return True
            else:
                _logger.info(
                    "command server's listener returned '{}' when it was "
                    "requested to kill itself".format(response)
                )
                return False
        else:
            _logger.info(
                "failed to receive response from the command server's "
                "listener, it likely hanged"
            )
            return False
    except (EOFError, ConnectionResetError) as exc:
        _logger.info(
            "failed to read from the command server's listener, it likely "
            "cleaned itself up already"
        )
        return True
    except ConnectionRefusedError as exc:
        _logger.info(
            "failed to connect to the command server's listener, it likely "
            "cleaned itself up already"
        )
        return True
    return False

