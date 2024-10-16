import urllib.parse

DEFAULT_COMMAND_SERVER_BACKLOG = 32
DEFAULT_COMMAND_SERVER_PORT = 4000

def parse_command_server_address(
    command_server_address: str,
    command_server_backlog: int = DEFAULT_COMMAND_SERVER_BACKLOG,
    command_server_authkey: bytes = b"",
) -> tuple:
    """Parse command server address into Listener / Client arguments.

    For example,

        from multiprocessing.connection import Listener, Client

        address1 = "tcp://localhost:4000"
        address2 = "unix:///tmp/cmdserver.sock"

        listener1 = Listener(*parse_command_server_address(address1))
        client1 = Client(*parse_command_server_address(address1))

        listener2 = Listener(*parse_command_server_address(address2))
        client2 = Client(*parse_command_server_address(address2))

    """

    if not isinstance(command_server_address, str):
        raise TypeError("command_server_address is not str")

    parsed_url = urllib.parse.urlparse(command_server_address)

    if parsed_url.scheme == "unix":
        scheme = "unix"
    elif parsed_url.scheme == "tcp":
        scheme = "tcp"
    else:
        raise ValueError(
            "command server address must start with unix:// or tcp://"
        )

    if scheme == "unix":
        path = parsed_url.path
        if not path:
            raise ValueError("invalid unix domain socket path")
        if os.path.exists(path):
            raise ValueError("file exists in the given unix domain socket path")
        address_dict = {
            "address": path,
            "family": "AF_UNIX",
            "backlog": command_server_backlog,
        }
        if command_server_authkey:
            address_dict["authkey"] = command_server_authkey
    elif scheme == "tcp":
        netloc = parsed_url.netloc
        split_netloc = netloc.split(":")
        if len(split_netloc) > 1:
            host_address, host_port = split_netloc[0], int(split_netloc[1])
        else:
            host_address, host_port = split_netloc[0], DEFAULT_COMMAND_SERVER_PORT
        address_dict = {
            "address": (host_address, host_port),
            "family": "AF_INET",
            "backlog": command_server_backlog,
        }
        if command_server_authkey:
            address_dict["authkey"] = command_server_authkey
    else:
        raise RuntimeError("fatal error: listen address scheme is not unix or tcp")

    return address_dict
