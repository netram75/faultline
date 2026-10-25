"""Check that a cluster answers: find the leader, SET a key, GET it back.

    python docker/smoke.py 6401 6402 6403

Standard library only, so it runs anywhere Python does, with nothing
installed. CI uses it on the Docker cluster. It speaks just enough of the
Redis protocol for SET and GET.
"""

import socket
import sys
import time


def call(port: int, *args: str) -> bytes:
    """Send one command, return the first line of the reply (and the value,
    for a bulk string)."""
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        parts.append(f"${len(arg)}\r\n{arg}\r\n".encode())
    with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
        s.sendall(b"".join(parts))
        reply = b""
        while not reply.endswith(b"\r\n") or (reply.startswith(b"$") and reply.count(b"\r\n") < 2):
            chunk = s.recv(4096)
            if not chunk:
                break
            reply += chunk
        if reply.startswith(b"$") and not reply.startswith(b"$-1"):
            return reply.split(b"\r\n")[1]
        return reply.rstrip(b"\r\n")


def main(ports: list[int], deadline: float = 60) -> int:
    end = time.monotonic() + deadline
    last = b""
    while time.monotonic() < end:
        for port in ports:
            try:
                last = call(port, "SET", "smoke", "ok")
                if last == b"+OK" and call(port, "GET", "smoke") == b"ok":
                    print(f"leader on port {port}: SET and GET work")
                    return 0
            except OSError as e:
                last = str(e).encode()  # not up yet
        time.sleep(0.5)
    print(f"no working leader after {deadline} s, last answer: {last!r}")
    return 1


if __name__ == "__main__":
    sys.exit(main([int(port) for port in sys.argv[1:]]))
