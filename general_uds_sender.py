#!/usr/bin/env python3
"""
send_uds_raw.py
Read JSON from stdin, send command to a uds and print onl;y the raw socket reply. Exit immediately on first received bytes.
Todo:
-use echo for nodered!
-use gpt to make it pretty
-fix potential error with uds missing root

stdin JSON example:
{
  "uds": "/tmp/gcode_ctrl.sock",
  "command": "M2",
  "append_newline": true,
  "shutdown_write": true,
  "connect_timeout_s": 2.0,
  "read_timeout_s": 5.0,
  "encoding": "utf-8"
}
"""

import sys, json, socket, select

def main():
    try:
        cfg = json.loads(sys.stdin.read())
    except Exception as e:
        print(f'{{"status":"FAULT","message":"Invalid JSON: {e}"}}')
        sys.exit(1)

    uds = cfg.get("uds")
    cmd = cfg.get("command")
    if not uds or cmd is None:
        print('{"status":"FAULT","message":"Missing uds or command"}')
        sys.exit(1)

    append_newline = bool(cfg.get("append_newline", True))
    shutdown_write = bool(cfg.get("shutdown_write", False))
    encoding = cfg.get("encoding", "utf-8")
    connect_timeout = float(cfg.get("connect_timeout_s", 2.0))
    read_timeout = float(cfg.get("read_timeout_s", 5.0))

    # prepare bytes
    if append_newline and not str(cmd).endswith("\n"):
        cmd = str(cmd) + "\n"
    try:
        outbound = str(cmd).encode(encoding)
    except Exception as e:
        print(f'{{"status":"FAULT","message":"Encode error: {e}"}}')
        sys.exit(1)
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(connect_timeout)
        s.connect(uds)
        s.setblocking(False)
        #s.send()
        s.sendall(outbound)
        if shutdown_write:
            try:
                s.shutdown(socket.SHUT_WR)
            except Exception:
                pass
    except Exception as e:
        print(f'{{"status":"FAULT","message":"Socket send error: {e}"}}')
        try:
            s.close()
        except Exception:
            pass
        sys.exit(1)
    try:
        rlist, _, _ = select.select([s], [], [], read_timeout)
        if not rlist:
            s.close()
            return
        try:
            data = s.recv(65536)
        except BlockingIOError:
            data = b""

        if data:
            try:
                sys.stdout.write(data.decode(encoding, errors="replace"))
            except Exception:
                sys.stdout.buffer.write(data)
            sys.stdout.flush()
    except Exception as e:
        print(f'{{"status":"FAULT","message":"Socket recv error: {e}"}}')
        sys.exit(1)
    finally:
        try:
            s.close()
        except Exception:
            pass
if __name__ == "__main__":
    main()