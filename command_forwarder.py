#!/usr/bin/env python3
# command_forwarder.py — CommandPort with explicit TCP & UDS FSMs,
# state transition broadcasts, reconnect/keepalive/heartbeat, and de-dup.

import socket
import yaml
import os
import time
import json
import threading
import hashlib
from enum import Enum, auto
from datetime import datetime
from typing import Set, Optional
import argparse

# ----------------------- CFG Parser -----------------------
parser = argparse.ArgumentParser(description="TCP Foorwarder for GCode Execution")
parser.add_argument("--cfg", default="config.yaml", type=str, help="Forwarder config file name. Defaults to config.yaml")

# ----------------- FSM enums -----------------
class TcpState(Enum):
    INIT = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    DISCONNECTED = auto()
    RECONNECTING = auto()
    CLOSED = auto()

class UdsState(Enum):
    INIT = auto()
    BOUND = auto()
    LISTENING = auto()
    SERVING = auto()
    ERROR = auto()
    CLOSED = auto()

# ----------------- CommandPort -----------------
class CommandPort:
    def __init__(self, config_path: str, port_name: str):
        self.config_path = config_path
        self.port_name = port_name.lower()
        self.load_config()

        self.tcp_sock: Optional[socket.socket] = None
        self.unix_sock: Optional[socket.socket] = None
        self.running = False

        self.clients: Set[socket.socket] = set()
        self.clients_lock = threading.Lock()
        self.robot_send_lock = threading.Lock()

        # De-dup (payload hash + ts) to avoid double broadcasts on duplicate TCP events
        self._last_broadcast_hash: Optional[bytes] = None
        self._last_broadcast_ts: float = 0.0
        self._dedup_lock = threading.Lock()

        # Single-thread guards
        self._reader_started = False
        self._hb_thread_started = False

        # Heartbeat activity tracking
        self._last_rx_mono = time.monotonic()

        # FSM states
        self._tcp_state = TcpState.INIT
        self._uds_state = UdsState.INIT

    # ---------------- Config ----------------
    def load_config(self):
        with open(self.config_path, "r") as f:
            cfg = yaml.safe_load(f)

        robot_cfg = cfg["robot"]
        unix_cfg = cfg.get("unix", {})
        timeouts_cfg = cfg.get("timeouts", {})
        ts_cfg = cfg.get("timestamps", {})
        hb_cfg = timeouts_cfg.get("heartbeat", {}) or {}

        self.robot_ip = robot_cfg["ip"]
        port_key = f"{self.port_name}_port"
        if port_key not in robot_cfg:
            raise KeyError(f"Robot config missing key '{port_key}'")
        self.robot_port = robot_cfg[port_key]

        paths = cfg.get("paths", {})
        self.unix_socket_path = paths.get(f"{self.port_name}_socket", "")

        if self.unix_socket_path == "":
            base_path = unix_cfg.get("path", "/tmp")
            if os.path.isdir(base_path):
                self.unix_socket_path = os.path.join(base_path, f"robot_forward_{self.port_name}.sock")
            else:
                root, ext = os.path.splitext(base_path)
                self.unix_socket_path = f"{root}_{self.port_name}{ext}"

        # TCP timeouts / reconnect
        self.sock_timeout = float(timeouts_cfg.get("socket", 3.0))
        self.backoff_min = float(timeouts_cfg.get("reconnect_min", 0.5))
        self.backoff_max = float(timeouts_cfg.get("reconnect_max", 5.0))

        # Optional de-dup window (seconds)
        self.dedup_window_s = float(timeouts_cfg.get("dedup_window_s", 0.30))

        # OS keepalive
        self.keepalive_enable = bool(timeouts_cfg.get("keepalive_enable", True))
        self.keepalive_idle_s = int(timeouts_cfg.get("keepalive_idle_s", 10))
        self.keepalive_intvl_s = int(timeouts_cfg.get("keepalive_intvl_s", 3))
        self.keepalive_cnt = int(timeouts_cfg.get("keepalive_cnt", 3))

        # Application heartbeat
        self.hb_enabled = bool(hb_cfg.get("enabled", False))
        self.hb_interval_s = float(hb_cfg.get("interval_s", 5.0))
        self.hb_payload = str(hb_cfg.get("payload", "\n"))

        # Timestamp injection
        self.ts_enabled = bool(ts_cfg.get("enabled", True))
        self.ts_mode = ts_cfg.get("mode", "utc").lower()  # "utc" or "local"
        self.ts_field = str(ts_cfg.get("field", "_timestamp"))

        print(f"[CFG] Robot {self.robot_ip}:{self.robot_port}")
        print(f"[CFG] UDS: {self.unix_socket_path}")
        print(f"[CFG] TCP timeout: {self.sock_timeout}s, backoff: {self.backoff_min}..{self.backoff_max}s")
        print(f"[CFG] Keepalive: {self.keepalive_enable} idle={self.keepalive_idle_s}s intvl={self.keepalive_intvl_s}s cnt={self.keepalive_cnt}")
        print(f"[CFG] Heartbeat: {self.hb_enabled} every {self.hb_interval_s}s payload={repr(self.hb_payload)}")
        print(f"[CFG] Timestamps: {self.ts_enabled} ({self.ts_mode}) field={self.ts_field}")
        print(f"[CFG] De-dup window: {self.dedup_window_s}s")

    # ---------------- Timestamp helper ----------------
    def _stamp(self, obj: dict):
        if self.ts_enabled and self.ts_field not in obj:
            ts = datetime.now().isoformat() if self.ts_mode == "local" else datetime.utcnow().isoformat() + "Z"
            obj[self.ts_field] = ts
        return obj

    # ---------------- De-dup helper ----------------
    def _should_drop_duplicate(self, payload: bytes) -> bool:
        if self.dedup_window_s <= 0:
            return False
        now = time.monotonic()
        h = hashlib.blake2b(payload, digest_size=16).digest()
        with self._dedup_lock:
            if self._last_broadcast_hash == h and (now - self._last_broadcast_ts) <= self.dedup_window_s:
                return True
            self._last_broadcast_hash = h
            self._last_broadcast_ts = now
            return False

    # ---------------- Broadcast helpers ----------------
    def _broadcast_bytes(self, data: bytes):
        drop = []
        with self.clients_lock:
            for c in list(self.clients):
                try:
                    c.sendall(data)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    drop.append(c)
            for c in drop:
                try: c.close()
                except Exception: pass
                self.clients.discard(c)

    def broadcast_json(self, obj: dict):
        payload_obj = self._stamp(dict(obj))
        try:
            payload = (json.dumps(payload_obj, separators=(",",":")) + "\n").encode()
        except Exception:
            return
        if not self._should_drop_duplicate(payload):
            self._broadcast_bytes(payload)

    def broadcast_raw_line(self, json_text: str):
        payload = (json_text + "\n").encode()
        if not self._should_drop_duplicate(payload):
            self._broadcast_bytes(payload)

    # ---------------- FSM transitions ----------------
    def _set_tcp_state(self, new_state: TcpState, reason: str = "", **meta):
        old = self._tcp_state
        if new_state is old:
            return
        self._tcp_state = new_state
        self.broadcast_json({
            "status":"INFO",
            "source":"tcp",
            "event":"tcp_state",
            "from": old.name,
            "to": new_state.name,
            "reason": reason,
            **meta
        })

    def _set_uds_state(self, new_state: UdsState, reason: str = "", **meta):
        old = self._uds_state
        if new_state is old:
            return
        self._uds_state = new_state
        self.broadcast_json({
            "status":"INFO",
            "source":"uds",
            "event":"uds_state",
            "from": old.name,
            "to": new_state.name,
            "reason": reason,
            **meta
        })

    # ---------------- TCP Keepalive ----------------
    def _apply_keepalive(self, s: socket.socket):
        if not self.keepalive_enable:
            return
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, 'TCP_KEEPIDLE'):
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, self.keepalive_idle_s)
            if hasattr(socket, 'TCP_KEEPINTVL'):
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, self.keepalive_intvl_s)
            if hasattr(socket, 'TCP_KEEPCNT'):
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, self.keepalive_cnt)
            self.broadcast_json({"status":"INFO","source":"tcp","message":"keepalive_on",
                                 "idle_s":self.keepalive_idle_s,"intvl_s":self.keepalive_intvl_s,"count":self.keepalive_cnt})
        except Exception as e:
            self.broadcast_json({"status":"ERROR","source":"tcp","error":"keepalive_config_failed","detail":str(e)})

    # ---------------- TCP connect/close with FSM ----------------
    def connect_once(self) -> bool:
        self._set_tcp_state(TcpState.CONNECTING, reason="connect_once")
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.sock_timeout)
            s.connect((self.robot_ip, self.robot_port))
            self._apply_keepalive(s)
            self.tcp_sock = s
            self._last_rx_mono = time.monotonic()
            self._set_tcp_state(TcpState.CONNECTED, reason="connect_success",
                                ip=self.robot_ip, port=self.robot_port)
            self.broadcast_json({"status":"INFO","source":"tcp","message":"connected"})
            print("[TCP] Connected")
            return True
        except Exception as e:
            self._set_tcp_state(TcpState.DISCONNECTED, reason="connect_failed", detail=str(e))
            self.broadcast_json({"status":"ERROR","source":"tcp","error":"connect_failed","detail":str(e)})
            print(f"[TCP] Connection failed: {e}")   
            return False

    def reconnect_loop(self):
        self._set_tcp_state(TcpState.RECONNECTING, reason="start_reconnect")
        backoff = self.backoff_min
        attempt = 0
        while self.running and self.tcp_sock is None:
            attempt += 1
            self.broadcast_json({"status":"INFO","source":"tcp","message":"reconnecting",
                                 "attempt":attempt,"backoff_s":round(backoff,3)})
            ok = self.connect_once()
            if ok:
                self.broadcast_json({"status":"INFO","source":"tcp","message":"reconnected","attempt":attempt})
                return
            time.sleep(backoff)
            backoff = min(backoff * 2.0, self.backoff_max)

    def ensure_robot_connected(self):
        if self.tcp_sock is not None:
            return
        if not self.connect_once():
            self.reconnect_loop()

    def close_robot(self, reason: str = "closed"):
        if self.tcp_sock:
            try:
                self.tcp_sock.close()
            except Exception:
                #print(f"[TCP] Can't close:{e}") #fix this
                print(f"[TCP] close robot error, interrupted")
                pass
            self.tcp_sock = None
            self._set_tcp_state(TcpState.DISCONNECTED, reason=reason)
            self.broadcast_json({"status":"ERROR","source":"tcp","error":"disconnected","detail":reason})


    # ---------------- TCP send ----------------
    def tcp_send_only(self, msg: str) -> bool:
        """
        Non-blocking pattern: send to TCP (caller includes newline if needed).
        Reader thread handles replies and broadcasts them.
        """
        self.ensure_robot_connected()
        if self.tcp_sock is None:
            return False
        try:
            with self.robot_send_lock:
                self.tcp_sock.sendall(msg.encode())
            return True
        except (socket.timeout, OSError, socket.error) as e:
            self.broadcast_json({"status":"ERROR","source":"tcp","error":"send_failed","detail":str(e)})
            self.close_robot(reason="send_error")
            return False

    # ---------------- TCP Reader (passive receive) ----------------
    def tcp_reader_loop(self):
        if self._reader_started:
            return
        self._reader_started = True

        decoder = json.JSONDecoder()
        buf = ""
        while self.running:
            self.ensure_robot_connected()
            if self.tcp_sock is None:
                time.sleep(0.2)
                continue

            try:
                chunk = self.tcp_sock.recv(1024)
                if not chunk:
                    self.close_robot(reason="remote_closed")
                    continue

                self._last_rx_mono = time.monotonic()
                buf += chunk.decode(errors="replace")

                # Extract complete JSON values (robust to concatenated replies)
                while True:
                    stripped = buf.lstrip()
                    if not stripped:
                        buf = ""
                        break
                    try:
                        obj, idx = decoder.raw_decode(stripped)
                        leading_ws = len(buf) - len(stripped)
                        end_idx = leading_ws + idx
                        buf = buf[end_idx:]
                    except json.JSONDecodeError:
                        break

                    # Normalize and timestamp
                    data = obj if isinstance(obj, dict) else {"data": obj}
                    self._stamp(data)

                    # Broadcast as a compact JSON line (with de-dup)
                    payload = (json.dumps(data, separators=(",",":")) + "\n").encode()
                    if not self._should_drop_duplicate(payload):
                        self._broadcast_bytes(payload)

            except socket.timeout:
                # idle; heartbeat thread handles probing if enabled
                continue
            except (OSError, socket.error) as e:
                self.broadcast_json({"status":"ERROR","source":"tcp","error":"recv_failed","detail":str(e)})
                self.close_robot(reason="recv_error")

    # ---------------- Heartbeat thread ----------------
    def heartbeat_loop(self):
        if not self.hb_enabled:
            return
        while self.running:
            time.sleep(self.hb_interval_s if self.hb_interval_s > 0 else 1.0)
            if not self.running:
                break
            if self.tcp_sock is None:
                # help reconnect faster
                self.ensure_robot_connected()
                continue
            idle = time.monotonic() - self._last_rx_mono
            if idle >= self.hb_interval_s:
                ok = self.tcp_send_only(self.hb_payload)
                if not ok:
                    # send failure already broadcast; ensure reconnect
                    self.ensure_robot_connected()

    # ---------------- UDS Server (with FSM) ----------------
    def start_unix_server(self):
    # INIT -> BOUND (keep your FSM calls here if you have them)
        if os.path.exists(self.unix_socket_path):
            try:
                os.remove(self.unix_socket_path)
            except Exception as e:
                print(f"[UNIX] unlink failed: {e}")
                self.broadcast_json({"status":"ERROR","source":"uds","error":"unlink_failed","detail":str(e)})

        self.unix_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.unix_sock.bind(self.unix_socket_path)
        except Exception as e:
            print(f"[UNIX] bind failed: {e}")
            self.broadcast_json({"status":"ERROR","source":"uds","error":"bind_failed","detail":str(e)})
            raise

        try:
            os.chmod(self.unix_socket_path, 0o666)
        except Exception as e:
            print(f"[UNIX] chmod failed: {e}")
            self.broadcast_json({"status":"ERROR","source":"uds","error":"chmod_failed","detail":str(e)})

        try:
            self.unix_sock.listen(16)
        except Exception as e:
            print(f"[UNIX] listen failed: {e}")
            self.broadcast_json({"status":"ERROR","source":"uds","error":"listen_failed","detail":str(e)})
            raise

        print(f"[UNIX] Listening on {self.unix_socket_path}")
        self.broadcast_json({"status":"INFO","source":"uds","message":"listening","path":self.unix_socket_path})
        threading.Thread(target=self._accept_loop, name="uds_accept", daemon=True).start()


    def _accept_loop(self):
        while self.running:
            try:
                conn, _ = self.unix_sock.accept()
                with self.clients_lock:
                    self.clients.add(conn)
                print(f"[UNIX] Client connected ({len(self.clients)} total)")
                self.broadcast_json({"status":"INFO","source":"uds","message":"client_connected","clients":len(self.clients)})
                threading.Thread(target=self._client_loop, args=(conn,), name="uds_client", daemon=True).start()
            except OSError as e:
                if self.running:
                    print(f"[UNIX] accept error: {e}")
                    self.broadcast_json({"status":"ERROR","source":"uds","error":"accept_error","detail":str(e)})
                time.sleep(0.2)


    def _client_loop(self, conn: socket.socket):
        """
        Read commands from this UNIX client. Each command is delimited by '\n' or '\0'.
        For each command, send to TCP immediately (non-blocking). Replies are read by
        the TCP reader thread and broadcast to all clients.
        """
        buf = b""
        try:
            while self.running:
                data = conn.recv(4096)
                if not data:
                    break
                buf += data

                while True:
                    nl = buf.find(b"\n")
                    nul = buf.find(b"\x00")
                    cut = -1
                    if nl != -1 and nul != -1:
                        cut = min(nl, nul)
                    elif nl != -1:
                        cut = nl
                    elif nul != -1:
                        cut = nul
                    if cut == -1:
                        break

                    line = buf[:cut].decode(errors="replace")
                    buf = buf[cut+1:]
                    line_stripped = line.strip()
                    if not line_stripped:
                        continue

                    print(f"[UNIX] Received: {line_stripped}")
                    ok = self.tcp_send_only(line if line.endswith("\n") else (line + "\n"))
                    if not ok:
                        print("[UNIX] TCP send failed for client command")
                        err = {"status":"ERROR","source":"proxy","error":"send_failed","detail":"cannot_send_to_tcp"}
                        try:
                            conn.sendall((json.dumps(err, separators=(",",":")) + "\n").encode())
                        except Exception:
                            pass
                        # broadcast so all listeners are aware
                        self.broadcast_json(err)

        except Exception as e:
            print(f"[UNIX] client loop error: {e}")
            self.broadcast_json({"status":"ERROR","source":"uds","error":"client_loop_error","detail":str(e)})
        finally:
            with self.clients_lock:
                self.clients.discard(conn)
            try:
                conn.close()
            except Exception:
                pass
            print("[UNIX] Client removed")
            self.broadcast_json({"status":"INFO","source":"uds","message":"client_disconnected","clients":len(self.clients)})


    # ---------------- Main ----------------
    def run(self):
        self.running = True
        self.start_unix_server()

        if not self._reader_started:
            threading.Thread(target=self.tcp_reader_loop, name="tcp_reader", daemon=True).start()
        if self.hb_enabled and not self._hb_thread_started:
            self._hb_thread_started = True
            threading.Thread(target=self.heartbeat_loop, name="tcp_heartbeat", daemon=True).start()

        self.broadcast_json({"status":"INFO","source":"sys","message":"running"})
        try:
            while self.running:
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.broadcast_json({"status":"INFO","source":"sys","message":"interrupt"})
        finally:
            self.shutdown()

    def shutdown(self):
        self.running = False
        self._set_uds_state(UdsState.CLOSED, reason="shutdown")
        self._set_tcp_state(TcpState.CLOSED, reason="shutdown")
        self.close_robot(reason="shutdown")
        with self.clients_lock:
            for c in list(self.clients):
                try: c.close()
                except Exception: pass
            self.clients.clear()
        try:
            if self.unix_sock:
                self.unix_sock.close()
        except Exception:
            pass
        if os.path.exists(self.unix_socket_path):
            try: os.remove(self.unix_socket_path)
            except Exception as e:
                self.broadcast_json({"status":"ERROR","source":"uds","error":"unlink_failed","detail":str(e)})
        self.broadcast_json({"status":"INFO","source":"sys","message":"stopped"})


if __name__ == "__main__":
    args = parser.parse_args()
    cp = CommandPort(args.cfg, "command")
    cp.run()
