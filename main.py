#!/usr/bin/env python3
# main_handler.py — handler with FSM, JSON routing, RUN_FILE streaming,
# resilient UDS, device→PIN translation, and async control-event broadcast

import asyncio, os, sys, re, yaml, time, json, contextlib
from enum import Enum, auto
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple, Set

# ----------------------- FSM -----------------------
class ProgramState(Enum):
    IDLE = auto()
    RUNNING_FILE = auto()
    PAUSED = auto()
    ABORTED = auto()

# ----------------------- Routing sets -----------------------
COMMAND_GCODES = {"G1", "G10", "G28", "G60", "G29", "G920"}           # → command socket
STATUS_MCODES = {"M123"} # -> command mcodes
COMMAND_MCODES = {"M17", "M18", "M203", "M204"}        # → command socket
CONTROL_MCODES = {"M24", "M25", "M2", "M100", "M101"}  # → control socket

# ----------------------- ABB Translation -----------------------

ABB_HW_CODE = {
    "M24": "pl!",
    "M25": "pz!",
    "M2": "emr",
    "M100": "rss",
    "M101": "rsp",
    "M123": "snd",
    "G1": "go!",
    "G28": "G1 X300 Y-450 Z700"
}

ABB_PARAM_CODE = {
    "F": "spd",
    "A": "acc",
    "D": "dac",
    "J": "jrk",
    "X": "xtg",
    "Y": "ytg",
    "Z": "ztg"
}

# ----------------------- Utilities -----------------------
def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

def j_ok(**extra) -> str:
    out = {"status":"OKAY","timestamp":now_iso()}
    out.update(extra)
    return json.dumps(out, separators=(",",":"))

def j_err(msg, **extra) -> str:
    out = {"status":"ERROR","message":str(msg),"timestamp":now_iso()}
    out.update(extra)
    return json.dumps(out, separators=(",",":"))

def strip_comment(line: str, cfg: dict) -> str:
    if not cfg.get("runfile", {}).get("strip_inline_comments", True):
        return line
    line = re.split(r";", line, maxsplit=1)[0]
    line = re.sub(r"\([^)]*\)", "", line)
    return line.strip()

def is_blank_or_comment(line: str, cfg: dict) -> bool:
    l = line.strip()
    if l == "":
        return True if cfg.get("runfile", {}).get("skip_blank_lines", True) else False
    if cfg.get("runfile", {}).get("allow_inline_comments", True):
        if l.startswith(";") or l.startswith("#"):
            return True
    return False

def head_word(cmd: str) -> str:
    t = cmd.strip().split()
    if not t: return ""
    hw = t[0].upper()
    m = re.match(r"([GMgm])0*([0-9]+)", hw)
    if m: return f"{m.group(1).upper()}{int(m.group(2))}"
    return hw

# G4 S2.5 -> wait 2.5 s
# G4 P500 -> wait 500 ms
def parse_g4_params(cmd: str) -> Optional[float]:
    m = re.match(r"^\s*[Gg]0*4\b([^#;()]*)", cmd)
    if not m: return None
    tail = m.group(1)
    s_match = re.search(r"[Ss]\s*([0-9]*\.?[0-9]+)", tail)
    p_match = re.search(r"[Pp]\s*([0-9]*\.?[0-9]+)", tail)
    if s_match: return float(s_match.group(1))
    if p_match: return float(p_match.group(1)) / 1000.0
    bare = re.search(r"([0-9]*\.?[0-9]+)", tail)
    if bare: return float(bare.group(1)) / 1000.0
    return None

def parse_json_response(s: str) -> dict:
    txt = s.strip().lstrip('\x00')
    try:
        return json.loads(txt)
    except Exception:
        up = txt.upper()
        if up.startswith("OKAY"):
            return {"status":"OKAY","message":txt,"timestamp":now_iso()}
        if up.startswith("FAULT"):
            return {"status":"FAULT","message":txt,"timestamp":now_iso()}
        if up.startswith("ERROR"):
            return {"status":"ERROR","message":txt,"timestamp":now_iso()}
        return {"status":"ERROR","message":"Non-JSON downstream reply","raw":s,"timestamp":now_iso()}
    
def translate_abb_command(cmd: str) -> list[str]:
    hw = cmd.split()[0].upper()

    if hw not in ABB_HW_CODE:
        print("Not a valid ABB command")
        return []
    
    if hw in CONTROL_MCODES:
        return [f"{ABB_HW_CODE[cmd]} 000"]

    if hw == "G28":
        params = " " + " ".join(cmd.split()[1:]) if len(cmd.split()) > 1 else ""
        cmd = ABB_HW_CODE[hw] + params
        hw = cmd.split()[0].upper()

    abb_commands = []
    for param in cmd.split()[1:]:
        letter = param[0].upper()
        nums = param[1:]
        abb_commands.append(f"{ABB_PARAM_CODE[letter]} {nums}")

    abb_commands.append(f"{ABB_HW_CODE[hw]} 000")
    return abb_commands

# ----------------------- Device translation -----------------------
def _expect(tokens: List[str], i: int) -> Optional[str]:
    return tokens[i] if i < len(tokens) else None

def translate_device_command(cfg_devices: Dict[str, Any], cmd: str) -> Tuple[Optional[str], Optional[Dict[str,Any]]]:
    tokens = cmd.strip().split()
    if not tokens:
        return None, {"status":"ERROR","message":"Empty device command","timestamp":now_iso()}
    head = tokens[0].upper()

    def onoff(tok: Optional[str]) -> Optional[str]:
        if tok is None: return None
        u = tok.upper()
        return u if u in ("ON","OFF") else None

    try:
        if head == "SUCTION":
            state = onoff(_expect(tokens, 1))
            if state is None:
                return None, {"status":"ERROR","message":"SUCTION requires ON|OFF","timestamp":now_iso(),"raw":cmd}
            pin = cfg_devices.get("SUCTION", {}).get("pin")
            if pin is None:
                return None, {"status":"ERROR","message":"SUCTION pin not configured","timestamp":now_iso()}
            return f"PIN:{pin} STATE:{state}", {"device":"SUCTION","pin":pin,"state":state,"raw":cmd}

        if head in ("AIR","FAN","PUMP"):
            idx = _expect(tokens, 1)
            state = onoff(_expect(tokens, 2))
            if idx is None or state is None:
                return None, {"status":"ERROR","message":f"{head} requires <index> ON|OFF","timestamp":now_iso(),"raw":cmd}
            pins = cfg_devices.get(head, {}).get("pins", {})
            pin = pins.get(str(idx))
            if pin is None:
                return None, {"status":"ERROR","message":f"{head} index {idx} not configured","timestamp":now_iso(),"raw":cmd}
            return f"PIN:{pin} STATE:{state}", {"device":head,"index":idx,"pin":pin,"state":state,"raw":cmd}

        if head == "WATER":
            mode = _expect(tokens, 1)
            state = onoff(_expect(tokens, 2))
            if mode is None or state is None:
                return None, {"status":"ERROR","message":"WATER requires DI|RO ON|OFF","timestamp":now_iso(),"raw":cmd}
            modeU = mode.upper()
            modes = cfg_devices.get("WATER", {}).get("modes", {})
            pin = modes.get(modeU)
            if pin is None:
                return None, {"status":"ERROR","message":f"WATER mode {modeU} not configured","timestamp":now_iso(),"raw":cmd}
            return f"PIN:{pin} STATE:{state}", {"device":"WATER","mode":modeU,"pin":pin,"state":state,"raw":cmd}

        return None, {"status":"ERROR","message":f"Unknown device '{head}'","timestamp":now_iso(),"raw":cmd}
    except Exception as e:
        return None, {"status":"ERROR","message":f"Device translation error: {e}","timestamp":now_iso(),"raw":cmd}

# ----------------------- Downstream UDS client with read pump -----------------------
class UDSClient:
    """
    Unix domain socket client:
      - Idempotent connects
      - Single read pump that:
          * If a request is pending -> routes next message to that request
          * Else -> pushes message into .events queue (unsolicited/event)
      - request() returns JSON dict
      - cancel_pending(reason) lets caller abort the in-flight wait (M2/M101 semantics)
    """
    def __init__(self, path: str, read_timeout: float, write_timeout: float, send_term: str, resp_terms: List[str]):
        self.path = path
        self.read_timeout = read_timeout
        self.write_timeout = write_timeout
        self.send_term = send_term
        self.resp_terms = resp_terms
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()         # serialize request() calls
        self._connect_lock = asyncio.Lock() # serialize connects
        self._connecting = False
        self._pump_task: Optional[asyncio.Task] = None
        self._pending: Optional[asyncio.Future] = None  # future for next reply to a request
        self.events: "asyncio.Queue[dict]" = asyncio.Queue(maxsize=1000)

    @property
    def connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    async def ensure_connected(self):
        if self.connected and self._pump_task and not self._pump_task.done():
            return
        loop = asyncio.get_event_loop()
        async with self._connect_lock:
            if self.connected and self._pump_task and not self._pump_task.done():
                return
            if self._connecting:
                deadline = loop.time() + self.write_timeout
                while self._connecting and loop.time() < deadline:
                    await asyncio.sleep(0.05)
                return
            self._connecting = True
            try:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_unix_connection(self.path),
                    timeout=self.write_timeout
                )
                # start pump
                self._pump_task = asyncio.create_task(self._pump_loop())
            finally:
                self._connecting = False

    async def close(self):
        with contextlib.suppress(Exception):
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
        self._reader = None
        self._writer = None
        if self._pump_task:
            self._pump_task.cancel()
        self._pump_task = None
        # do not touch self._pending here; caller may want to cancel explicitly

    def cancel_pending(self, reason: str):
        """Abort the in-flight wait (e.g., after M2 or M101). Safe anytime."""
        if self._pending is not None and not self._pending.done():
            try:
                self._pending.set_result({"status":"ERROR","message":reason,"timestamp":now_iso(),"port":self.path})
            except Exception:
                pass
        self._pending = None

    async def _read_until_any(self) -> List[str]:
        msgs = []
        buf = b""
        while True:
            chunk = await self._reader.read(1024)
            if not chunk:
                raise ConnectionError("peer closed")
            buf += chunk
            s = buf.decode("utf-8", errors="replace")
            for t in self.resp_terms:
                if s.endswith(t) or (t in s and s.split(t)[-1] == ""):
                    for msg in s.split(t):
                        if msg != "":
                            msgs.append(msg.lstrip('\x00'.join(self.resp_terms)).rstrip("").join(self.resp_terms))

                    return msgs

    async def _pump_loop(self):
        try:
            while True:
                raws = await self._read_until_any()
                for raw in raws:
                    d = parse_json_response(raw)
                    # Route to pending request if present; else treat as event
                    if self._pending is not None and not self._pending.done():
                        fut = self._pending
                        self._pending = None
                        try:
                            fut.set_result(d)
                        except Exception:
                            pass
                    else:
                        # drop if queue full (avoid backpressure deadlock)
                        try:
                            self.events.put_nowait(d)
                        except asyncio.QueueFull:
                            try:
                                _ = self.events.get_nowait()
                            except Exception:
                                pass
                            with contextlib.suppress(Exception):
                                self.events.put_nowait(d)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # notify any waiter
            if self._pending is not None and not self._pending.done():
                self._pending.set_result({"status":"ERROR","message":f"Pump error: {e}","timestamp":now_iso(),"port":self.path})
            await self.close()

    async def request(self, payload: str, translate_abb: bool = True) -> dict:
        if translate_abb:
            payloads = translate_abb_command(payload)
        else:
            payloads = [payload]
        
        for load in payloads:
            async with self._lock:
                try:
                    await self.ensure_connected()
                    # create a future to capture the next message as our reply
                    loop = asyncio.get_event_loop()
                    self._pending = loop.create_future()
                    self._writer.write((load + self.send_term).encode())
                    await asyncio.wait_for(self._writer.drain(), timeout=self.write_timeout)
                    # wait for reply delivered by pump
                    reply = await asyncio.wait_for(self._pending, timeout=self.read_timeout)
                    if load == payloads[-1]:
                        return reply
                    
                except Exception as e:
                    await self.close()
                    return {"status":"ERROR","message":f"{type(e).__name__}: {e}","timestamp":now_iso(),"port":self.path}
                finally:
                    # safety: clear pending if still set
                    if self._pending is not None and not self._pending.done():
                        self._pending.cancel()
                    self._pending = None

    # async def send_only(self, payload: str) -> dict:
    #     async with self._lock:
    #         try:
    #             await self.ensure_connected()
    #             self._writer.write((payload + self.send_term).encode())
    #             await asyncio.wait_for(self._writer.drain(), timeout=self.write_timeout)
    #             return True
                
    #         except Exception as e:
    #             await self.close()
    #             return False


# ----------------------- Handler Server -----------------------
@dataclass
class Config:
    handler_path: str
    command_path: str
    control_path: str
    status_path: str
    device_path: str
    default_dir: str
    default_file: str
    read_timeout_s: float
    write_timeout_s: float
    resp_terms: List[str]
    send_term: str
    devices: Dict[str, Any]
    runfile: dict
    reconnect_min_ms: int = 250
    reconnect_max_ms: int = 5000

class MainHandler:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.state = ProgramState.IDLE
        self.file_location_dir = cfg.default_dir
        self.file_default_name = cfg.default_file
        # Note: very long read timeouts on command/control per your change
        self.command = UDSClient(cfg.command_path, 999999, cfg.write_timeout_s, cfg.send_term, cfg.resp_terms)
        self.control = UDSClient(cfg.control_path, 999999, cfg.write_timeout_s, cfg.send_term, cfg.resp_terms)
        self.status = UDSClient(cfg.status_path, 99999, cfg.write_timeout_s, cfg.send_term, cfg.resp_terms)
        #self.device  = UDSClient(cfg.device_path,  cfg.read_timeout_s, cfg.write_timeout_s, cfg.send_term, cfg.resp_terms)
         # --- Multi-device sockets (numbered) ---
        # Build map: socket_id (int) -> UDSClient
        self.device_clients: Dict[int, UDSClient] = {}
        sockets_map = (self.cfg.devices or {}).get("sockets", {}) or {}
        for k, path in sockets_map.items():
            try:
                sid = int(k)
            except Exception:
                continue
            self.device_clients[sid] = UDSClient(
                path,
                self.cfg.read_timeout_s,
                self.cfg.write_timeout_s,
                self.cfg.send_term,
                self.cfg.resp_terms
            )
        self._pending_motion = False
        self._runfile_task: Optional[asyncio.Task] = None
        self._pause_event = asyncio.Event(); self._pause_event.set()
        self._maintainers: List[asyncio.Task] = []
        self._event_watchers: List[asyncio.Task] = []
        self._clients: Set[asyncio.StreamWriter] = set()

    async def start(self):
        # Keep downstreams alive
        self._maintainers = [
            asyncio.create_task(self._maintain_connection("command", self.command)),
            asyncio.create_task(self._maintain_connection("control", self.control)),
            asyncio.create_task(self._maintain_connection("status", self.status)),
            #asyncio.create_task(self._maintain_connection("device",  self.device)),
        ]
                # NEW: maintain each numbered device socket
        for sid, cli in self.device_clients.items():
            self._maintainers.append(
                asyncio.create_task(self._maintain_connection(f"device#{sid}", cli))
            )
        # Watch events from command & control; ignore INFO; react to ERROR
        self._event_watchers = [
            asyncio.create_task(self._event_loop("command", self.command.events)),
            asyncio.create_task(self._event_loop("control", self.control.events)),
            asyncio.create_task(self._event_loop("status", self.status.events)),
            # If you want device events too, uncomment:
            # asyncio.create_task(self._event_loop("device",  self.device.events)),
        ]
        # Start handler server
        try: os.unlink(self.cfg.handler_path)
        except FileNotFoundError: pass
        self.server = await asyncio.start_unix_server(self._client_connected, path=self.cfg.handler_path)
        print(f"[{now_iso()}] handler listening at {self.cfg.handler_path}")
        async with self.server:
            await self.server.serve_forever()
    def _lowest_device_socket_id(self) -> Optional[int]:
        return min(self.device_clients.keys()) if self.device_clients else None

    def _resolve_device_socket_id(self, meta: Dict[str, Any]) -> Optional[int]:
        """
        Decide which device socket to use for a given device meta.
        Priority:
          - devices.routing[TYPE] may be:
              * int (fixed)
              * {by_index: {...}, default: int}
              * {by_mode: {...}, default: int}
        Fallback:
          - lowest numbered socket id
          - if none exist, return None
        """
        routes = (self.cfg.devices or {}).get("routing", {}) or {}
        dtype = str(meta.get("device", "")).upper()
        rule = routes.get(dtype)

        # Fixed int route
        if isinstance(rule, int):
            return rule

        # Dict rule with possible sub-keys
        if isinstance(rule, dict):
            # by_index
            idx = meta.get("index")
            if idx is not None and "by_index" in rule:
                by_index = rule.get("by_index") or {}
                # keys are strings in YAML
                sidx = str(idx)
                if sidx in by_index:
                    try:
                        return int(by_index[sidx])
                    except Exception:
                        pass
            # by_mode
            mode = meta.get("mode")
            if mode is not None and "by_mode" in rule:
                by_mode = rule.get("by_mode") or {}
                smode = str(mode).upper()
                if smode in by_mode:
                    try:
                        return int(by_mode[smode])
                    except Exception:
                        pass
            # default
            if "default" in rule:
                try:
                    return int(rule["default"])
                except Exception:
                    pass

        # Fallback: lowest available device socket id
        return self._lowest_device_socket_id()

    def _get_device_client(self, meta: Dict[str, Any]) -> Optional[UDSClient]:
        sid = self._resolve_device_socket_id(meta)
        if sid is None:
            return None
        return self.device_clients.get(sid) or None


    async def _maintain_connection(self, label: str, client: UDSClient):
        backoff = self.cfg.reconnect_min_ms / 1000.0
        while True:
            try:
                await client.ensure_connected()
                await asyncio.sleep(1.0)
                backoff = self.cfg.reconnect_min_ms / 1000.0
            except Exception as e:
                print(f"[{now_iso()}] maintain[{label}]: {e} (retry in {backoff:.2f}s)")
                await asyncio.sleep(backoff)
                backoff = min(self.cfg.reconnect_max_ms / 1000.0, backoff * 2.0 if backoff > 0 else 0.25)

    async def _event_loop(self, source: str, q: "asyncio.Queue[dict]"):
        """Consume unsolicited events from forwarders.
           - Ignore INFO
           - On ERROR: cancel pending waits, abort runfile, broadcast error
           - On any event that includes a control 'gcode' (M2/M25/M24/M100/M101),
             apply side-effects locally (so G4/RUN_FILE react even if control
             was sent by a different client directly to the control forwarder).
        """
        while True:
            try:
                evt = await q.get()
                status = str(evt.get("status","")).upper()

                # If reply carries a gcode from the control socket, apply it.
                # We accept a few shapes: {reply:{gcode:"M25"}}, {gcode:"M25"}, or a raw control echo.
                gcode = None
                rep = evt.get("reply")
                if isinstance(rep, dict):
                    gcode = rep.get("gcode") or rep.get("GCODE")
                if not gcode:
                    gcode = evt.get("gcode") or evt.get("GCODE")

                if source == "control" and isinstance(gcode, str):
                    self._apply_control_side_effects(gcode.strip().upper())

                # Ignore INFO chatter
                if status == "INFO":
                    continue

                # On ERROR: cancel waits + abort streaming
                if status == "ERROR":
                    await self._on_downstream_error(source, evt)
                    await self._broadcast(json.dumps(
                        {"event":"downstream_error","source":source,"reply":evt,"timestamp":now_iso()},
                        separators=(",",":")
                    ))
                    continue

                if source == "command" and evt.get("complete", False):
                    self._pending_motion = False

                # if evt.get("request_ack", False):
                #     await asyncio.sleep(0.1)
                #     if source == "status":
                #         successful_acknowledgment = await self.status.request("ack", translate_abb=False)
                #     elif source == "control":
                #         successful_acknowledgment = await self.control.request("ack", translate_abb=False)
                #     elif source == "command":
                #         successful_acknowledgment = await self.command.request("ack", translate_abb=False)
                    
                #     print("ack:")
                #     print(successful_acknowledgment)

                #     if not successful_acknowledgment:
                #         await self._on_downstream_error(source, evt)
                #         await self._broadcast(json.dumps(
                #             {"event":"Failed to send acknowledgment message","source":source,"reply":evt,"timestamp":now_iso()},
                #             separators=(",",":")
                #         ))

                # Otherwise (OKAY/FAULT/etc.), just pass through as an event
                await self._broadcast(json.dumps(
                    {"event":"downstream_event","source":source,"reply":evt,"timestamp":now_iso()},
                    separators=(",",":")
                ))
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[{now_iso()}] event_loop[{source}] error: {e}")

    def _apply_control_side_effects(self, gcode: str):
        """Mirror the effects as if these were issued through the handler.
           This lets external control-socket clients interrupt/pause/resume
           internal operations (e.g., G4 dwell; RUN_FILE streaming)."""
        if gcode == "M2":
            # Abort everything: open the gate and cancel pending (command-side) wait
            self.state = ProgramState.ABORTED
            self._pause_event.set()
            self._pending_motion = False
            try:
                self.command.cancel_pending("aborted by external M2")
            except Exception:
                pass

        elif gcode == "M25":
            # Pause (do not cancel pending so M24 can resume)
            if self.state != ProgramState.ABORTED:
                self.state = ProgramState.PAUSED
            self._pause_event.clear()
            self._pending_motion = False

        elif gcode == "M24":
            # Resume gate
            if self.state != ProgramState.ABORTED:
                if (self._runfile_task and not self._runfile_task.done()):
                    self.state = ProgramState.RUNNING_FILE              
                    self._pending_motion = True
                else:
                    self.state = ProgramState.IDLE
                    self._pending_motion = False
                
            self._pause_event.set()

        elif gcode == "M100":
            # Clear ABORTED state
            if self.state == ProgramState.ABORTED:
                self.state = ProgramState.IDLE
                self._pause_event.set()
                self._pending_motion = False

        elif gcode == "M101":
            # Clear pending while PAUSED
            self._pending_motion = False
            if self.state == ProgramState.PAUSED:
                try:
                    self.command.cancel_pending("cleared by external M101")
                except Exception:
                    pass
            pass

    async def _pauseable_sleep(self, seconds: float) -> bool:
        """Sleep that obeys PAUSE (M25) and ABORT (M2).
           Returns True if completed fully; False if aborted."""
        remaining = max(0.0, float(seconds))
        loop = asyncio.get_event_loop()
        deadline = loop.time() + remaining

        while True:
            # If aborted, bail immediately
            if self.state == ProgramState.ABORTED:
                return False

            # If paused, wait until resumed
            await self._pause_event.wait()
            if self.state == ProgramState.ABORTED:
                return False

            now = loop.time()
            if now >= deadline:
                return True

            # Sleep in short slices so we can react quickly to pause/abort
            slice_s = min(0.1, deadline - now)
            await asyncio.sleep(slice_s)

    async def _on_downstream_error(self, source: str, evt: dict):
        """Handle tcp disconnects/timeouts/errors reported by command/control forwarders."""
        reason = evt.get("error") or evt.get("message") or "downstream_error"

        # Cancel any in-flight waits so the awaiting request completes with ERROR immediately
        try:
            self.command.cancel_pending(f"{source} error: {reason}")
        except Exception:
            pass
        try:
            self.control.cancel_pending(f"{source} error: {reason}")
        except Exception:
            pass

        # If a RUN_FILE is active, abort it cleanly
        if self._runfile_task and not self._runfile_task.done():
            # mark state aborted and open the gate; current awaited request will resolve to ERROR and the loop will stop
            self.state = ProgramState.ABORTED
            self._pause_event.set()

    async def _broadcast(self, payload: str):
        term = self.cfg.send_term or "\n"
        dead = []
        for w in list(self._clients):
            try:
                w.write((payload + term).encode())
                await w.drain()
            except Exception:
                dead.append(w)
        for w in dead:
            with contextlib.suppress(Exception):
                w.close()
                await w.wait_closed()
            self._clients.discard(w)

    async def _client_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._clients.add(writer)
        try:
            while True:
                line = await self._read_line(reader)
                if line is None:
                    break
                reply = await self._handle_command(line, writer)
                if reply is not None:
                    await self._write_line(writer, reply)
        except Exception as e:
            with contextlib.suppress(Exception):
                await self._write_line(writer, j_err(f"{type(e).__name__}: {e}"))
        finally:
            with contextlib.suppress(Exception):
                self._clients.discard(writer)
                writer.close()
                await writer.wait_closed()

    async def _read_line(self, reader: asyncio.StreamReader) -> Optional[str]:
        buf = b""
        while True:
            chunk = await reader.read(1024)
            if not chunk:
                return None if not buf else buf.decode("utf-8", errors="replace").rstrip("\0\n\r")
            buf += chunk
            s = buf.decode("utf-8", errors="replace")
            if s.endswith("\0") or s.endswith("\n"):
                return s.rstrip("\0\n\r")

    async def _write_line(self, writer: asyncio.StreamWriter, s: str):
        term = self.cfg.send_term or "\n"
        writer.write((s + term).encode())
        await writer.drain()

    # ----------------------- Routing + FSM -----------------------
    async def _handle_command(self, raw: str, writer: Optional[asyncio.StreamWriter]=None) -> Optional[str]:
        cmd = raw.strip().upper()
        if not cmd:
            return j_err("Empty command")

        # FILE_LOCATION
        if cmd.upper().startswith("FILE_LOCATION"):
            m = re.match(r"(?i)FILE_LOCATION\s*=\s*(.+)$", cmd) or re.match(r"(?i)FILE_LOCATION\s+(.+)$", cmd)
            path = m.group(1).strip() if m else None
            if not path: return j_err("FILE_LOCATION requires a path")
            if not os.path.isdir(path): return j_err(f"Not a directory: {path}")
            self.file_location_dir = path
            return j_ok(message="FILE_LOCATION set")

        # RUN_FILE (streaming)
        if cmd.upper().startswith("RUN_FILE"):
            parts = cmd.split(maxsplit=1)
            filename = self.file_default_name if len(parts) == 1 else (parts[1].strip() or self.file_default_name)
            full = filename if os.path.isabs(filename) else os.path.join(self.file_location_dir, filename)
            if not os.path.isfile(full):
                return j_err(f"File not found: {full}")
            if self.state == ProgramState.RUNNING_FILE:
                return j_err("Already running a file")
            if self.state == ProgramState.ABORTED:
                return j_err("Program is ABORTED (use M100 to clear)")

            if writer: await self._write_line(writer, j_ok(message="RUN_FILE started", file=full))
            self._runfile_task = asyncio.create_task(self._run_file(full, writer))
            return None

        hw = head_word(cmd)

        # Control M-codes
        if hw in CONTROL_MCODES:
            self._apply_control_side_effects(hw)
            # if hw == "M2":
            #     # Abort immediately and cancel any pending command reply (G1/G10/G28/G60)
            #     self.state = ProgramState.ABORTED
            #     self._pause_event.set()
            #     self.command.cancel_pending("aborted by M2")

            # elif hw == "M25":
            #     # Pause: DO NOT cancel pending waits (so M24 can continue them)
            #     if self.state != ProgramState.ABORTED:
            #         self.state = ProgramState.PAUSED
            #     self._pause_event.clear()

            # elif hw == "M24":
            #     # Resume: reopen gate; pending command (if any) may complete
            #     if self.state != ProgramState.ABORTED:
            #         self.state = ProgramState.RUNNING_FILE if (self._runfile_task and not self._runfile_task.done()) else ProgramState.IDLE
            #     self._pause_event.set()

            # elif hw == "M100":
            #     # Clear ABORTED -> IDLE
            #     if self.state == ProgramState.ABORTED:
            #         self.state = ProgramState.IDLE
            #         self._pause_event.set()

            # elif hw == "M101":
            #     # Only valid while PAUSED; cancel any pending command wait
            #     if self.state != ProgramState.PAUSED:
            #         return j_err("M101 allowed only while PAUSED", gcode="M101", state=self.state.name)
            #     self.command.cancel_pending("cleared by M101")

            resp = await self.control.request(cmd)
            return json.dumps(resp, separators=(",",":"))

        if hw in STATUS_MCODES:
            resp = await self.status.request(cmd)
            return json.dumps(resp, separators=(",",":"))

        # Device commands → translate to PIN and forward (multi-socket routing)
        if self._is_device_command(hw):
            payload, meta_or_err = translate_device_command(self.cfg.devices, cmd)
            if payload is None:
                return json.dumps(meta_or_err, separators=(",",":"))

            meta = meta_or_err or {}
            cli = self._get_device_client(meta)
            if cli is None:
                return j_err("No device socket available for route",
                             device=meta.get("device"),
                             index=meta.get("index"),
                             mode=meta.get("mode"))

            resp = await cli.request(payload)
            if isinstance(resp, dict):
                resp = {**resp, **{"device_meta": meta, "routed_socket": getattr(cli, "path", "unknown")}}
            return json.dumps(resp, separators=(",",":"))

        # G4 internally
        if hw == "G4":
            dwell_s = parse_g4_params(cmd)
            if dwell_s is None:
                return j_err("Invalid G4 parameters", gcode="G4")
            # >>> changed: use pauseable/abortable sleep
            ok = await self._pauseable_sleep(dwell_s)
            if not ok:
                return j_err("Aborted during G4 dwell", gcode="G4")
            return j_ok(gcode="G4")

        # Command-side G/M
        if hw in COMMAND_GCODES or hw in COMMAND_MCODES:
            if self.state == ProgramState.PAUSED:
                return j_err(f"Unable to complete {cmd} while paused")
            elif not self._pending_motion:
                self._pending_motion = True
                resp = await self.command.request(cmd)
            else:
                return j_err(f"Unable to complete {cmd} while motion is pending")
            
            return json.dumps(resp, separators=(",",":"))

        # Optional: forward other Gxx to command (comment out to strictly allow-list)
        # if hw.startswith("G") and re.match(r"^G\d+$", hw):
        #     resp = await self.command.request(cmd)
        #     return json.dumps(resp, separators=(",",":"))

        # if hw.startswith("M") and re.match(r"^M\d+$", hw):
        #     return j_err("Unknown/unsupported M-code for this handler", gcode=hw)

        return j_err("Unknown command category")
    def _pick_device_client(self, socket_index: int) -> Optional[UDSClient]:
        if not self.device_clients: return None
        if socket_index < 0 or socket_index >= len(self.device_clients): return None
        return self.device_clients.get(socket_index)


    def _is_device_command(self, head: str) -> bool:
        return head in self.cfg.devices

    # ----------------------- RUN_FILE engine (streams JSON) -----------------------
    async def _run_file(self, path: str, writer: Optional[asyncio.StreamWriter]):
        self.state = ProgramState.RUNNING_FILE
        self._pause_event.set()
        try:
            await self._notify(writer, j_ok(event="runfile_start", file=path))
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for lineno, raw in enumerate(f, start=1):
                    if self.state == ProgramState.ABORTED:
                        await self._notify(writer, j_err("Aborted", event="runfile_abort")); break
                    await self._pause_event.wait()
                    if self.state == ProgramState.ABORTED:
                        await self._notify(writer, j_err("Aborted", event="runfile_abort")); break

                    line = raw.strip()
                    if is_blank_or_comment(line, {"runfile": self.cfg.runfile}): continue
                    line = strip_comment(line, {"runfile": self.cfg.runfile})
                    if not line: continue

                    hw = head_word(line)

                    if hw == "G4":
                        dwell_s = parse_g4_params(line)
                        if dwell_s is None:
                            await self._notify(writer, j_err("Invalid G4 parameters", event="line", gcode="G4", lineno=lineno, raw=line))
                            self.state = ProgramState.IDLE; return
                        await self._notify(writer, j_ok(event="line_send", gcode="G4", lineno=lineno, raw=line, action="dwell", seconds=dwell_s))

                        # >>> changed: pauseable/abortable sleep
                        ok = await self._pauseable_sleep(dwell_s)
                        if not ok:
                            await self._notify(writer, j_err("Aborted during G4 dwell", event="runfile_abort", gcode="G4", lineno=lineno))
                            self.state = ProgramState.ABORTED
                            return

                        await self._notify(writer, j_ok(event="line_done", gcode="G4", lineno=lineno))
                        continue

                    # Route per sets
                    if hw in COMMAND_GCODES or hw in COMMAND_MCODES:
                        self._pending_motion = True
                        resp = await self.command.request(line)
                        await self._notify(writer, json.dumps({"event":"line_resp","lineno":lineno,"raw":line,"source":"command","reply":resp,"timestamp":now_iso()}, separators=(",",":")))
                    elif hw in CONTROL_MCODES:
                        self._apply_control_side_effects(hw)
                        resp = await self.control.request(line)
                        await self._notify(writer, json.dumps({"event":"line_resp","lineno":lineno,"raw":line,"source":"control","reply":resp,"timestamp":now_iso()}, separators=(",",":")))
                    elif hw in STATUS_MCODES:
                        resp = await self.status.request(line)
                        await self._notify(writer, json.dumps({"event":"line_resp","lineno":lineno,"raw":line,"source":"status","reply":resp,"timestamp":now_iso()}, separators=(",",":")))
                    elif self._is_device_command(hw):
                        payload, meta_or_err = translate_device_command(self.cfg.devices, line)
                        if payload is None:
                            await self._notify(writer, json.dumps(meta_or_err, separators=(",",":")))
                            self.state = ProgramState.IDLE
                            return

                        meta = meta_or_err or {}
                        # Use routing logic instead of raw socket_index
                        cli = self._get_device_client(meta)
                        if cli is None:
                            await self._notify(
                                writer,
                                j_err("No device socket available for route",
                                    event="line", lineno=lineno, raw=line,
                                    device=meta.get("device"), index=meta.get("index"), mode=meta.get("mode"))
                            )
                            self.state = ProgramState.IDLE
                            return

                        resp = await cli.request(payload)
                        await self._notify(
                            writer,
                            json.dumps({
                                "event": "line_resp",
                                "lineno": lineno,
                                "raw": line,
                                "source": f"device_route",
                                "translated": payload,
                                "meta": meta,
                                "reply": resp,
                                "timestamp": now_iso()
                            }, separators=(",",":"))
                        )

                        # Decide continue/stop
                        resp_obj = resp if isinstance(resp, dict) else parse_json_response(str(resp))
                        st = str(resp_obj.get("status","")).upper()
                        if st == "OKAY":
                            await self._notify(writer, j_ok(event="line_done", lineno=lineno))
                            continue
                        elif st in ("FAULT","ERROR"):
                            await self._notify(writer, json.dumps({
                                "status": st,
                                "event": "runfile_stop",
                                "lineno": lineno,
                                "reply": resp_obj,
                                "timestamp": now_iso()
                            }, separators=(",",":")))
                            self.state = ProgramState.IDLE
                            return
                        else:
                            await self._notify(writer, j_err("Unknown downstream status", event="runfile_stop", lineno=lineno, reply=resp_obj))
                            self.state = ProgramState.IDLE
                            return

                    # Decide continue/stop
                    resp_obj = resp if isinstance(resp, dict) else parse_json_response(str(resp))
                    st = str(resp_obj.get("status","")).upper()
                    if st == "OKAY":
                        await self._notify(writer, j_ok(event="line_done", lineno=lineno))
                        while self._pending_motion:
                            await asyncio.sleep(0.1)
                        continue
                    elif st in ("FAULT","ERROR"):
                        await self._notify(writer, json.dumps({"status":st,"event":"runfile_stop","lineno":lineno,"reply":resp_obj,"timestamp":now_iso()}, separators=(",",":")))
                        self.state = ProgramState.IDLE; return
                    else:
                        await self._notify(writer, j_err("Unknown downstream status", event="runfile_stop", lineno=lineno, reply=resp_obj))
                        self.state = ProgramState.IDLE; return

            if self.state != ProgramState.ABORTED:
                self.state = ProgramState.IDLE
                await self._notify(writer, j_ok(event="runfile_complete", file=path))
        except Exception as e:
            self.state = ProgramState.IDLE
            await self._notify(writer, j_err(f"RUN_FILE exception: {e}", event="runfile_error"))

    async def _notify(self, writer: Optional[asyncio.StreamWriter], payload: str):
        if writer is None:
            print(payload); return
        try:
            await self._write_line(writer, payload)
        except Exception:
            print(payload)

# ----------------------- Loader & Main -----------------------

def load_config(path: str) -> Config:
    with open(path, "r") as f:
        y = yaml.safe_load(f)
    paths = y.get("paths", {})
    io = y.get("io", {})
    devs = y.get("devices", {})
    runfile = y.get("runfile", {})
    resp_terms = io.get("resp_terminators", ["\0", "\n"])
    send_term = io.get("send_terminator", "\n")
    reconnect_min_ms = int(io.get("reconnect_min_ms", 250))
    reconnect_max_ms = int(io.get("reconnect_max_ms", 5000))
    return Config(
        handler_path = paths.get("handler_socket", "/tmp/handler_socket"),
        command_path = paths.get("command_socket", "/tmp/command_socket"),
        control_path = paths.get("control_socket", "/tmp/control_socket"),
        status_path = paths.get("status_socket", "/tmp/status_socket"),
        device_path  = paths.get("device_socket",  "/tmp/device_socket"),
        default_dir  = paths.get("default_dir", "/opt/jobs"),
        default_file = paths.get("default_file", "job.gcode"),
        read_timeout_s = float(io.get("read_timeout_s", 5)),
        write_timeout_s = float(io.get("write_timeout_s", 2)),
        resp_terms = resp_terms,
        send_term = send_term,
        devices = devs,
        runfile = runfile,
        reconnect_min_ms = reconnect_min_ms,
        reconnect_max_ms = reconnect_max_ms
    )

async def amain():
    cfg_path = os.environ.get("HANDLER_CONFIG", os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.yaml"))
    cfg = load_config(cfg_path)
    os.makedirs(os.path.dirname(cfg.handler_path), exist_ok=True)
    handler = MainHandler(cfg)
    await handler.start()

def main():
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"[{now_iso()}] FATAL: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
