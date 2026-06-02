"""
Kernel bridge — wraps a jupyter_client AsyncKernelManager and translates
ZMQ channel messages into the PyNoteFlow custom WebSocket protocol.

Message protocol (server → client):
  { type: 'stream',         msg_id, name, text }
  { type: 'display_data',   msg_id, data, metadata }
  { type: 'execute_result', msg_id, data, execution_count }
  { type: 'error',          msg_id, ename, evalue, traceback }
  { type: 'execute_reply',  msg_id, status, execution_count }
  { type: 'input_request',  msg_id, prompt, password }
  { type: 'status',         execution_state }
  { type: 'pong' }
  { type: 'info',           python_version, cwd, platform }

Message protocol (client → server):
  { type: 'execute',     msg_id, code }
  { type: 'interrupt' }
  { type: 'restart' }
  { type: 'input_reply', msg_id, value }
  { type: 'ping' }
  { type: 'complete',    msg_id, code, cursor_pos }
"""
import asyncio
import json as _json
import logging
import os
import platform
import re
import sys
import time

logger = logging.getLogger(__name__)


# ── Config helpers ────────────────────────────────────────────────────────────

def _pnf_config_path() -> str:
    return os.path.join(os.path.expanduser('~'), '.pynoteflow', 'config.json')

def _load_pnf_config() -> dict:
    """Load ~/.pynoteflow/config.json or return {}."""
    try:
        p = _pnf_config_path()
        if os.path.isfile(p):
            with open(p, 'r', encoding='utf-8') as f:
                return _json.load(f)
    except Exception:
        pass
    return {}

def save_pnf_config(updates: dict) -> None:
    """Merge updates into ~/.pynoteflow/config.json."""
    p = _pnf_config_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    cfg = _load_pnf_config()
    cfg.update(updates)
    with open(p, 'w', encoding='utf-8') as f:
        _json.dump(cfg, f, indent=2)

def get_kernel_python() -> str:
    """Return the Python executable to use for the IPython kernel.

    Priority:
      1. 'kernel_python' key in ~/.pynoteflow/config.json (explicit user setting)
      2. sys.executable — the PNF server's OWN Python. By design the server is
         installed via `uv tool install pynoteflow-server`, so this is the
         dedicated uv-tool interpreter at
         `~/AppData/Roaming/uv/tools/pynoteflow-server/Scripts/python.exe`
         (or the equivalent on macOS/Linux). Using this as the universal
         default means PNF kernel, PNF PTY, and the auto-registered Jupyter
         kernelspec all share ONE interpreter — exactly what the user wants.
    """
    try:
        kp = _load_pnf_config().get('kernel_python', '')
        if kp and os.path.isfile(kp):
            return kp
    except Exception:
        pass
    return sys.executable


class KernelBridge:
    """Manages one IPython kernel and forwards messages to an aiohttp WebSocket."""

    def __init__(self):
        self.km = None          # AsyncKernelManager
        self.kc = None          # AsyncKernelClient
        self._ws = None         # current aiohttp WebSocketResponse
        self._iopub_task = None
        self._shell_task = None
        self._stdin_task = None
        # Map kernel msg_id → client msg_id
        self._exec_map: dict[str, str] = {}
        # Map kernel msg_id -> execution metadata (e.g. install commands)
        self._exec_meta: dict[str, dict] = {}
        # Server-side guard: reject concurrent/duplicate pip installs even if
        # a buggy client dispatches duplicate execute requests.
        self._install_active = False
        self._install_active_msg_id: str | None = None
        self._install_active_ts = 0.0          # wall-clock time lock was set
        self._install_timeout_s = 600.0        # auto-expire stale lock after 10 min
        self._last_install_fingerprint = ""
        self._last_install_ts = 0.0
        self._install_dedupe_window_s = 8.0

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        import jupyter_client
        self.km = jupyter_client.AsyncKernelManager()
        # Use the configured kernel Python (may differ from the server's Python).
        kp = get_kernel_python()
        if kp != sys.executable:
            self.km.kernel_spec.argv[0] = kp
            logger.info("Using custom kernel Python: %s", kp)
        await self.km.start_kernel()
        self.kc = self.km.client()
        self.kc.start_channels()
        try:
            await asyncio.wait_for(self.kc.wait_for_ready(), timeout=60)
        except asyncio.TimeoutError:
            raise RuntimeError("Kernel did not become ready within 60 s")
        self._iopub_task = asyncio.create_task(self._iopub_loop(), name="iopub")
        self._shell_task = asyncio.create_task(self._shell_loop(), name="shell")
        self._stdin_task = asyncio.create_task(self._stdin_loop(), name="stdin")
        logger.info("Kernel started (pid %s)", self.km.kernel.pid if hasattr(self.km, 'kernel') else '?')

    async def stop(self) -> None:
        for task in (self._iopub_task, self._shell_task, self._stdin_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self.kc:
            self.kc.stop_channels()
        if self.km:
            await self.km.shutdown_kernel(now=True)
        logger.info("Kernel stopped")

    async def restart(self) -> None:
        # Cancel message-consuming loops before calling wait_for_ready().
        # Without this, _shell_loop() races with wait_for_ready() for the
        # kernel_info_reply on the shell ZMQ channel; if _shell_loop() wins
        # that race, wait_for_ready() never sees the reply and blocks for the
        # full 60-second timeout — freezing the WebSocket dispatch loop.
        for task in (self._iopub_task, self._shell_task, self._stdin_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._iopub_task = self._shell_task = self._stdin_task = None

        self._exec_map.clear()
        self._exec_meta.clear()
        self._install_active = False
        self._install_active_msg_id = None
        await self.km.restart_kernel()
        await asyncio.wait_for(self.kc.wait_for_ready(), timeout=60)

        # Restart message loops for the fresh kernel
        self._iopub_task = asyncio.create_task(self._iopub_loop(), name="iopub")
        self._shell_task = asyncio.create_task(self._shell_loop(), name="shell")
        self._stdin_task = asyncio.create_task(self._stdin_loop(), name="stdin")
        logger.info("Kernel restarted")

    async def interrupt(self) -> None:
        await self.km.interrupt_kernel()

    # ── WebSocket attachment ─────────────────────────────────────────────────

    def attach_ws(self, ws) -> None:
        self._ws = ws
        # A new client connection means the previous browser session is gone.
        # Any outstanding install lock is stale — clear it so the new session
        # can install packages immediately without hitting "InstallBusy".
        self._install_active = False
        self._install_active_msg_id = None
        self._exec_map.clear()
        self._exec_meta.clear()
        logger.debug("New client attached — install lock and pending exec maps cleared")

    def detach_ws(self) -> None:
        self._ws = None

    # ── Client commands ──────────────────────────────────────────────────────

    @staticmethod
    def _extract_install_target(code: str) -> str:
        """Best-effort package token extraction from pip install snippets."""
        # Path 1: explicit shell command, e.g. !pip install torch --index-url ...
        m = re.search(r"pip\s+install\s+([^\s\"']+)", code, flags=re.IGNORECASE)
        if m:
            target = m.group(1).strip()
            if target and not target.startswith("-"):
                return target
        # Path 2: extension-generated python wrapper with _pargs = ['install', ...]
        m = re.search(r"_pargs\s*=\s*\[(.*?)\]", code, flags=re.DOTALL)
        if m:
            raw = m.group(1)
            toks = re.findall(r"['\"]([^'\"]+)['\"]", raw)
            if toks:
                if toks[0].lower() == "install" and len(toks) > 1:
                    for t in toks[1:]:
                        if t and not t.startswith("-"):
                            return t
                if toks[0].lower() != "install":
                    return toks[0]
        return ""

    @staticmethod
    def _is_install_command(code: str) -> bool:
        c = (code or "").lower()
        if "pip uninstall" in c:
            return False
        if re.search(r"(^|\W)pip\s+install(\s|$)", c):
            return True
        if "\"-m\", \"pip\"" in c and "'install'" in c:
            return True
        if "\"-m\", \"pip\"" in c and '"install"' in c:
            return True
        if "_pargs" in c and "install" in c and "pip" in c:
            return True
        return False

    @staticmethod
    def _is_upgrade_or_force(code: str) -> bool:
        """Return True when the command is an explicit upgrade or force-reinstall.
        These bypass the duplicate-fingerprint dedup window so the user can
        deliberately upgrade/reinstall even within the cooldown period."""
        c = (code or "").lower()
        return (
            "--force-reinstall" in c
            or "--force_reinstall" in c
            or "--upgrade" in c
            or bool(re.search(r"\s-u(\s|$)", c))
        )

    async def execute(self, code: str, client_msg_id: str) -> None:
        is_install = self._is_install_command(code)
        pkg = self._extract_install_target(code)
        if is_install:
            now = time.time()
            is_force_upgrade = self._is_upgrade_or_force(code)
            fingerprint = f"{pkg}|{code.strip()[:300]}"
            # Auto-expire stale install lock (safety net for crashes/lost sessions)
            if self._install_active and (now - self._install_active_ts) > self._install_timeout_s:
                logger.warning("Install lock expired after %.0fs — auto-clearing", now - self._install_active_ts)
                self._install_active = False
                self._install_active_msg_id = None
            # Reject exact duplicate install burst (double-click / race).
            # Skipped for explicit --upgrade / --force-reinstall so the user
            # can intentionally re-run an upgrade command without cooldown.
            if (
                not is_force_upgrade
                and self._last_install_fingerprint == fingerprint
                and (now - self._last_install_ts) < self._install_dedupe_window_s
            ):
                await self._send({
                    "type": "error",
                    "msg_id": client_msg_id,
                    "ename": "DuplicateInstall",
                    "evalue": f"Duplicate install ignored for '{pkg or 'package'}' (already started).",
                    "traceback": [],
                })
                # Send execute_reply so the client's onDone callback fires and
                # the browser's _sysBusy flag is cleared properly.
                await self._send({
                    "type": "execute_reply",
                    "msg_id": client_msg_id,
                    "status": "error",
                    "execution_count": None,
                })
                return
            # Reject concurrent install while another one is active (always enforced)
            if self._install_active:
                await self._send({
                    "type": "error",
                    "msg_id": client_msg_id,
                    "ename": "InstallBusy",
                    "evalue": "Another package install is still running. Please wait for completion.",
                    "traceback": [],
                })
                # Send execute_reply so the client's onDone callback fires and
                # the browser's _sysBusy flag is cleared properly.
                await self._send({
                    "type": "execute_reply",
                    "msg_id": client_msg_id,
                    "status": "error",
                    "execution_count": None,
                })
                return
            self._install_active = True
            self._install_active_ts = now
            # Only update dedup fingerprint for non-force installs
            if not is_force_upgrade:
                self._last_install_fingerprint = fingerprint
                self._last_install_ts = now
        kernel_msg_id = self.kc.execute(code, store_history=True)
        self._exec_map[kernel_msg_id] = client_msg_id
        self._exec_meta[kernel_msg_id] = {"is_install": is_install, "pkg": pkg}
        if is_install:
            self._install_active_msg_id = kernel_msg_id

    async def complete(self, code: str, cursor_pos: int, client_msg_id: str) -> None:
        kernel_msg_id = self.kc.complete(code, cursor_pos)
        # shell loop will forward the reply
        self._exec_map[kernel_msg_id] = client_msg_id

    def input_reply(self, value: str) -> None:
        self.kc.input(value)

    # ── Channel pump loops ────────────────────────────────────────────────────

    async def _iopub_loop(self) -> None:
        while True:
            try:
                msg = await self.kc.get_iopub_msg()
                await self._handle_iopub(msg)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("iopub loop: %s", exc)
                await asyncio.sleep(0)

    async def _shell_loop(self) -> None:
        while True:
            try:
                msg = await self.kc.get_shell_msg()
                await self._handle_shell(msg)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("shell loop: %s", exc)
                await asyncio.sleep(0)

    async def _stdin_loop(self) -> None:
        while True:
            try:
                msg = await self.kc.get_stdin_msg()
                await self._handle_stdin(msg)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("stdin loop: %s", exc)
                await asyncio.sleep(0)

    # ── Message translators ───────────────────────────────────────────────────

    def _client_id(self, kernel_msg_id: str) -> str:
        return self._exec_map.get(kernel_msg_id, kernel_msg_id)

    async def _send(self, obj: dict) -> None:
        if self._ws and not self._ws.closed:
            try:
                await self._ws.send_json(obj)
            except Exception as exc:
                logger.debug("ws send: %s", exc)

    async def _handle_iopub(self, msg: dict) -> None:
        mt = msg.get("msg_type", "")
        parent_id = msg.get("parent_header", {}).get("msg_id", "")
        client_id = self._client_id(parent_id)
        content = msg.get("content", {})

        if mt == "status":
            await self._send({"type": "status", "execution_state": content.get("execution_state", "idle")})

        elif mt == "stream":
            await self._send({
                "type": "stream",
                "msg_id": client_id,
                "name": content.get("name", "stdout"),
                "text": content.get("text", ""),
            })

        elif mt == "display_data":
            await self._send({
                "type": "display_data",
                "msg_id": client_id,
                "data": content.get("data", {}),
                "metadata": content.get("metadata", {}),
            })

        elif mt == "execute_result":
            await self._send({
                "type": "execute_result",
                "msg_id": client_id,
                "data": content.get("data", {}),
                "execution_count": content.get("execution_count"),
            })

        elif mt == "error":
            await self._send({
                "type": "error",
                "msg_id": client_id,
                "ename": content.get("ename", ""),
                "evalue": content.get("evalue", ""),
                "traceback": content.get("traceback", []),
            })

        elif mt == "clear_output":
            await self._send({"type": "clear_output", "msg_id": client_id})

    async def _handle_shell(self, msg: dict) -> None:
        mt = msg.get("msg_type", "")
        parent_id = msg.get("parent_header", {}).get("msg_id", "")
        client_id = self._client_id(parent_id)
        content = msg.get("content", {})

        if mt == "execute_reply":
            meta = self._exec_meta.pop(parent_id, {})
            if meta.get("is_install") and parent_id == self._install_active_msg_id:
                self._install_active = False
                self._install_active_msg_id = None
            self._exec_map.pop(parent_id, None)
            await self._send({
                "type": "execute_reply",
                "msg_id": client_id,
                "status": content.get("status", "ok"),
                "execution_count": content.get("execution_count"),
            })

        elif mt == "complete_reply":
            self._exec_meta.pop(parent_id, None)
            self._exec_map.pop(parent_id, None)
            await self._send({
                "type": "complete_reply",
                "msg_id": client_id,
                "matches": content.get("matches", []),
                "cursor_start": content.get("cursor_start", 0),
                "cursor_end": content.get("cursor_end", 0),
                "metadata": content.get("metadata", {}),
            })

    async def _handle_stdin(self, msg: dict) -> None:
        mt = msg.get("msg_type", "")
        parent_id = msg.get("parent_header", {}).get("msg_id", "")
        client_id = self._client_id(parent_id)
        content = msg.get("content", {})

        if mt == "input_request":
            await self._send({
                "type": "input_request",
                "msg_id": client_id,
                "prompt": content.get("prompt", ""),
                "password": content.get("password", False),
            })


def get_server_info() -> dict:
    from . import __version__ as _v
    kp = get_kernel_python()
    return {
        "type": "info",
        "python_version": sys.version,
        "cwd": os.getcwd(),
        "platform": platform.platform(),
        "server_version": _v,
        "kernel_python": kp,
    }


# ── Kernel discovery & switching ─────────────────────────────────────────────

import subprocess as _subprocess
import hashlib as _hashlib
import glob as _glob


def _probe_python(path: str, timeout: float = 4.0) -> dict | None:
    """Run `path -c <probe>` and return {path, version, impl, prefix} or None."""
    if not path or not os.path.isfile(path):
        return None
    try:
        probe = (
            "import sys,platform,json;"
            "print(json.dumps({"
            "'version': sys.version.split()[0],"
            "'impl': platform.python_implementation(),"
            "'prefix': sys.prefix"
            "}))"
        )
        creationflags = 0
        if os.name == "nt":
            creationflags = getattr(_subprocess, "CREATE_NO_WINDOW", 0)
        r = _subprocess.run(
            [path, "-c", probe],
            capture_output=True, text=True, timeout=timeout,
            creationflags=creationflags,
        )
        if r.returncode != 0:
            return None
        info = _json.loads((r.stdout or "").strip().splitlines()[-1])
        info["path"] = path
        return info
    except Exception:
        return None


def _candidate_python_paths() -> list[str]:
    """Collect candidate python.exe paths from common locations (no probing)."""
    out: list[str] = []
    home = os.path.expanduser("~")
    appdata = os.environ.get("APPDATA", "")
    local = os.environ.get("LOCALAPPDATA", "")
    winapps = os.path.join(local, "Microsoft", "WindowsApps").lower() if local else ""

    # 1) Currently configured kernel python
    out.append(get_kernel_python())
    # 2) Server's own python
    out.append(sys.executable)

    # 3) PATH lookups
    import shutil as _sh
    for cmd in ("python", "python3", "python3.11", "python3.12", "python3.10"):
        p = _sh.which(cmd)
        if p:
            out.append(p)

    # 4) Windows py launcher
    if os.name == "nt":
        try:
            r = _subprocess.run(["py", "-0p"], capture_output=True, text=True, timeout=4)
            for line in (r.stdout or "").splitlines():
                # lines like " -V:3.11 *        C:\\Path\\python.exe"
                m = re.search(r"([A-Za-z]:\\[^\s]+python\.exe)", line)
                if m:
                    out.append(m.group(1))
        except Exception:
            pass

        # 5) uv tool envs
        if appdata:
            out.extend(_glob.glob(os.path.join(appdata, "uv", "tools", "*", "Scripts", "python.exe")))
        # 6) python.org installs
        if local:
            out.extend(_glob.glob(os.path.join(local, "Programs", "Python", "Python*", "python.exe")))
        # 7) Conda envs
        for root in (os.path.join(home, "Anaconda3"), os.path.join(home, "miniconda3"),
                     os.path.join(home, "anaconda3"), os.path.join(home, "Miniconda3")):
            out.append(os.path.join(root, "python.exe"))
            out.extend(_glob.glob(os.path.join(root, "envs", "*", "python.exe")))
        # 8) Workspace .venv (cwd at server start)
        out.append(os.path.join(os.getcwd(), ".venv", "Scripts", "python.exe"))
    else:
        # Linux/Mac fallbacks
        for p in ("/usr/bin/python3", "/usr/local/bin/python3", "/opt/homebrew/bin/python3"):
            out.append(p)

    # Filter to existing files, dedup case-insensitively, skip Windows Store stubs
    seen: set[str] = set()
    final: list[str] = []
    for p in out:
        if not p:
            continue
        try:
            real = os.path.realpath(p)
        except Exception:
            real = p
        key = real.lower() if os.name == "nt" else real
        if key in seen:
            continue
        if not os.path.isfile(real):
            continue
        # Skip Windows Store app-execution-alias stubs
        if winapps and winapps in real.lower():
            # Allow only if it actually works (real install). Most stubs are 0 bytes.
            try:
                if os.path.getsize(real) < 1024:
                    continue
            except Exception:
                pass
        seen.add(key)
        final.append(real)
    return final


def discover_pythons() -> list[dict]:
    """Return list of {path, version, impl, prefix, label, is_active}."""
    active = get_kernel_python()
    try:
        active_real = os.path.realpath(active)
    except Exception:
        active_real = active
    results: list[dict] = []
    for p in _candidate_python_paths():
        info = _probe_python(p)
        if not info:
            continue
        # Friendly label
        prefix = info.get("prefix", "")
        base = os.path.basename(prefix.rstrip(os.sep)) or os.path.basename(p)
        src = ""
        lower = p.lower()
        if "\\uv\\tools\\" in lower or "/uv/tools/" in lower:
            src = "uv tool"
        elif "\\anaconda" in lower or "\\miniconda" in lower or "/anaconda" in lower:
            src = "conda"
        elif "\\windowsapps\\" in lower:
            src = "Microsoft Store"
        elif "\\programs\\python\\" in lower:
            src = "python.org"
        elif "\\.venv\\" in lower or "/.venv/" in lower:
            src = "venv"
        info["label"] = f"Python {info.get('version','?')} — {base}" + (f" ({src})" if src else "")
        info["is_active"] = (os.path.realpath(p).lower() == active_real.lower()) if os.name == "nt" else (os.path.realpath(p) == active_real)
        results.append(info)
    # Sort: active first, then by version desc, then by label
    results.sort(key=lambda r: (not r.get("is_active"), r.get("version", ""), r.get("label", "")), reverse=False)
    # Put active at top by reversing the bool key
    results.sort(key=lambda r: 0 if r.get("is_active") else 1)
    return results


def _kernelspec_name_for(python_path: str) -> str:
    """Stable kernelspec name derived from the interpreter path."""
    h = _hashlib.sha1(os.path.realpath(python_path).lower().encode("utf-8")).hexdigest()[:8]
    return f"pnf-{h}"


def register_jupyter_kernelspec(python_path: str, ensure_ipykernel: bool = True,
                                timeout: float = 120.0) -> dict:
    """Install ipykernel (if missing) and register a `--user` kernelspec
    named pnf-<hash> pointing at *python_path*. Returns dict with status.
    """
    if not os.path.isfile(python_path):
        return {"ok": False, "error": f"Not a file: {python_path}"}
    name = _kernelspec_name_for(python_path)
    base = os.path.basename(os.path.dirname(os.path.dirname(python_path))) or os.path.basename(python_path)
    display = f"PyNoteFlow: {base}"
    creationflags = getattr(_subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
    log: list[str] = []

    def _run(args: list[str]) -> tuple[int, str, str]:
        try:
            r = _subprocess.run(args, capture_output=True, text=True,
                                timeout=timeout, creationflags=creationflags)
            return r.returncode, (r.stdout or ""), (r.stderr or "")
        except Exception as e:
            return -1, "", str(e)

    # Ensure ipykernel is installed (try import first; if missing and allowed, pip install)
    rc, _so, _se = _run([python_path, "-c", "import ipykernel"])
    if rc != 0:
        if not ensure_ipykernel:
            return {"ok": False, "error": "ipykernel not installed and auto-install disabled"}
        log.append("Installing ipykernel...")
        rc, so, se = _run([python_path, "-m", "pip", "install", "--user", "--quiet", "ipykernel"])
        if rc != 0:
            return {"ok": False, "error": "pip install ipykernel failed",
                    "log": log + [so, se]}

    # Register kernelspec
    log.append(f"Registering kernelspec '{name}'...")
    rc, so, se = _run([python_path, "-m", "ipykernel", "install", "--user",
                       "--name", name, "--display-name", display])
    if rc != 0:
        return {"ok": False, "error": "ipykernel install failed",
                "log": log + [so, se], "name": name}
    return {"ok": True, "name": name, "display_name": display, "log": log + [so]}


# Attach a switch method to KernelBridge via monkey-patch (keeps diff small).
async def _bridge_switch_kernel_python(self: "KernelBridge", new_path: str) -> dict:
    """Persist new kernel python, stop current kernel, start fresh one."""
    if not os.path.isfile(new_path):
        raise ValueError(f"Python executable not found: {new_path}")
    save_pnf_config({"kernel_python": new_path})
    # Stop + restart with fresh KernelManager so argv[0] is rebuilt
    try:
        await self.stop()
    except Exception:
        logger.exception("Error stopping kernel during switch")
    self.km = None
    self.kc = None
    self._iopub_task = self._shell_task = self._stdin_task = None
    self._exec_map.clear()
    self._exec_meta.clear()
    self._install_active = False
    self._install_active_msg_id = None
    await self.start()
    return {"kernel_python": new_path, "version": (_probe_python(new_path) or {}).get("version", "?")}

KernelBridge.switch_kernel_python = _bridge_switch_kernel_python  # type: ignore[attr-defined]
