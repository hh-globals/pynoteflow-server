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
import logging
import os
import platform
import re
import sys
import time

logger = logging.getLogger(__name__)


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
        self._last_install_fingerprint = ""
        self._last_install_ts = 0.0
        self._install_dedupe_window_s = 8.0

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        import jupyter_client
        self.km = jupyter_client.AsyncKernelManager()
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

    async def execute(self, code: str, client_msg_id: str) -> None:
        is_install = self._is_install_command(code)
        pkg = self._extract_install_target(code)
        if is_install:
            now = time.time()
            fingerprint = f"{pkg}|{code.strip()[:300]}"
            # Reject exact duplicate install burst (double-click / race)
            if (
                self._last_install_fingerprint == fingerprint
                and (now - self._last_install_ts) < self._install_dedupe_window_s
            ):
                await self._send({
                    "type": "error",
                    "msg_id": client_msg_id,
                    "ename": "DuplicateInstall",
                    "evalue": f"Duplicate install ignored for '{pkg or 'package'}' (already started).",
                    "traceback": [],
                })
                return
            # Reject concurrent install while another one is active
            if self._install_active:
                await self._send({
                    "type": "error",
                    "msg_id": client_msg_id,
                    "ename": "InstallBusy",
                    "evalue": "Another package install is still running. Please wait for completion.",
                    "traceback": [],
                })
                return
            self._install_active = True
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
    return {
        "type": "info",
        "python_version": sys.version,
        "cwd": os.getcwd(),
        "platform": platform.platform(),
        "server_version": "1.0.2",
    }
