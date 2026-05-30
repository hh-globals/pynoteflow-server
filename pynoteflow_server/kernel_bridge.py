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
import sys
import uuid

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
        self._exec_map.clear()
        await self.km.restart_kernel()
        await asyncio.wait_for(self.kc.wait_for_ready(), timeout=60)
        logger.info("Kernel restarted")

    async def interrupt(self) -> None:
        await self.km.interrupt_kernel()

    # ── WebSocket attachment ─────────────────────────────────────────────────

    def attach_ws(self, ws) -> None:
        self._ws = ws

    def detach_ws(self) -> None:
        self._ws = None

    # ── Client commands ──────────────────────────────────────────────────────

    def execute(self, code: str, client_msg_id: str) -> None:
        kernel_msg_id = self.kc.execute(code, store_history=True)
        self._exec_map[kernel_msg_id] = client_msg_id

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
                msg = await asyncio.wait_for(self.kc.get_iopub_msg(), timeout=0.1)
                await self._handle_iopub(msg)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("iopub loop: %s", exc)
                await asyncio.sleep(0.05)

    async def _shell_loop(self) -> None:
        while True:
            try:
                msg = await asyncio.wait_for(self.kc.get_shell_msg(), timeout=0.1)
                await self._handle_shell(msg)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("shell loop: %s", exc)
                await asyncio.sleep(0.05)

    async def _stdin_loop(self) -> None:
        while True:
            try:
                msg = await asyncio.wait_for(self.kc.get_stdin_msg(), timeout=0.1)
                await self._handle_stdin(msg)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("stdin loop: %s", exc)
                await asyncio.sleep(0.05)

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
            self._exec_map.pop(parent_id, None)
            await self._send({
                "type": "execute_reply",
                "msg_id": client_id,
                "status": content.get("status", "ok"),
                "execution_count": content.get("execution_count"),
            })

        elif mt == "complete_reply":
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
        "server_version": "1.0.0",
    }
