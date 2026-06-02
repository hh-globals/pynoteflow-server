"""
aiohttp server — HTTP + WebSocket endpoints.

Endpoints:
  GET  /ping          Health check → { status: 'ok', version: '1.0.0' }
  GET  /info          Kernel info  → { python_version, cwd, platform, ... }
  GET  /ws            WebSocket — single persistent connection for the extension
  GET  /ws/pty        WebSocket — PTY terminal session (one per connection)
  POST /restart       Restart the kernel → { status: 'restarting' }
  POST /interrupt     Interrupt the kernel → { status: 'interrupted' }

CORS:
  Only chrome-extension:// and ms-browser-extension:// origins accepted.
  localhost http:// origins also allowed (for dev/testing).
"""
import asyncio
import json
import logging
import os
import shlex
import sys
from aiohttp import web, WSMsgType

from . import __version__
from .kernel_bridge import (
    KernelBridge, get_server_info, get_kernel_python,
    _load_pnf_config, save_pnf_config,
    discover_pythons, register_jupyter_kernelspec, _kernelspec_name_for,
)

logger = logging.getLogger(__name__)

_ALLOWED_ORIGIN_PREFIXES = (
    "chrome-extension://",
    "ms-browser-extension://",
    "http://localhost",
    "http://127.0.0.1",
    "null",      # regular browsers send Origin: null for file:// pages
    "file://",   # Electron (VS Code) sends Origin: file:// for file:// pages
)


def _is_allowed_origin(origin: str | None) -> bool:
    if origin is None:
        return True  # Same-origin or non-browser client
    return any(origin.startswith(p) for p in _ALLOWED_ORIGIN_PREFIXES)


@web.middleware
async def cors_middleware(request: web.Request, handler):
    origin = request.headers.get("Origin")
    if not _is_allowed_origin(origin):
        raise web.HTTPForbidden(reason="Origin not allowed")

    if request.method == "OPTIONS":
        # Preflight
        resp = web.Response()
    else:
        resp = await handler(request)

    if origin:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


# ── HTTP handlers ─────────────────────────────────────────────────────────────

async def handle_ping(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "version": __version__, "server": "pynoteflow"})


async def handle_info(request: web.Request) -> web.Response:
    bridge: KernelBridge = request.app["bridge"]
    info = get_server_info()
    try:
        ka = bridge.km.is_alive() if bridge.km is not None else False
        if asyncio.iscoroutine(ka):
            ka = await ka
        info["kernel_alive"] = bool(ka)
    except Exception:
        info["kernel_alive"] = False
    return web.json_response(info)


async def handle_restart(request: web.Request) -> web.Response:
    bridge: KernelBridge = request.app["bridge"]
    try:
        await bridge.restart()
        return web.json_response({"status": "ok"})
    except Exception as exc:
        return web.json_response({"status": "error", "message": str(exc)}, status=500)


async def handle_interrupt(request: web.Request) -> web.Response:
    bridge: KernelBridge = request.app["bridge"]
    await bridge.interrupt()
    return web.json_response({"status": "ok"})


# ── WebSocket handler ─────────────────────────────────────────────────────────

async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    origin = request.headers.get("Origin")
    if not _is_allowed_origin(origin):
        raise web.HTTPForbidden(reason="Origin not allowed")

    bridge: KernelBridge = request.app["bridge"]
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    # Only one client at a time
    old_ws = bridge._ws
    if old_ws and not old_ws.closed:
        await old_ws.close(code=1001, message=b"New client connected")
    bridge.attach_ws(ws)

    # Send initial info
    info = get_server_info()
    info["type"] = "info"
    try:
        ka = bridge.km.is_alive() if bridge.km is not None else False
        if asyncio.iscoroutine(ka):
            ka = await ka
        info["kernel_alive"] = bool(ka)
    except Exception:
        info["kernel_alive"] = False
    await ws.send_json(info)

    logger.info("WebSocket client connected (origin=%s)", origin)

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    await _dispatch(bridge, ws, data)
                except json.JSONDecodeError:
                    await ws.send_json({"type": "error", "msg_id": None, "ename": "ParseError",
                                        "evalue": "Invalid JSON", "traceback": []})
            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break
    finally:
        bridge.detach_ws()
        logger.info("WebSocket client disconnected")

    return ws


async def _dispatch(bridge: KernelBridge, ws, data: dict) -> None:
    t = data.get("type")

    if t == "execute":
        code = data.get("code", "")
        msg_id = data.get("msg_id", "")
        if not msg_id:
            await ws.send_json({"type": "error", "msg_id": None, "ename": "ProtocolError",
                                "evalue": "msg_id required", "traceback": []})
            return
        await bridge.execute(code, msg_id)

    elif t == "interrupt":
        await bridge.interrupt()

    elif t == "restart":
        try:
            await bridge.restart()
            await ws.send_json({"type": "restart_reply", "status": "ok"})
        except Exception as exc:
            await ws.send_json({"type": "restart_reply", "status": "error", "message": str(exc)})

    elif t == "input_reply":
        bridge.input_reply(data.get("value", ""))

    elif t == "complete":
        code = data.get("code", "")
        cursor_pos = data.get("cursor_pos", len(code))
        msg_id = data.get("msg_id", "")
        await bridge.complete(code, cursor_pos, msg_id)

    elif t == "ping":
        await ws.send_json({"type": "pong"})


# ── PTY terminal WebSocket handler ────────────────────────────────────────────
#
# Protocol (text frames only):
#   client → server:  { "type": "input",  "data": "<chars>" }
#                     { "type": "resize", "cols": N, "rows": N }
#   server → client:  { "type": "output", "data": "<chars>" }
#                     { "type": "exit",   "code": N }
#
# On Unix    — uses pty.openpty() for a real PTY with full ANSI support.
# On Windows — uses pywinpty (ConPTY, Windows 10 1809+) for a real PTY.

async def handle_ws_pty(request: web.Request) -> web.WebSocketResponse:
    origin = request.headers.get("Origin")
    if not _is_allowed_origin(origin):
        raise web.HTTPForbidden(reason="Origin not allowed")

    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    _IS_WIN = sys.platform == "win32"

    if _IS_WIN:
        # Windows: pywinpty — real ConPTY (requires pywinpty package)
        try:
            import winpty
        except ImportError:
            await ws.send_json({"type": "output", "data":
                "\r\n\x1b[31mError: pywinpty not installed.\x1b[0m\r\n"
                "Run:  pip install pywinpty  then restart the server.\r\n"})
            return ws

        # Prepend the kernel's Python Scripts dir to PATH so pip/python in
        # the PTY shell resolves to the same environment as the kernel.
        _kernel_py = get_kernel_python()
        _py_scripts = os.path.dirname(_kernel_py)
        _pty_env = dict(os.environ)
        _pty_env['PATH'] = _py_scripts + os.pathsep + _pty_env.get('PATH', '')
        # Propagate virtual environment if the server is running inside one.
        if sys.prefix != getattr(sys, 'base_prefix', sys.prefix):
            _pty_env['VIRTUAL_ENV'] = sys.prefix

        loop = asyncio.get_event_loop()
        shell = "powershell.exe"
        # Launch PowerShell with -NoExit and an init command that redefines
        # `python`, `python3`, `pip`, `pip3`, and `pip3.X` to delegate to the
        # exact kernel Python. This guarantees `pip list` in the PTY terminal
        # matches the packages the kernel can import — eliminating the
        # PATH-pip-vs-kernel-pip mismatch users were seeing.
        _kp = _kernel_py.replace("'", "''")  # PowerShell single-quote escape
        _init = (
            "$ErrorActionPreference='SilentlyContinue';"
            f"$Global:PNF_KERNEL_PY = '{_kp}';"
            "function python  { & $Global:PNF_KERNEL_PY @args }; "
            "function python3 { & $Global:PNF_KERNEL_PY @args }; "
            "function pip     { & $Global:PNF_KERNEL_PY -m pip @args }; "
            "function pip3    { & $Global:PNF_KERNEL_PY -m pip @args }; "
            "Write-Host '[pnf-pty] python/pip routed to kernel interpreter:' "
            "-ForegroundColor Cyan; "
            "Write-Host \"        $Global:PNF_KERNEL_PY\" -ForegroundColor Cyan;"
        )
        pty_proc = await loop.run_in_executor(
            None, lambda: winpty.PtyProcess.spawn(
                [shell, "-NoExit", "-NoLogo", "-Command", _init],
                env=_pty_env, dimensions=(24, 220)))

        def _winpty_read(p):
            """Blocking read — runs in executor thread."""
            try:
                data = p.read(4096)   # blocks until data available or EOF
                return data           # str; empty string means no data yet
            except EOFError:
                return None
            except Exception:
                return None

        async def _read_winpty():
            try:
                while pty_proc.isalive():
                    chunk = await loop.run_in_executor(
                        None, lambda: _winpty_read(pty_proc))
                    if chunk is None:
                        break
                    if chunk and not ws.closed:
                        await ws.send_json({"type": "output", "data": chunk})
            except Exception:
                pass
            if not ws.closed:
                await ws.send_json({"type": "exit", "code": getattr(pty_proc, 'exitstatus', 0) or 0})

        read_task = asyncio.ensure_future(_read_winpty())

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except Exception:
                        continue
                    if data.get("type") == "input":
                        text = data.get("data", "")
                        await loop.run_in_executor(None, lambda t=text: pty_proc.write(t))
                    elif data.get("type") == "resize":
                        cols = int(data.get("cols", 80))
                        rows = int(data.get("rows", 24))
                        await loop.run_in_executor(
                            None, lambda c=cols, r=rows: pty_proc.setwinsize(r, c))
                elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                    break
        finally:
            read_task.cancel()
            try:
                pty_proc.terminate(force=True)
            except Exception:
                pass

    else:
        # Unix: real PTY via pty module
        import pty
        import fcntl
        import termios
        import struct

        master_fd, slave_fd = pty.openpty()

        shell = os.environ.get("SHELL", "/bin/bash")
        # Prepend kernel's Python bin dir so pip/python in the PTY shell
        # resolves to the same environment as the kernel.
        _py_bin = os.path.dirname(get_kernel_python())
        _path_env = _py_bin + os.pathsep + os.environ.get('PATH', '')
        env = {**os.environ, "TERM": "xterm-256color", "PATH": _path_env}
        if sys.prefix != getattr(sys, 'base_prefix', sys.prefix):
            env['VIRTUAL_ENV'] = sys.prefix
        proc = await asyncio.create_subprocess_exec(
            shell,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            close_fds=True,
            env=env,
        )
        os.close(slave_fd)

        loop = asyncio.get_event_loop()

        async def _read_pty():
            try:
                while True:
                    chunk = await loop.run_in_executor(None, lambda: _safe_read(master_fd))
                    if chunk is None:
                        break
                    try:
                        text = chunk.decode("utf-8", errors="replace")
                    except Exception:
                        text = chunk.decode("latin-1", errors="replace")
                    if not ws.closed:
                        await ws.send_json({"type": "output", "data": text})
            except Exception:
                pass
            if not ws.closed:
                await ws.send_json({"type": "exit", "code": proc.returncode or 0})

        def _safe_read(fd):
            import select, errno
            try:
                r, _, _ = select.select([fd], [], [], 0.05)
                if r:
                    return os.read(fd, 4096)
                return b""
            except OSError as e:
                if e.errno in (errno.EIO, errno.EBADF):
                    return None
                return b""

        read_task = asyncio.ensure_future(_read_pty())

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except Exception:
                        continue
                    if data.get("type") == "input":
                        text = data.get("data", "")
                        os.write(master_fd, text.encode("utf-8", errors="replace"))
                    elif data.get("type") == "resize":
                        cols = int(data.get("cols", 80))
                        rows = int(data.get("rows", 24))
                        win = struct.pack("HHHH", rows, cols, 0, 0)
                        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, win)
                elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                    break
        finally:
            read_task.cancel()
            try:
                os.close(master_fd)
            except Exception:
                pass
            try:
                proc.terminate()
            except Exception:
                pass

    return ws


# ── Config endpoints ──────────────────────────────────────────────────────────

async def handle_get_config(request: web.Request) -> web.Response:
    """GET /config — return current PNF config + active kernel Python path."""
    cfg = dict(_load_pnf_config())
    cfg["_kernel_python_active"] = get_kernel_python()
    cfg["_server_python"] = sys.executable
    return web.json_response(cfg)


async def handle_post_config(request: web.Request) -> web.Response:
    """POST /config — update PNF config (e.g. kernel_python path)."""
    try:
        data = await request.json()
    except Exception:
        raise web.HTTPBadRequest(reason="Invalid JSON")
    if "kernel_python" in data:
        kp = data["kernel_python"]
        if kp and not os.path.isfile(kp):
            return web.json_response({"error": f"Python executable not found: {kp}"}, status=400)
    save_pnf_config(data)
    return web.json_response({"status": "ok", "config": _load_pnf_config()})


# ── Kernel discovery & switch ─────────────────────────────────────────

async def handle_list_kernels(request: web.Request) -> web.Response:
    """GET /kernels/list — discover Python interpreters on the system."""
    loop = asyncio.get_event_loop()
    items = await loop.run_in_executor(None, discover_pythons)
    return web.json_response({
        "active": get_kernel_python(),
        "items": items,
        "kernelspec_name": _kernelspec_name_for(get_kernel_python()),
    })


async def handle_switch_kernel(request: web.Request) -> web.Response:
    """POST /kernel/switch {python, register_jupyter?} — switch PNF kernel
    interpreter and (optionally) register a matching Jupyter kernelspec."""
    bridge: KernelBridge = request.app["bridge"]
    try:
        data = await request.json()
    except Exception:
        raise web.HTTPBadRequest(reason="Invalid JSON")
    new_path = (data.get("python") or "").strip()
    if not new_path or not os.path.isfile(new_path):
        return web.json_response({"ok": False, "error": f"Python not found: {new_path}"}, status=400)
    register = bool(data.get("register_jupyter", True))

    result: dict = {"ok": True, "python": new_path}
    # 1) Register Jupyter kernelspec FIRST (so caller can switch jupyter even
    #    if the PNF restart hiccups). Safe even if same path is registered again.
    if register:
        loop = asyncio.get_event_loop()
        try:
            ks = await loop.run_in_executor(None, register_jupyter_kernelspec, new_path)
            result["jupyter_kernelspec"] = ks
        except Exception as e:
            result["jupyter_kernelspec"] = {"ok": False, "error": str(e)}
    # 2) Switch PNF kernel
    try:
        sw = await bridge.switch_kernel_python(new_path)
        result["pnf_kernel"] = {"ok": True, **sw}
    except Exception as e:
        logger.exception("switch_kernel_python failed")
        result["ok"] = False
        result["pnf_kernel"] = {"ok": False, "error": str(e)}
        return web.json_response(result, status=500)
    return web.json_response(result)


# ── App factory & runner ──────────────────────────────────────────────────────

async def create_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    bridge = KernelBridge()
    app["bridge"] = bridge

    app.router.add_get("/ping", handle_ping)
    app.router.add_get("/info", handle_info)
    app.router.add_get("/ws", handle_ws)
    app.router.add_get("/ws/pty", handle_ws_pty)
    app.router.add_post("/restart", handle_restart)
    app.router.add_post("/interrupt", handle_interrupt)
    app.router.add_get("/config", handle_get_config)
    app.router.add_post("/config", handle_post_config)
    app.router.add_get("/kernels/list", handle_list_kernels)
    app.router.add_post("/kernel/switch", handle_switch_kernel)
    # Preflight
    app.router.add_route("OPTIONS", "/{path_info:.*}", lambda r: web.Response())

    async def on_startup(app):
        await app["bridge"].start()

    async def on_shutdown(app):
        await app["bridge"].stop()

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app


async def run_server(host: str = "localhost", port: int = 5891) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    app = await create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    # Auto-register a Jupyter kernelspec for the current kernel_python, so
    # Jupyter users see the same interpreter the PNF server is using without
    # any manual setup. This is idempotent — re-registering the same path is
    # a no-op apart from updating the kernelspec metadata.
    _kp = get_kernel_python()

    # If the kernel python is the PNF server's own uv-tool env, uv strips
    # pip during install. Re-add pip so the PTY's `pip` override actually
    # works (it routes to `<kp> -m pip`).
    if _kp == sys.executable:
        try:
            import importlib
            try:
                importlib.import_module("pip")
            except ImportError:
                import subprocess as _sp
                logging.info("Bootstrapping pip into kernel env via ensurepip…")
                _sp.run([_kp, "-m", "ensurepip", "--upgrade"],
                        capture_output=True, timeout=120)
        except Exception as _e:
            logging.warning("ensurepip skipped: %s", _e)

    try:
        loop = asyncio.get_event_loop()
        ks_result = await loop.run_in_executor(
            None, register_jupyter_kernelspec, _kp)
        if ks_result.get("ok"):
            logging.info(
                "Registered Jupyter kernelspec '%s' → %s",
                ks_result.get("name", "?"), _kp)
        else:
            logging.warning(
                "Could not register Jupyter kernelspec: %s",
                ks_result.get("log", "(unknown)")[:200])
    except Exception as _e:
        logging.warning("Kernelspec auto-register skipped: %s", _e)

    print("=" * 60)
    print(f"  PyNoteFlow Server  v{__version__}")
    print(f"  Listening on  http://{host}:{port}")
    print(f"  Python        {sys.version.split()[0]}")
    print(f"  Kernel        {_kp}")
    print()
    print("  Open PyNoteFlow and click  [Connect Kernel]")
    print("  then choose  'PyNoteFlow Server (localhost)'")
    print()
    print("  Press  Ctrl+C  to stop.")
    print("=" * 60)

    # Keep running until interrupted
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()
